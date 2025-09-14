use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use crossbeam::sync::ShardedLock;

use crate::lsm::LsmBuilder;
use crate::meta::Meta;
use crate::meta::MetaKey;
use crate::meta::MetaReader;
use crate::meta::MetaSynced;
use crate::meta::MetaSyncedSnapshot;
use crate::meta::SyncType;
use crate::obsidian::Shards;
use crate::tablet::TabletId;
use crate::range::Range;
use crate::storage::Storage;
use crate::tablet::DataTablet;
use crate::tablet::MetaTablet;
use crate::tablet::ShardMetaTablet;
use crate::tablet::Tablet;
use crate::types::ColoGroupId;
use crate::types::ShardId;
use crate::types::Timestamp;
use crate::util::Background;
use crate::util::Retry;

pub(crate) struct Shard<S, M> {
    bg: Background,
    inner: Arc<ShardInner<S, M>>,
}

impl<S, M> Shard<S, M>
where
    S: Storage,
    M: Meta + 'static,
{
    pub(crate) async fn new(
        shard_id: ShardId,
        storage: Arc<S>,
        meta: Arc<M>,
        shards: Arc<dyn Shards>,
        lsm_builder: LsmBuilder<S>,
    ) -> anyhow::Result<Self> {
        let meta_synced = Arc::new(MetaSynced::new(Arc::clone(&meta)));

        let init_tablets: HashMap<TabletId, Arc<dyn Tablet + 'static>> = {
            let mut init_tablets = HashMap::new();
            if shard_id == TabletId::META.0 {
                let meta_tablet = MetaTablet::new(lsm_builder.clone().build().await?).await?;
                init_tablets.insert(
                    TabletId::META,
                    Arc::new(meta_tablet) as Arc<dyn Tablet>,
                );
            }

            let shard_meta_tablet = ShardMetaTablet::new(
                shard_id,
                lsm_builder.clone().build().await?,
                meta_synced.clone(),
                shards.clone(),
            ).await?;
            init_tablets.insert(
                TabletId::shard_meta(shard_id),
                Arc::new(shard_meta_tablet) as Arc<dyn Tablet>,
            );

            init_tablets
        };

        let inner = Arc::new(ShardInner {
            id: shard_id,
            storage,
            meta,
            meta_synced: meta_synced.clone(),
            shards,
            lsm_builder,
            tablets: ShardedLock::new(init_tablets),
        });

        let bg = Background::new();

        {
            let inner = inner.clone();
            meta_synced
                .subscribe(move |sync_type, snapshot| {
                    let inner = inner.clone();
                    async move {
                        Self::sync_meta(inner.clone(), sync_type, snapshot).await;
                    }
                })
                .await;
        }

        Ok(Self { bg, inner })
    }

    async fn sync_meta(
        inner: Arc<ShardInner<S, M>>,
        sync_type: SyncType,
        snapshot: MetaSyncedSnapshot,
    ) {
        Retry::new()
            .indefinitely(&async || -> anyhow::Result<()> {
                Self::try_sync_meta(inner.clone(), sync_type.clone(), snapshot.clone()).await
            })
            .await;
    }

    async fn try_sync_meta(
        inner: Arc<ShardInner<S, M>>,
        sync_type: SyncType,
        snapshot: MetaSyncedSnapshot,
    ) -> anyhow::Result<()> {
        match sync_type {
            SyncType::Initial => {
                let owned_tablet_ids = snapshot.shard_tablet_ids(inner.id).await?;
                for tablet_id in owned_tablet_ids {
                    let tablet_metadata = snapshot.tablet_metadata(tablet_id).await?;
                    inner
                        .ensure_tablet(
                            tablet_id,
                            tablet_metadata.colo_group_id,
                            tablet_metadata.range,
                        )
                        .await?;
                }
            }
            SyncType::Tx(meta_keys) => {
                for meta_key in meta_keys {
                    if let MetaKey::Tablet(tablet_id) = meta_key {
                        if tablet_id.0 != inner.id {
                            continue;
                        }

                        let tablet_metadata = snapshot.tablet_metadata(tablet_id).await?;
                        inner
                            .ensure_tablet(
                                tablet_id,
                                tablet_metadata.colo_group_id,
                                tablet_metadata.range,
                            )
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<S, M> crate::obsidian::Shard for Shard<S, M>
where
    S: Storage,
    M: Meta + 'static,
{
    fn id(&self) -> ShardId {
        self.inner.id
    }

    fn tablet(&self, tablet_id: TabletId) -> anyhow::Result<Arc<dyn Tablet>> {
        let tablets = self.inner.tablets.read().unwrap();
        Ok(tablets
            .get(&tablet_id)
            .ok_or_else(|| anyhow!("{:?} not found", tablet_id))?
            .clone())
    }

    async fn wait_meta_sync(&self, ts: Timestamp) -> anyhow::Result<()> {
        self.inner.meta_synced.wait(ts).await?;

        Ok(())
    }
}

#[async_trait]
impl<S, M> crate::obsidian::Shard for Arc<Shard<S, M>>
where
    S: Storage,
    M: Meta + 'static,
{
    fn id(&self) -> ShardId {
        Shard::id(self)
    }

    fn tablet(&self, tablet_id: TabletId) -> anyhow::Result<Arc<dyn Tablet>> {
        Shard::tablet(self, tablet_id)
    }

    async fn wait_meta_sync(&self, ts: Timestamp) -> anyhow::Result<()> {
        Shard::wait_meta_sync(self, ts).await
    }
}

struct ShardInner<S, M> {
    id: ShardId,
    storage: Arc<S>,
    meta: Arc<M>,
    meta_synced: Arc<MetaSynced>,
    shards: Arc<dyn Shards>,
    lsm_builder: LsmBuilder<S>,

    tablets: ShardedLock<HashMap<TabletId, Arc<dyn Tablet + 'static>>>,
}

impl<S, M> ShardInner<S, M>
where
    M: Meta + 'static,
    S: Storage,
{
    async fn ensure_tablet(
        &self,
        tablet_id: TabletId,
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
    ) -> anyhow::Result<()> {
        if tablet_id.0 != self.id {
            return Err(anyhow!(
                "can't create {:?}: wrong shard {:?}",
                tablet_id,
                self.id
            ));
        }

        {
            let tablets = self.tablets.read().unwrap();
            if tablets.contains_key(&tablet_id) {
                return Ok(());
            }
        }

        log::info!(
            "creating {:?} for {:?}/{:?}",
            tablet_id,
            colo_group_id,
            range
        );

        let lsm = self.lsm_builder.clone().build().await?;

        let tablet = DataTablet::new(
            tablet_id,
            colo_group_id,
            range,
            lsm,
            Arc::clone(&self.meta_synced),
            Arc::clone(&self.storage),
            Arc::clone(&self.shards),
        )
        .await?;

        let mut tablets = self.tablets.write().unwrap();
        if tablets.contains_key(&tablet_id) {
            return Ok(());
        }
        tablets.insert(tablet_id, Arc::new(tablet));

        Ok(())
    }
}
