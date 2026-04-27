use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use crossbeam::sync::ShardedLock;

use crate::lsm::Lsm;
use crate::lsm::LsmOptions;
use crate::meta::MetaKey;
use crate::meta::MetaReader;
use crate::meta::MetaState;
use crate::meta::MetaSubscriber;
use crate::meta::MetaSynced;
use crate::meta::MetaSyncedSnapshot;
use crate::meta::SyncType;
use crate::meta::TabletState;
use crate::runtime::Meta;
use crate::runtime::Shards;
use crate::runtime::Storage;
use crate::runtime::Tablet;
use crate::tablet::DataTablet;
use crate::tablet::DataTablet2;
use crate::tablet::HydratingTablet;
use crate::tablet::MetaTablet;
use crate::tablet::ShardMetaTablet;
use crate::tablet::TabletJournalWriter;
use crate::types::ranges_to_splits;
use crate::util::Retry;
use crate::util::WithBackground;
use crate::Bound;
use crate::ColoGroupId;
use crate::InternalError;
use crate::JournalEntry;
use crate::Key;
use crate::Range;
use crate::ShardId;
use crate::TabletId;
use crate::TabletJournalEntry;
use crate::Timestamp;
use crate::TxOutcome;
use crate::Txid;

pub(crate) struct Shard(WithBackground<ShardInner>);

impl Shard {
    pub(crate) async fn new(
        shard_id: ShardId,
        storage: Arc<dyn Storage>,
        meta: Arc<dyn Meta>,
        shards: Arc<dyn Shards>,
        lsm_options: LsmOptions,
        mut lsms: HashMap<TabletId, Lsm>,
        journal: Arc<dyn ShardJournalWriter>,
    ) -> anyhow::Result<Self> {
        // TODO: We need to make sure we don't rewind MetaSynced's snapshot on promotion. We can
        // choose to either:
        // 1. Persist the synced snapshot into ShardMetaTablet before responding to wait_meta_sync
        // 2. Sync to latest during promotion
        //
        // (2) requires meta to be available to promote anything, which isn't great.
        let meta_synced = Arc::new(MetaSynced::new(Arc::clone(&meta)));

        let shard_meta_lsm = match lsms.remove(&TabletId::shard_meta(shard_id)) {
            Some(shard_meta_lsm) => shard_meta_lsm,
            None => Lsm::empty(lsm_options.clone(), Arc::clone(&storage)).await?,
        };
        let shard_meta_tablet = ShardMetaTablet::new(
            shard_id,
            shard_meta_lsm,
            Arc::new(ShardTabletJournalWriter::new(
                TabletId::shard_meta(shard_id),
                Arc::clone(&journal),
            )),
            meta_synced.clone(),
            shards.clone(),
        );

        let meta_tablet = if shard_id == TabletId::META.0 {
            let meta_lsm = match lsms.remove(&TabletId::META) {
                Some(meta_lsm) => meta_lsm,
                None => Lsm::empty(lsm_options.clone(), Arc::clone(&storage)).await?,
            };
            Some(MetaTablet::new(
                meta_lsm,
                Arc::new(ShardTabletJournalWriter::new(
                    TabletId::META,
                    Arc::clone(&journal),
                )),
            ))
        } else {
            None
        };

        let init_tablets: HashMap<TabletId, Arc<DataTablet2>> = {
            let mut init_tablets = HashMap::new();

            for (tablet_id, lsm) in lsms.into_iter() {
                // TODO: Move to shard_meta_tablet.
                let tablet_metadata = meta_synced.snapshot().tablet_metadata(tablet_id).await?;

                let data_tablet = DataTablet::new(
                    tablet_id,
                    tablet_metadata.colo_group_id,
                    tablet_metadata.range,
                    lsm,
                    Arc::new(ShardTabletJournalWriter::new(
                        tablet_id,
                        Arc::clone(&journal),
                    )),
                    Arc::clone(&meta_synced),
                    Arc::clone(&storage),
                    Arc::clone(&shards),
                );

                init_tablets.insert(tablet_id, Arc::new(data_tablet));
            }

            init_tablets
        };

        let shard = Shard(WithBackground::new(Arc::new(ShardInner {
            id: shard_id,
            storage,
            meta,
            meta_synced: meta_synced.clone(),
            shards,
            shard_meta_tablet: Arc::new(shard_meta_tablet),
            tablets: ShardedLock::new(init_tablets),
            lsm_options,
            journal,
        })));

        meta_synced.subscribe(&shard.0);

        Ok(shard)
    }
}

#[async_trait]
impl crate::runtime::Shard for Shard {
    fn id(&self) -> ShardId {
        self.0.id
    }

    fn tablet(&self, tablet_id: TabletId) -> anyhow::Result<Arc<dyn Tablet>> {
        if tablet_id == TabletId::shard_meta(self.0.id) {
            return Ok(Arc::clone(&self.0.shard_meta_tablet) as Arc<dyn Tablet>);
        }

        let tablets = self.0.tablets.read().unwrap();
        Ok(tablets
            .get(&tablet_id)
            .ok_or_else(|| anyhow!("{:?} not found", tablet_id))?
            .clone())
    }

    async fn wait_meta_sync(&self, ts: Timestamp) -> anyhow::Result<()> {
        self.0.meta_synced.wait(ts).await?;

        Ok(())
    }

    async fn tx_try_commit(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<TxOutcome> {
        self.0
            .shard_meta_tablet
            .tx_try_commit(txid, ts, precond_keys, mut_keys)
            .await
    }

    async fn tx_try_abort(&self, txid: Txid) -> anyhow::Result<TxOutcome> {
        self.0.shard_meta_tablet.tx_try_abort(txid).await
    }

    async fn tx_wait(&self, txid: Txid) -> Result<TxOutcome, InternalError> {
        self.0.shard_meta_tablet.tx_wait(txid).await
    }
}

struct ShardInner {
    id: ShardId,
    storage: Arc<dyn Storage>,
    meta: Arc<dyn Meta>,
    meta_synced: Arc<MetaSynced>,
    shards: Arc<dyn Shards>,
    journal: Arc<dyn ShardJournalWriter>,
    lsm_options: LsmOptions,

    shard_meta_tablet: Arc<ShardMetaTablet>,
    meta_tablet: Option<MetaTablet>, // Present only if id==ShardId::META.
    tablets: ShardedLock<HashMap<TabletId, Arc<DataTablet2>>>,
}

impl ShardInner {
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
        if tablet_id == TabletId::shard_meta(self.id) {
            return Err(anyhow!(
                "can't create {:?}: shard meta always exists",
                tablet_id
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

        let tablet = DataTablet::new(
            tablet_id,
            colo_group_id,
            range,
            Lsm::empty(self.lsm_options.clone(), Arc::clone(&self.storage)).await?,
            Arc::new(ShardTabletJournalWriter::new(
                tablet_id,
                Arc::clone(&self.journal),
            )),
            Arc::clone(&self.meta_synced),
            Arc::clone(&self.storage),
            Arc::clone(&self.shards),
        );

        let mut tablets = self.tablets.write().unwrap();
        if tablets.contains_key(&tablet_id) {
            return Ok(());
        }
        tablets.insert(tablet_id, Arc::new(tablet));

        Ok(())
    }

    async fn try_sync_meta(
        &self,
        sync_type: &SyncType,
        snapshot: &MetaSyncedSnapshot,
    ) -> anyhow::Result<()> {
        match sync_type {
            SyncType::Initial => {
                let owned_tablet_ids = snapshot.shard_tablet_ids(self.id).await?;
                for tablet_id in owned_tablet_ids {
                    let tablet_metadata = snapshot.tablet_metadata(tablet_id).await?;
                    self.ensure_tablet(
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
                        if tablet_id.0 != self.id {
                            continue;
                        }

                        let tablet_metadata = snapshot.tablet_metadata(*tablet_id).await?;
                        self.ensure_tablet(
                            *tablet_id,
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

    fn new_data_tablet(
        &self,
        tablet_id: TabletId,
        tablet_metadata: ShardTabletMetadata,
        lsm: Lsm,
    ) -> anyhow::Result<DataTablet2> {
        Ok(match tablet_metadata.state {
            TabletState::Active => DataTablet2::new_active(DataTablet::new(
                tablet_id,
                tablet_metadata.colo_group_id,
                tablet_metadata.range,
                lsm,
                Arc::new(ShardTabletJournalWriter::new(
                    tablet_id,
                    Arc::clone(&self.journal),
                )),
                Arc::clone(&self.meta_synced),
                Arc::clone(&self.storage),
                Arc::clone(&self.shards),
            )),
            TabletState::Hydrating => {
                let srcs = if let Some(TabletTransfer::Dst { srcs }) = tablet_metadata.transfer {
                    srcs
                } else {
                    return Err(anyhow!(
                        "{:?} is in state {:?} but does not have any sources",
                        tablet_id,
                        TabletState::Hydrating
                    ));
                };

                DataTablet2::new_hydrating(HydratingTablet::new(
                    tablet_id,
                    tablet_metadata.colo_group_id,
                    tablet_metadata.range,
                    self.lsm_options.clone(),
                    Arc::clone(&self.storage),
                    Arc::clone(&self.shards),
                    Arc::new(ShardTabletJournalWriter::new(
                        tablet_id,
                        Arc::clone(&self.journal),
                    )),
                    srcs,
                ))
            }
            _ => {
                todo!()
            }
        })
    }

    async fn empty_or_transition_tablet(
        &self,
        tablet_id: TabletId,
        tablet_metadata: ShardTabletMetadata,
    ) -> anyhow::Result<()> {
        let mut tablets = self.tablets.write().unwrap();
        if let Some(tablet) = tablets.get(&tablet_id) {
            match tablet_metadata.state {
                TabletState::Defunct => {
                    tablet.transition_defunct().await?;
                }
                TabletState::Hydrating => {
                    if !tablet.is_hydrating().await {
                        return Err(anyhow!(
                            "{:?}'s state is intended to be {:?}, but cannot transition to it",
                            tablet_id,
                            TabletState::Hydrating
                        ));
                    }
                }
                TabletState::Active => {
                    tablet.transition_active().await?;
                }
                TabletState::Frozen => {
                    tablet.transition_active().await?;
                }
            }
            return Ok(());
        }

        tablets.insert(
            tablet_id,
            Arc::new(self.new_data_tablet(
                tablet_id,
                tablet_metadata,
                Lsm::empty(self.lsm_options.clone(), Arc::clone(&self.storage)).await?,
            )?),
        );

        Ok(())
    }

    async fn shard_tablet_metadata(
        tablet_id: TabletId,
        snapshot: MetaSyncedSnapshot,
    ) -> anyhow::Result<ShardTabletMetadata> {
        let tablet_metadata = snapshot.tablet_metadata(tablet_id).await?;

        let apparent_tablet_state = match tablet_metadata.state {
            MetaState::Stable(state) => state,
            MetaState::Transitioning(_, next_state) => next_state,
        };

        let tablet_transfer = if let Some(transfer_id) = tablet_metadata.transfer_id {
            let transfer_metadata = snapshot.transfer_metadata(transfer_id).await?;

            Some(if transfer_metadata.dsts.contains(&tablet_id) {
                TabletTransfer::Dst {
                    srcs: transfer_metadata.srcs.clone(),
                }
            } else if transfer_metadata.srcs.contains(&tablet_id) {
                let mut dst_ranges = vec![];
                for dst_tablet_id in transfer_metadata.dsts {
                    let dst_tablet_metadata = snapshot.tablet_metadata(dst_tablet_id).await?;
                    dst_ranges.push(dst_tablet_metadata.range);
                }

                let splits = ranges_to_splits(dst_ranges)?;
                TabletTransfer::Src { splits }
            } else {
                return Err(anyhow!(
                    "{:?} is marked with {:?} but is neither src nor dst",
                    tablet_id,
                    transfer_id
                ));
            })
        } else {
            None
        };

        Ok(ShardTabletMetadata {
            colo_group_id: tablet_metadata.colo_group_id,
            range: tablet_metadata.range.clone(),
            state: apparent_tablet_state,
            transfer: tablet_transfer,
        })
    }
}

#[async_trait]
impl MetaSubscriber for ShardInner {
    async fn sync_meta(&self, sync_type: SyncType, snapshot: MetaSyncedSnapshot) {
        Retry::new()
            .indefinitely(&async || -> anyhow::Result<()> {
                self.try_sync_meta(&sync_type, &snapshot).await
            })
            .await;
    }
}

#[async_trait]
pub(crate) trait ShardJournalWriter: Send + Sync + 'static {
    async fn append(&self, entry: JournalEntry) -> anyhow::Result<()>;
}

struct ShardTabletJournalWriter {
    tablet_id: TabletId,
    inner: Arc<dyn ShardJournalWriter>,
}

impl ShardTabletJournalWriter {
    fn new(tablet_id: TabletId, inner: Arc<dyn ShardJournalWriter>) -> ShardTabletJournalWriter {
        ShardTabletJournalWriter { tablet_id, inner }
    }
}

#[async_trait]
impl TabletJournalWriter for ShardTabletJournalWriter {
    async fn append(&self, entry: TabletJournalEntry) -> anyhow::Result<()> {
        self.inner
            .append(JournalEntry {
                tablet_id: self.tablet_id,
                entry,
            })
            .await
    }
}

struct ShardTabletMetadata {
    colo_group_id: ColoGroupId,
    range: Range<Vec<u8>>,
    state: TabletState,
    transfer: Option<TabletTransfer>,
}

enum TabletTransfer {
    Src { splits: Vec<Bound<Vec<u8>>> },
    Dst { srcs: Vec<TabletId> },
}
