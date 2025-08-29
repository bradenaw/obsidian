use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::lsm::Lsm;
use crate::lsm::Manifest;
use crate::meta::MetaSynced;
use crate::meta::MetaSyncedSnapshot;
use crate::meta::TabletState;
use crate::obsidian::InternalError;
use crate::obsidian::Shards;
use crate::obsidian::TabletId;
use crate::obsidian::TxOutcome;
use crate::obsidian::Txid;
use crate::range::Range;
use crate::storage::Storage;
use crate::tablet::protected::ProtectedLsm;
use crate::tablet::tablet_inner::TabletInner;
use crate::tablet::Tablet;
use crate::types::ColoGroupId;
use crate::types::Direction;
use crate::types::HistoryRange;
use crate::types::Key;
use crate::types::KeyspaceId;
use crate::types::Mutation;
use crate::types::Precondition;
use crate::types::Record;
use crate::types::Revision;
use crate::types::Timestamp;
use crate::util::Background;

pub(crate) struct DataTablet<S: Storage> {
    inner: Arc<TabletInner<S>>,
    meta_synced: Arc<MetaSynced>,

    bg: Background,
}

#[async_trait]
impl<S: Storage + Send + Sync + 'static> Tablet for DataTablet<S> {
    async fn get(&self, ts: Timestamp, key: &Key) -> Result<Option<Record>, InternalError> {
        self.inner.get(ts, key).await
    }

    async fn get_latest(&self, key: Key) -> Result<(Timestamp, Option<Record>), InternalError> {
        self.inner.get_latest(key).await
    }

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> Result<Timestamp, InternalError> {
        self.inner.latest_snapshot(keys).await
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<Range<Vec<u8>>>), InternalError> {
        self.inner
            .scan_page(ts, keyspace_id, range, direction, limit)
            .await
    }

    async fn history_page(
        &self,
        key: Key,
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Revision>, Option<HistoryRange>), InternalError> {
        self.inner.history_page(key, range, direction, limit).await
    }

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        self.inner.write(preconds, muts).await
    }

    async fn prepare(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        self.inner.prepare(txid, preconds, muts).await
    }

    async fn try_commit(
        &self,
        _txid: Txid,
        _ts: Timestamp,
        _precond_keys: BTreeSet<Key>,
        _mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<TxOutcome> {
        Err(anyhow!("DataTablet::try_commit not allowed").into())
    }

    async fn try_abort(&self, _txid: Txid) -> anyhow::Result<TxOutcome> {
        Err(anyhow!("DataTablet::try_abort not allowed").into())
    }

    async fn wait(&self, _txid: Txid) -> Result<TxOutcome, InternalError> {
        Err(anyhow!("DataTablet::wait not allowed").into())
    }

    async fn cleanup_committed(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        self.inner
            .cleanup_committed(txid, ts, precond_keys, mut_keys)
            .await
    }

    async fn wait_meta_sync(&self, ts: Timestamp) -> anyhow::Result<()> {
        self.meta_synced.wait(ts).await
    }

    async fn manifest(&self) -> anyhow::Result<Manifest> {
        Ok(self.inner.manifest())
    }

    async fn wait_mostly_hydrated(&self) -> anyhow::Result<()> {
        self.inner.wait_mostly_hydrated().await
    }

    async fn catchup(&self) -> anyhow::Result<()> {
        self.inner.catchup().await
    }
}

impl<S: Storage + Send + Sync + 'static> DataTablet<S> {
    pub async fn new(
        tablet_id: TabletId,
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
        lsm: Lsm<S>,
        meta_synced: Arc<MetaSynced>,
        storage: Arc<S>,
        shards: Arc<dyn Shards + Sync + Send>,
    ) -> anyhow::Result<Self> {
        let (prepare_sender, prepare_receiver) = mpsc::channel(1024);
        let (commit_sender, _) = mpsc::channel(1);

        lsm.create_keyspace(KeyspaceId::TX_OUTCOMES).await?;

        let inner = Arc::new(TabletInner::new(
            tablet_id,
            colo_group_id,
            range,
            ProtectedLsm::new(tablet_id, lsm, TabletState::None),
            prepare_sender.clone(),
            commit_sender.clone(),
        ));

        let bg = Background::new();

        bg.spawn({
            let inner = inner.clone();
            let shards = shards.clone();
            async move {
                inner.resolve_prepared(&shards, prepare_receiver).await;
            }
        });

        bg.spawn({
            let inner = inner.clone();
            let prepare_sender = prepare_sender.clone();
            async move {
                inner.scan_for_pending_mutations(prepare_sender).await;
            }
        });

        bg.spawn({
            let inner = inner.clone();
            async move {
                inner.scan_for_precond_locks(prepare_sender).await;
            }
        });

        bg.spawn({
            let inner = inner.clone();
            async move {
                inner.abort_long_waits().await;
            }
        });

        {
            let inner = inner.clone();
            meta_synced
                .subscribe(move |sync_type, snapshot: MetaSyncedSnapshot| {
                    let inner = inner.clone();
                    let shards = shards.clone();
                    let storage = storage.clone();
                    async move {
                        inner.sync_meta(&storage, &shards, sync_type, snapshot).await;
                    }
                })
                .await;
        }

        Ok(Self {
            inner,
            meta_synced,
            bg,
        })
    }
}
