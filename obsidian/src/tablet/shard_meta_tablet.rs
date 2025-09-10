use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::lsm::Lsm;
use crate::lsm::Manifest;
use crate::meta::MetaSynced;
use crate::meta::TabletState;
use crate::obsidian::InternalError;
use crate::obsidian::Shards;
use crate::obsidian::TabletId;
use crate::obsidian::TxOutcome;
use crate::obsidian::Txid;
use crate::range::Bound;
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
use crate::types::ShardId;
use crate::types::Timestamp;
use crate::util::Background;

/// ShardMetaTablets are owned by a single shard and own a range of ColoGroupId::SHARD_META that
/// begins with their own shard ID.
///
/// They are distinct from other kinds of tablets:
///
/// 1. They always have TabletState::Active. Their range cannot be moved to another tablet.
/// 2. They only host the TX_OUTCOMES keyspace so they refuse regular writes but do accept
///    try_commit/try_abort.
pub(crate) struct ShardMetaTablet<S>
where
    S: Storage + Send + Sync + 'static,
{
    inner: Arc<TabletInner<S>>,
    bg: Background,
}

impl<S> ShardMetaTablet<S>
where
    S: Storage + Send + Sync + 'static,
{
    pub(crate) async fn new(
        shard_id: ShardId,
        lsm: Lsm<S>,
        meta_synced: Arc<MetaSynced>,
        shards: Arc<dyn Shards + Sync + Send>,
    ) -> anyhow::Result<Self> {
        lsm.create_keyspace(KeyspaceId::TX_OUTCOMES).await?;

        let (prepare_sender, _) = mpsc::channel(1);
        let (commit_sender, commit_receiver) = mpsc::channel(128);

        let tablet_id = TabletId::shard_meta(shard_id);

        let inner = Arc::new(TabletInner::new(
            tablet_id,
            ColoGroupId::SHARD_META,
            TabletId::shard_meta_owned_range(shard_id),
            ProtectedLsm::new(tablet_id, lsm, TabletState::Active),
            prepare_sender,
            commit_sender.clone(),
        ));

        let bg = Background::new();

        bg.spawn({
            let inner = inner.clone();
            async move {
                inner.scan_for_committed_outcomes(commit_sender).await;
            }
        });

        bg.spawn({
            let inner = inner.clone();
            let meta_synced = meta_synced.clone();
            let shards = shards.clone();
            async move {
                inner
                    .cleanup_committed_outcomes(meta_synced, shards, commit_receiver)
                    .await;
            }
        });

        Ok(Self { inner, bg })
    }
}

#[async_trait]
impl<S> Tablet for ShardMetaTablet<S>
where
    S: Storage + Send + Sync + 'static,
{
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
        _preconds: Vec<Precondition>,
        _muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        Err(anyhow!("ShardMetaTablet::write not allowed").into())
    }

    async fn prepare(
        &self,
        _txid: Txid,
        _preconds: Vec<Precondition>,
        _muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        Err(anyhow!("ShardMetaTablet::prepare not allowed").into())
    }

    async fn try_commit(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<TxOutcome> {
        self.inner
            .try_commit(txid, ts, precond_keys, mut_keys)
            .await
    }

    async fn try_abort(&self, txid: Txid) -> anyhow::Result<TxOutcome> {
        self.inner.try_abort(txid).await
    }

    async fn wait(&self, txid: Txid) -> Result<TxOutcome, InternalError> {
        self.inner.wait(txid).await
    }

    async fn cleanup_committed(
        &self,
        _txid: Txid,
        _ts: Timestamp,
        _precond_keys: BTreeSet<Key>,
        _mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        Err(anyhow!("ShardMetaTablet::cleanup_committed not allowed").into())
    }

    async fn wait_meta_sync(&self, _ts: Timestamp) -> anyhow::Result<()> {
        Err(anyhow!("ShardMetaTablet::wait_meta_sync not allowed").into())
    }

    async fn manifest(&self) -> anyhow::Result<Manifest> {
        Ok(self.inner.manifest())
    }

    async fn wait_mostly_hydrated(&self) -> anyhow::Result<()> {
        Err(anyhow!("ShardMetaTablet::wait_mostly_hydrated not allowed").into())
    }

    async fn catchup(&self) -> anyhow::Result<()> {
        Err(anyhow!("ShardMetaTablet::catchup not allowed").into())
    }

    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>> {
        Err(anyhow!("ShardMetaTablet::find_split not allowed").into())
    }
}
