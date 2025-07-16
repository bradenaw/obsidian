use std::collections::BTreeMap;
use std::collections::BTreeSet;

use anyhow::anyhow;
use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::lsm::Lsm;
use crate::meta::TabletState;
use crate::obsidian::InternalError;
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

/// MetaTablets are special from LsmTablets in two necessary ways:
///
/// 1. They always have TabletState::Active, and never transitions, because they can't participate
///    in transfers. This is important because all other tablets default to having no RW
///    permissions until they observe their own TabletMetadata. Obviously that doesn't work for the
///    tablet hosting TabletId::META, because it needs to receive a write to make any
///    TabletMetadata at all.
/// 2. They cannot participate in 2PC. For the sake of the simplicity of the interface, it
///    implements those methods, but always errors.
pub(crate) struct MetaTablet<S>
where
    S: Storage + Send + Sync + 'static,
{
    inner: TabletInner<S>,
}

impl<S> MetaTablet<S>
where
    S: Storage + Send + Sync + 'static,
{
    pub(crate) async fn new(lsm: Lsm<S>) -> anyhow::Result<Self> {
        lsm.create_keyspace(KeyspaceId::META).await?;

        let (prepare_sender, _) = mpsc::channel(1024);
        let (commit_sender, _) = mpsc::channel(128);

        Ok(Self {
            inner: TabletInner::new(
                TabletId::META,
                ColoGroupId::META,
                Range::all(),
                ProtectedLsm::new(TabletId::META, lsm, TabletState::Active),
                prepare_sender,
                commit_sender,
            ),
        })
    }
}

#[async_trait]
impl<S> Tablet for MetaTablet<S>
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
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        self.inner.write(preconds, muts).await
    }

    async fn prepare(
        &self,
        _txid: Txid,
        _preconds: Vec<Precondition>,
        _muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        Err(anyhow!("MetaTablet::prepare not allowed").into())
    }

    async fn try_commit(
        &self,
        _txid: Txid,
        _ts: Timestamp,
        _precond_keys: BTreeSet<Key>,
        _mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<TxOutcome> {
        Err(anyhow!("MetaTablet::try_commit not allowed").into())
    }

    async fn try_abort(&self, _txid: Txid) -> anyhow::Result<TxOutcome> {
        Err(anyhow!("MetaTablet::try_abort not allowed").into())
    }

    async fn wait(&self, _txid: Txid) -> Result<TxOutcome, InternalError> {
        Err(anyhow!("MetaTablet::wait not allowed").into())
    }

    async fn cleanup_committed(
        &self,
        _txid: Txid,
        _ts: Timestamp,
        _precond_keys: BTreeSet<Key>,
        _mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        Err(anyhow!("MetaTablet::cleanup_committed not allowed").into())
    }

    async fn wait_meta_sync(&self, _ts: Timestamp) -> anyhow::Result<()> {
        Err(anyhow!("MetaTablet::wait_meta_sync not allowed").into())
    }
}
