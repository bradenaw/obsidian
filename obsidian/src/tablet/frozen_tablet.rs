use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;

use crate::lsm::Manifest;
use crate::runtime::Shards;
use crate::runtime::Storage;
use crate::runtime::Tablet;
use crate::tablet::read_only_lsm::ReadOnlyLsm;
use crate::tablet::tablet_inner::TabletInner;
use crate::tablet::DataTablet;
use crate::tablet::TabletJournalWriter;
use crate::Bound;
use crate::ColoGroupId;
use crate::Direction;
use crate::HistoryRange;
use crate::InternalError;
use crate::Key;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Precondition;
use crate::Range;
use crate::Record;
use crate::Revision;
use crate::TabletId;
use crate::Timestamp;
use crate::Txid;

pub(crate) struct FrozenTablet {
    inner: TabletInner<ReadOnlyLsm>,
    storage: Arc<dyn Storage>,
    shards: Arc<dyn Shards>,
}

impl FrozenTablet {
    pub fn new(
        tablet_id: TabletId,
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
        lsm: ReadOnlyLsm,
        storage: Arc<dyn Storage>,
        shards: Arc<dyn Shards>,
    ) -> Self {
        Self {
            inner: TabletInner::new(tablet_id, colo_group_id, range, lsm),
            storage,
            shards,
        }
    }

    pub fn make_active(self, journal: Arc<dyn TabletJournalWriter>) -> DataTablet {
        DataTablet::new_inner(
            self.inner.tablet_id,
            self.inner.colo_group_id,
            self.inner.range,
            self.inner.lsm.make_writeable(journal),
            self.storage,
            self.shards,
        )
    }
}

#[async_trait]
impl Tablet for FrozenTablet {
    async fn get_multi(
        &self,
        ts: Timestamp,
        keys: BTreeSet<Key>,
    ) -> Result<BTreeMap<Key, Record>, InternalError> {
        self.inner.get_multi(ts, keys).await
    }

    async fn get_latest_multi(
        &self,
        keys: BTreeSet<Key>,
    ) -> Result<(Timestamp, BTreeMap<Key, Record>), InternalError> {
        self.inner.get_latest_multi(keys).await
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
        Err(anyhow!("FrozenTablet::write not allowed").into())
    }

    async fn prepare(
        &self,
        _txid: Txid,
        _preconds: Vec<Precondition>,
        _muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        Err(anyhow!("FrozenTablet::prepare not allowed").into())
    }

    async fn cleanup_committed(
        &self,
        _txid: Txid,
        _ts: Timestamp,
        _precond_keys: BTreeSet<Key>,
        _mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        Err(anyhow!("FrozenTablet::cleanup_committed not allowed").into())
    }

    async fn manifest(&self) -> anyhow::Result<Manifest> {
        Ok(self.inner.manifest())
    }

    async fn wait_mostly_hydrated(&self) -> anyhow::Result<()> {
        Err(anyhow!("FrozenTablet::wait_mostly_hydrated not allowed").into())
    }

    async fn catchup(&self) -> anyhow::Result<()> {
        Err(anyhow!("FrozenTablet::catchup not allowed").into())
    }

    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>> {
        self.find_split().await
    }
}
