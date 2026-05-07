use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::Manifest;
use crate::runtime::Shards;
use crate::runtime::Storage;
use crate::tablet::active_tablet::ActiveTablet;
use crate::tablet::read_only_lsm::ReadOnlyLsm;
use crate::tablet::tablet_inner::TabletInner;
use crate::tablet::TabletJournalWriter;
use crate::ColoGroupId;
use crate::Direction;
use crate::HistoryRange;
use crate::InternalError;
use crate::Key;
use crate::KeyspaceId;
use crate::Range;
use crate::Record;
use crate::Revision;
use crate::TabletId;
use crate::Timestamp;

pub(super) struct FrozenTablet {
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

    pub fn tablet_id(&self) -> TabletId {
        self.inner.tablet_id
    }

    pub fn colo_group_id(&self) -> ColoGroupId {
        self.inner.colo_group_id
    }

    pub fn make_active(self, journal: Arc<dyn TabletJournalWriter>) -> ActiveTablet {
        ActiveTablet::new(
            self.inner.tablet_id,
            self.inner.colo_group_id,
            self.inner.range,
            self.inner.lsm.make_writeable(journal),
            self.storage,
            self.shards,
        )
    }

    pub async fn get_multi(
        &self,
        ts: Timestamp,
        keys: BTreeSet<Key>,
    ) -> Result<BTreeMap<Key, Record>, InternalError> {
        self.inner.get_multi(ts, keys).await
    }

    pub async fn get_latest_multi(
        &self,
        keys: BTreeSet<Key>,
    ) -> Result<(Timestamp, BTreeMap<Key, Record>), InternalError> {
        self.inner.get_latest_multi(keys).await
    }

    pub async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> Result<Timestamp, InternalError> {
        self.inner.latest_snapshot(keys).await
    }

    pub async fn scan_page(
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

    pub async fn history_page(
        &self,
        key: Key,
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Revision>, Option<HistoryRange>), InternalError> {
        self.inner.history_page(key, range, direction, limit).await
    }

    pub async fn manifest(&self) -> anyhow::Result<Manifest> {
        Ok(self.inner.manifest())
    }
}
