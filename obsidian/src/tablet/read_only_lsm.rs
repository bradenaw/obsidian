use std::sync::Arc;

use crate::lsm::Lsm;
use crate::lsm::Manifest;
use crate::tablet::journaled_lsm::JournaledLsm;
use crate::tablet::TabletJournalWriter;
use crate::Bound;
use crate::Direction;
use crate::HistoryRange;
use crate::KeyspaceId;
use crate::Range;
use crate::Revision;
use crate::RevisionValue;
use crate::Timestamp;

pub(super) trait LsmRead {
    async fn get(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        key: &[u8],
    ) -> anyhow::Result<Option<(Timestamp, RevisionValue)>>;

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<Revision>, Option<Range<Vec<u8>>>)>;

    async fn history_page(
        &self,
        keyspace_id: KeyspaceId,
        key: &[u8],
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<(Timestamp, RevisionValue)>, Option<HistoryRange>)>;

    fn manifest(&self) -> Manifest;

    fn keyspaces(&self) -> Vec<KeyspaceId>;

    fn find_split(&self) -> Option<Bound<Vec<u8>>>;
}

pub(super) struct ReadOnlyLsm {
    lsm: Lsm,
}

impl ReadOnlyLsm {
    pub fn new(lsm: Lsm) -> Self {
        Self { lsm }
    }

    pub fn make_writeable(self, journal: Arc<dyn TabletJournalWriter>) -> JournaledLsm {
        self.lsm.unpause_compaction();
        JournaledLsm::new(self.lsm, journal)
    }
}

impl LsmRead for ReadOnlyLsm {
    async fn get(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        key: &[u8],
    ) -> anyhow::Result<Option<(Timestamp, RevisionValue)>> {
        self.lsm.get(ts, keyspace_id, key).await
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<Revision>, Option<Range<Vec<u8>>>)> {
        self.lsm
            .scan_page(ts, keyspace_id, range, direction, limit)
            .await
    }

    async fn history_page(
        &self,
        keyspace_id: KeyspaceId,
        key: &[u8],
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<(Timestamp, RevisionValue)>, Option<HistoryRange>)> {
        self.lsm
            .history_page(keyspace_id, key, range, direction, limit)
            .await
    }

    fn manifest(&self) -> Manifest {
        self.lsm.manifest()
    }

    fn keyspaces(&self) -> Vec<KeyspaceId> {
        self.lsm.keyspaces()
    }

    fn find_split(&self) -> Option<Bound<Vec<u8>>> {
        self.lsm.find_split()
    }
}
