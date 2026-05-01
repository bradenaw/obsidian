use std::collections::BTreeMap;
use std::sync::Arc;

use crate::lsm::Lsm;
use crate::lsm::Manifest;
use crate::tablet::read_only_lsm::LsmRead;
use crate::tablet::read_only_lsm::ReadOnlyLsm;
use crate::tablet::TabletJournalWriter;
use crate::util::Retry;
use crate::Bound;
use crate::Direction;
use crate::HistoryRange;
use crate::InternalError;
use crate::Key;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Range;
use crate::Revision;
use crate::RevisionValue;
use crate::TabletJournalEntry;
use crate::Timestamp;

pub(super) trait LsmWrite {
    fn set_splits(&self, splits: Vec<Bound<Vec<u8>>>);

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()>;

    async fn write(
        &self,
        ts: Timestamp,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<(), InternalError>;

    async fn flush(&self) -> anyhow::Result<()>;
}

pub(super) struct JournaledLsm {
    lsm: Lsm,
    journal: Arc<dyn TabletJournalWriter>,
}

impl JournaledLsm {
    pub fn new(lsm: Lsm, journal: Arc<dyn TabletJournalWriter>) -> Self {
        Self { lsm, journal }
    }

    pub async fn make_read_only(self) -> ReadOnlyLsm {
        // Important: the expectation for a read-only LSM is that its manifest shows all of the
        // writes.
        Retry::new()
            .indefinitely(&async || self.flush().await)
            .await;
        ReadOnlyLsm::new(self.lsm).await
    }
}

impl LsmRead for JournaledLsm {
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

impl LsmWrite for JournaledLsm {
    fn set_splits(&self, splits: Vec<Bound<Vec<u8>>>) {
        self.lsm.set_splits(splits)
    }

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        // TODO: Journal.
        self.lsm.create_keyspace(keyspace_id);
        Ok(())
    }

    async fn write(
        &self,
        ts: Timestamp,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<(), InternalError> {
        self.journal
            .append(TabletJournalEntry::Write(
                ts,
                muts.iter()
                    .map(|((keyspace_id, key), mutation)| {
                        let value = match mutation {
                            Mutation::Put(value) => RevisionValue::Regular(value.clone()),
                            Mutation::Delete => RevisionValue::Tombstone,
                        };
                        (*keyspace_id, key.clone(), value)
                    })
                    .collect(),
            ))
            .await?;

        for (key, mutation) in muts {
            self.lsm.write(ts, key, mutation);
        }

        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        // TODO: Journal.
        //let seqno = self.journal.append(TabletJournalEntry::NoOp).await?;
        self.lsm.flush().await?;
        //let manifest = self.lsm.manifest();
        //self.journal
        //    .append(TabletJournalEntry::Manifest(seqno, manifest))
        //    .await?;
        Ok(())
    }
}
