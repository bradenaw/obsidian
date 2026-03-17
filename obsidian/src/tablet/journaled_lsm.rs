use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::anyhow;
use futures::TryStreamExt;

use crate::lsm::Lsm;
use crate::lsm::LsmOptions;
use crate::lsm::Manifest;
use crate::lsm::Preloaded;
use crate::lsm::Preloader;
use crate::runtime::Storage;
use crate::runtime::Wal;
use crate::util::hexlify;
use crate::Bound;
use crate::Direction;
use crate::HistoryRange;
use crate::Key;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Range;
use crate::Revision;
use crate::RevisionValue;
use crate::Timestamp;
use crate::TabletJournalEntry;
use crate::WalSeq;
use crate::WriteError;

pub(super) struct JournaledLsm {
    lsm: Lsm,
    wal: Arc<dyn Wal>,
}

impl JournaledLsm {
    pub async fn open(
        lsm_options: LsmOptions,
        wal: Arc<dyn Wal>,
        storage: Arc<dyn Storage>,
    ) -> anyhow::Result<Self> {
        let (lsm, _) = Self::recovery(lsm_options, &wal, &storage).await?;

        Ok(Self { lsm, wal })
    }

    pub async fn write(
        &self,
        ts: Timestamp,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<(), WriteError> {
        let writes = muts
            .iter()
            .map(|((keyspace_id, key), mutation)| {
                let revision_value = match mutation {
                    Mutation::Put(value) => RevisionValue::Regular(value.clone()),
                    Mutation::Delete => RevisionValue::Tombstone,
                };
                (*keyspace_id, key.clone(), revision_value)
            })
            .collect();

        self.wal.append(TabletJournalEntry::Write(ts, writes)).await?;

        for (key, mutation) in muts {
            self.lsm.write(ts, key, mutation);
        }

        Ok(())
    }

    pub async fn get(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        key: &[u8],
    ) -> anyhow::Result<Option<(Timestamp, RevisionValue)>> {
        self.lsm.get(ts, keyspace_id, key).await
    }

    pub async fn scan_page(
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

    pub async fn history_page(
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

    pub async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        // TODO: Journal.
        self.lsm.create_keyspace(keyspace_id)
    }

    pub fn manifest(&self) -> Manifest {
        self.lsm.manifest()
    }

    pub fn keyspaces(&self) -> Vec<KeyspaceId> {
        self.lsm.keyspaces()
    }

    pub async fn flush(&self) -> anyhow::Result<()> {
        let seqno = self.wal.append(TabletJournalEntry::NoOp).await?;
        self.lsm.flush().await?;
        let manifest = self.lsm.manifest();
        self.wal.append(TabletJournalEntry::Manifest(seqno, manifest)).await?;
        Ok(())
    }

    pub fn find_split(&self) -> Option<Bound<Vec<u8>>> {
        self.lsm.find_split()
    }

    pub fn load(&self, preloaded: Preloaded) -> anyhow::Result<()> {
        self.lsm.load(preloaded)
    }

    pub fn set_splits(&self, splits: Vec<Bound<Vec<u8>>>) {
        self.lsm.set_splits(splits)
    }

    pub async fn pause_compaction(&self) {
        self.lsm.pause_compaction().await;
    }

    pub fn unpause_compaction(&self) {
        self.lsm.unpause_compaction();
    }

    async fn recovery(
        lsm_options: LsmOptions,
        wal: &Arc<dyn Wal>,
        storage: &Arc<dyn Storage>,
    ) -> anyhow::Result<(Lsm, WalSeq)> {
        let oldest_seqno = wal.oldest_available().await?;
        let mut newest_seqno = WalSeq(1);
        let mut wal_stream = wal.read(oldest_seqno);

        let mut preloader = Preloader::new(Arc::clone(storage));
        let mut entries = VecDeque::new();

        while let Some((seqno, entry)) = wal_stream.try_next().await? {
            match entry {
                TabletJournalEntry::NoOp => {}
                TabletJournalEntry::Write(ts, kvs) => {
                    entries.push_back((seqno, ts, kvs));
                }
                TabletJournalEntry::Manifest(included_seqno, manifest) => {
                    let trim_to_idx = entries
                        .binary_search_by_key(&included_seqno, |(seqno, _, _)| *seqno)
                        .unwrap_or_else(core::convert::identity);
                    entries.drain(0..trim_to_idx);

                    preloader.set_manifest(manifest);
                }
            }
            newest_seqno = seqno;
        }

        let preloaded = preloader.load().await?;

        let lsm = Lsm::open(lsm_options, Arc::clone(storage), preloaded).await?;

        for (_, ts, kvs) in entries {
            for (keyspace_id, key, value) in kvs {
                // It's possible that this revision is already present since the seqno in
                // TabletJournalEntry::Manifest is a lower bound, the manifest may already contain newer
                // writes.
                if let Some((existing_ts, existing_value)) = lsm.get(ts, keyspace_id, &key).await? {
                    if existing_ts == ts {
                        if value != existing_value {
                            return Err(anyhow!(
                                "duplicate revision for {}@{} with differing values",
                                hexlify(&key[..]),
                                ts,
                            ));
                        }
                        continue;
                    }
                }

                let mutation = match value {
                    RevisionValue::Regular(value) => Mutation::Put(value),
                    RevisionValue::Tombstone => Mutation::Delete,
                };

                lsm.write(ts, (keyspace_id, key), mutation);
            }
        }

        Ok((lsm, newest_seqno))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use futures::TryStreamExt;

    use crate::lsm::LsmOptions;
    use crate::runtime::Wal;
    use crate::tablet::journaled_lsm::JournaledLsm;
    use crate::test::MemStorage;
    use crate::test::MemWal;
    use crate::ColoGroupId;
    use crate::KeyspaceId;
    use crate::Mutation;
    use crate::RevisionValue;
    use crate::Timestamp;
    use crate::TabletJournalEntry;

    #[tokio::test]
    async fn test_recovery() -> anyhow::Result<()> {
        let _ = pretty_env_logger::try_init();

        let wal = Arc::new(MemWal::new()) as Arc<dyn Wal>;
        let storage = Arc::new(MemStorage::new());

        let lsm = JournaledLsm::open(
            LsmOptions {
                l0_max_size: 128,
                l1_max_size: 1024,
                block_size_target: 128,
                run_size_target: 512,
            },
            Arc::clone(&wal),
            storage.clone(),
        )
        .await?;

        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
        lsm.create_keyspace(keyspace_id).await?;

        let mut map = BTreeMap::new();
        let mut write_ts = 5;
        for _ in 0..10 {
            // We consider these writes to be 10 bytes (1 key + 8 ts + 1 value), so this is
            // enough to overfill a memtable.
            for i in 0..24 {
                let v = (i % 179) as u8;
                lsm.write(
                    Timestamp(write_ts),
                    BTreeMap::from([((keyspace_id, vec![i as u8]), Mutation::Put(vec![v]))]),
                )
                .await?;
                write_ts += 2;
                map.insert(i as u8, v);
            }

            for (k, v) in &map {
                assert_eq!(
                    lsm.get(Timestamp(write_ts), keyspace_id, &[*k])
                        .await?
                        .map(|(_, b)| b),
                    Some(RevisionValue::Regular(vec![*v])),
                );
            }
        }

        lsm.flush().await?;

        // Make sure we actually did do a compaction.
        assert!(lsm.manifest().runs().next().is_some());

        drop(lsm);

        // Trim to ensure we're actually recovering from the manifest and not just replaying the
        // writes.
        {
            let mut trim_before = None;
            let mut stream = wal.read(wal.oldest_available().await?);
            while let Some((_, entry)) = stream.try_next().await? {
                if let TabletJournalEntry::Manifest(included_seqno, _) = entry {
                    trim_before = Some(included_seqno);
                }
            }
            wal.trim(trim_before.expect("didn't actually write a manifest"))
                .await?;
        }

        // Rebuild the LSM from the same WAL and storage, this should recover everything.
        let lsm = JournaledLsm::open(LsmOptions::default(), wal, storage).await?;

        for (k, v) in &map {
            assert_eq!(
                lsm.get(Timestamp(write_ts), keyspace_id, &[*k])
                    .await?
                    .map(|(_, b)| b),
                Some(RevisionValue::Regular(vec![*v]))
            );
        }

        Ok(())
    }
}
