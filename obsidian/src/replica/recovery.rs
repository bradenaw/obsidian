use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::anyhow;

use crate::lsm::Lsm;
use crate::lsm::LsmOptions;
use crate::lsm::Manifest;
use crate::lsm::Preloader;
use crate::runtime;
use crate::util::hexlify;
use crate::JournalEntry;
use crate::JournalSeq;
use crate::KeyspaceId;
use crate::Mutation;
use crate::RevisionValue;
use crate::TabletId;
use crate::TabletJournalEntry;
use crate::Timestamp;

pub(super) struct ShardRecovery {
    lsm_options: LsmOptions,
    storage: Arc<dyn runtime::Storage>,

    tablets: HashMap<TabletId, TabletRecovery>,
}

impl ShardRecovery {
    pub fn empty(lsm_options: LsmOptions, storage: Arc<dyn runtime::Storage>) -> ShardRecovery {
        ShardRecovery {
            lsm_options,
            storage,
            tablets: HashMap::new(),
        }
    }

    pub fn from_manifests(
        lsm_options: LsmOptions,
        storage: Arc<dyn runtime::Storage>,
        manifests: HashMap<TabletId, Manifest>,
    ) -> ShardRecovery {
        let mut recovery = ShardRecovery::empty(lsm_options.clone(), Arc::clone(&storage));
        for (tablet_id, manifest) in manifests {
            recovery.tablets.insert(
                tablet_id,
                TabletRecovery::from_manifest(lsm_options.clone(), Arc::clone(&storage), manifest),
            );
        }
        recovery
    }

    pub fn process(&mut self, seqno: JournalSeq, entry: JournalEntry) {
        let tablet = self.tablets.entry(entry.tablet_id).or_insert_with(|| {
            TabletRecovery::empty(self.lsm_options.clone(), Arc::clone(&self.storage))
        });
        tablet.process(seqno, entry.entry);
    }

    pub async fn wait(self) -> anyhow::Result<HashMap<TabletId, Lsm>> {
        let mut lsms = HashMap::new();
        for (tablet_id, tablet_recovery) in self.tablets.into_iter() {
            lsms.insert(tablet_id, tablet_recovery.wait().await?);
        }
        Ok(lsms)
    }
}

struct TabletRecovery {
    writes: VecDeque<(
        JournalSeq,
        Timestamp,
        Vec<(KeyspaceId, Vec<u8>, RevisionValue)>,
    )>,
    preloader: Preloader,
    lsm_options: LsmOptions,
    storage: Arc<dyn runtime::Storage>,
}

impl TabletRecovery {
    fn empty(lsm_options: LsmOptions, storage: Arc<dyn runtime::Storage>) -> TabletRecovery {
        TabletRecovery {
            writes: VecDeque::new(),
            preloader: Preloader::new(Arc::clone(&storage)),
            lsm_options,
            storage,
        }
    }

    fn from_manifest(
        lsm_options: LsmOptions,
        storage: Arc<dyn runtime::Storage>,
        manifest: Manifest,
    ) -> TabletRecovery {
        let mut recovery = TabletRecovery::empty(lsm_options, storage);
        recovery.preloader.set_manifest(manifest);
        recovery
    }

    fn process(&mut self, seqno: JournalSeq, entry: TabletJournalEntry) {
        match entry {
            TabletJournalEntry::NoOp => {}
            TabletJournalEntry::Write(ts, kvs) => {
                self.writes.push_back((seqno, ts, kvs));
            }
            TabletJournalEntry::Manifest(included_seqno, manifest) => {
                let trim_to_idx = self
                    .writes
                    .binary_search_by_key(&included_seqno, |(seqno, _, _)| *seqno)
                    .unwrap_or_else(core::convert::identity);
                self.writes.drain(0..trim_to_idx);

                self.preloader.set_manifest(manifest);
            }
        }
    }

    async fn wait(self) -> anyhow::Result<Lsm> {
        let preloaded = self.preloader.load().await?;

        let lsm = Lsm::open(self.lsm_options, self.storage, preloaded);

        for (_, ts, kvs) in self.writes {
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

        Ok(lsm)
    }
}
