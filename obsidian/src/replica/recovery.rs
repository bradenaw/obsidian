use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::anyhow;

use crate::lsm::Lsm;
use crate::lsm::LsmOptions;
use crate::lsm::Manifest;
use crate::lsm::Preloader;
use crate::replica::replica::ShardEntry;
use crate::runtime;
use crate::util::hexlify;
use crate::KeyspaceId;
use crate::Mutation;
use crate::RevisionValue;
use crate::TabletId;
use crate::Timestamp;
use crate::WalEntry;
use crate::WalSeq;

pub(super) struct ShardRecovery {
    tablets: HashMap<TabletId, TabletRecovery>,
}

impl ShardRecovery {
    pub fn empty() -> ShardRecovery {
        todo!();
    }

    pub fn from_manifests(manifests: HashMap<TabletId, Manifest>) -> ShardRecovery {
        todo!();
    }

    pub fn process(&mut self, seqno: WalSeq, entry: ShardEntry) {
        let tablet = self
            .tablets
            .entry(entry.tablet_id)
            .or_insert_with(TabletRecovery::empty);
        tablet.process(seqno, entry.entry);
    }

    pub fn wait(self) -> HashMap<TabletId, Lsm> {
        todo!();
    }
}

struct TabletRecovery {
    writes: VecDeque<(WalSeq, Timestamp, Vec<(KeyspaceId, Vec<u8>, RevisionValue)>)>,
    preloader: Preloader,
    lsm_options: LsmOptions,
    storage: Arc<dyn runtime::Storage>,
}

impl TabletRecovery {
    fn empty() -> TabletRecovery {
        todo!();
    }

    fn from_manifest(manifest: Manifest) -> TabletRecovery {
        todo!();
    }

    fn process(&mut self, seqno: WalSeq, entry: WalEntry) {
        match entry {
            WalEntry::NoOp => {}
            WalEntry::Write(ts, kvs) => {
                self.writes.push_back((seqno, ts, kvs));
            }
            WalEntry::Manifest(included_seqno, manifest) => {
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

        let lsm = Lsm::open(self.lsm_options, self.storage, preloaded).await?;

        for (_, ts, kvs) in self.writes {
            for (keyspace_id, key, value) in kvs {
                // It's possible that this revision is already present since the seqno in
                // WalEntry::Manifest is a lower bound, the manifest may already contain newer
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
