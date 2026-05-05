use anyhow::anyhow;

use crate::lsm::Manifest;
use crate::pb;
use crate::JournalSeq;
use crate::KeyspaceId;
use crate::Revision;
use crate::RevisionValue;
use crate::Timestamp;

#[derive(Clone, Debug)]
pub(crate) enum TabletJournalEntry {
    NoOp,
    Write(Timestamp, Vec<(KeyspaceId, Vec<u8>, RevisionValue)>),
    /// The given manifest contains at least all of the writes through this sequence number. It may
    /// contain more.
    Manifest(JournalSeq, Manifest),
}

impl From<TabletJournalEntry> for pb::internal::TabletJournalEntry {
    fn from(value: TabletJournalEntry) -> Self {
        Self {
            entry_type: Some(match value {
                TabletJournalEntry::NoOp => pb::internal::tablet_journal_entry::EntryType::NoOp(()),
                TabletJournalEntry::Write(ts, revisions) => {
                    pb::internal::tablet_journal_entry::EntryType::Write(
                        pb::internal::tablet_journal_entry::WriteEntry {
                            ts: ts.as_micros(),
                            revisions: revisions
                                .into_iter()
                                .map(|(keyspace_id, key_bytes, value)| {
                                    pb::Revision::from(Revision {
                                        key: (keyspace_id, key_bytes),
                                        ts,
                                        value,
                                    })
                                })
                                .collect(),
                        },
                    )
                }
                TabletJournalEntry::Manifest(lower_bound_seq, manifest) => {
                    pb::internal::tablet_journal_entry::EntryType::Manifest(
                        pb::internal::tablet_journal_entry::ManifestEntry {
                            lower_bound_seq: lower_bound_seq.0,
                            manifest: Some(manifest.into()),
                        },
                    )
                }
            }),
        }
    }
}

impl TryFrom<pb::internal::TabletJournalEntry> for TabletJournalEntry {
    type Error = anyhow::Error;

    fn try_from(value: pb::internal::TabletJournalEntry) -> Result<Self, Self::Error> {
        Ok(
            match value
                .entry_type
                .ok_or_else(|| anyhow!("missing entry_type"))?
            {
                pb::internal::tablet_journal_entry::EntryType::NoOp(_) => TabletJournalEntry::NoOp,
                pb::internal::tablet_journal_entry::EntryType::Write(write_entry_pb) => {
                    let ts = Timestamp::from_micros(write_entry_pb.ts);
                    let revisions = write_entry_pb
                        .revisions
                        .into_iter()
                        .map(|revision_pb| {
                            let revision = Revision::try_from(revision_pb)?;
                            if revision.ts != ts {
                                return Err(anyhow!(
                                    "ts mismatch between entry and revision: {:?} != {:?}",
                                    revision.ts,
                                    ts
                                ));
                            }
                            Ok((revision.key.0, revision.key.1, revision.value))
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?;

                    TabletJournalEntry::Write(ts, revisions)
                }
                pb::internal::tablet_journal_entry::EntryType::Manifest(manifest_entry) => {
                    let lower_bound_seq = JournalSeq(manifest_entry.lower_bound_seq);
                    let manifest = Manifest::try_from(
                        manifest_entry
                            .manifest
                            .ok_or_else(|| anyhow!("missing manifest"))?,
                    )?;

                    TabletJournalEntry::Manifest(lower_bound_seq, manifest)
                }
            },
        )
    }
}
