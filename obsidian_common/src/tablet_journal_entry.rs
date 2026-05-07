use crate::Manifest;
use crate::JournalSeq;
use crate::KeyspaceId;
use crate::RevisionValue;
use crate::Timestamp;

#[derive(Clone, Debug)]
pub enum TabletJournalEntry {
    NoOp,
    Write(Timestamp, Vec<(KeyspaceId, Vec<u8>, RevisionValue)>),
    /// The given manifest contains at least all of the writes through this sequence number. It may
    /// contain more.
    Manifest(JournalSeq, Manifest),
}
