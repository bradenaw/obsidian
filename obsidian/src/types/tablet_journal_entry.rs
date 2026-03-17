use crate::lsm::Manifest;
use crate::KeyspaceId;
use crate::RevisionValue;
use crate::Timestamp;
use crate::WalSeq;

#[derive(Clone, Debug)]
pub(crate) enum TabletJournalEntry {
    NoOp,
    Write(Timestamp, Vec<(KeyspaceId, Vec<u8>, RevisionValue)>),
    /// The given manifest contains at least all of the writes through this sequence number. It may
    /// contain more.
    Manifest(WalSeq, Manifest),
}
