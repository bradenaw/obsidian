use std::fmt::Debug;

use anyhow::anyhow;
use obsidian_pb as pb;

use crate::meta::TabletState;

/// The state for a transfer of a key range from source tablet(s) to destination tablet(s).
///
/// Transfers take the form of move (1:1), split (1:n), and merge (n:1). We don't bother with m:n
/// transfers.
///
/// This is the state machine of a transfer, with the [`TabletState`] of source(s) on the left and
/// destination(s) on the right. Note that there is always at least one `[c**]` tablet (otherwise
/// we lose data), and `[**w]` never exists alongside a separate `[c**]` (otherwise the `[c**]`
/// tablet's complete status cannot be guaranteed).
///
/// See [`TabletState`] for an explanation of the `[crw]` notation.
///
/// ```text
///    ┌──────────────────────────┐                                                                 
///    │           Copy           │                                                                 
///    ├──────────────────────────┤                                                                 
///    │ Active [crw] │ Hydrating ├───────────────┐                                                 
///    └─────────────┬────────────┘               │                                                 
///                  │                            │                                                 
///                  v                            v                                                 
///    ┌──────────────────────────┐    ┌────────────────────────┐                                   
///    │         Catchup          │    │         Aborted        │                                   
///    ├──────────────────────────┤    ├────────────────────────┤                                   
///    │ Frozen [cr_] │ Hydrating ├───>│ Active [crw] │ Defunct │                                   
///    └─────────────┬────────────┘    └────────────────────────┘                                   
///                  │                            ^                                                 
///                  v                            │                                                 
///   ┌─────────────────────────────┐             │                                                 
///   │            Synced           │             │                                                 
///   ├─────────────────────────────┤             │                                                 
///   │ Frozen [cr_] │ Frozen [cr_] ├─────────────┘                                                 
///   └──────────────┬──────────────┘                                                               
///                  │                                                                              
///                  v                                                                              
///      ┌────────────────────────┐                                                                 
///      │         Handoff        │                                                                 
///      ├────────────────────────┤                                                                 
///      │ Defunct │ Frozen [cr_] │                                                                 
///      └───────────┬────────────┘                                                                 
///                  │                                                                              
///                  v                                                                              
///      ┌────────────────────────┐                                                                 
///      │        Complete        │                                                                 
///      ├────────────────────────┤                                                                 
///      │ Defunct │ Active [crw] │                                                                 
///      └────────────────────────┘                                                                 
///        * Transfer Succeeded! *                                                                  
/// ```
#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub(crate) enum TransferState {
    //           src(s)     dst(s)
    /// The starting state for a transfer. The source is still serving normally and the
    /// destination is bulk-copying the data.
    Copy, //     Active     Hydrating
    /// The source is no longer accepting writes and the destination is copying the last of the
    /// changes.
    Catchup, //  Frozen     Hydrating
    /// Both source and destination are frozen, and both have matching full copies of the data.
    Synced, //   Frozen     Frozen
    /// The source relinquishes its copy of the data.
    Handoff, //  Defunct    Frozen
    /// The destination accepts writes.
    Complete, // Defunct    Active
    /// The transfer was cancelled, the source remains the authoritative copy of the data.
    Aborted, //  Active     Defunct
}

impl TransferState {
    pub fn tablet_states(&self) -> (TabletState, TabletState) {
        match self {
            TransferState::Copy => (TabletState::Active, TabletState::Hydrating),
            TransferState::Catchup => (TabletState::Frozen, TabletState::Hydrating),
            TransferState::Handoff => (TabletState::Defunct, TabletState::Frozen),
            TransferState::Synced => (TabletState::Frozen, TabletState::Frozen),
            TransferState::Complete => (TabletState::Defunct, TabletState::Active),
            TransferState::Aborted => (TabletState::Active, TabletState::Defunct),
        }
    }

    pub fn can_transition(&self, to: Self) -> bool {
        #[allow(clippy::match_like_matches_macro)]
        match (self, to) {
            (TransferState::Copy, TransferState::Catchup) => true,
            (TransferState::Catchup, TransferState::Synced) => true,
            (TransferState::Synced, TransferState::Handoff) => true,
            (TransferState::Handoff, TransferState::Complete) => true,

            (TransferState::Copy, TransferState::Aborted) => true,
            (TransferState::Catchup, TransferState::Aborted) => true,
            (TransferState::Handoff, TransferState::Aborted) => true,

            _ => false,
        }
    }
}

impl TryFrom<pb::internal::TransferState> for TransferState {
    type Error = anyhow::Error;

    fn try_from(value_pb: pb::internal::TransferState) -> Result<Self, Self::Error> {
        Ok(match value_pb {
            pb::internal::TransferState::Copy => Self::Copy,
            pb::internal::TransferState::Catchup => Self::Catchup,
            pb::internal::TransferState::Synced => Self::Synced,
            pb::internal::TransferState::Handoff => Self::Handoff,
            pb::internal::TransferState::Complete => Self::Complete,
            pb::internal::TransferState::Aborted => Self::Aborted,
            pb::internal::TransferState::Unknown => return Err(anyhow!("unknown TransferState")),
        })
    }
}

impl From<TransferState> for pb::internal::TransferState {
    fn from(value: TransferState) -> Self {
        match value {
            TransferState::Copy => pb::internal::TransferState::Copy,
            TransferState::Catchup => pb::internal::TransferState::Catchup,
            TransferState::Synced => pb::internal::TransferState::Synced,
            TransferState::Handoff => pb::internal::TransferState::Handoff,
            TransferState::Complete => pb::internal::TransferState::Complete,
            TransferState::Aborted => pb::internal::TransferState::Aborted,
        }
    }
}
