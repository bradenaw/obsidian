use std::fmt::Debug;

use anyhow::anyhow;
use bitmask_enum::bitmask;

use crate::pb;

// State properties shown as [hcrw] for hydrating, complete, readable, writable on states that have
// any.
//
// In a range transfer, the source tablet starts at Active and the destination starts at Hydrating.
// The goal is to get the source to Defunct and the destination to Active.
//
//                                          ┌─────────┐                                           //
//                                          │ Defunct │<──────────────────────┐                   //
//                                          └────┬────┘                       │                   //
//                                               │                            │                   //
//                                               v                            │                   //
//                                      ┌──────────────────┐                  │                   //
//       new transfer destination╶─────>│ Hydrating [h___] ├──────────────────┤                   //
//                                      └────────┬─────────┘                  │                   //
//                                               │                            │                   //
//                                               v                            │                   //
//                                       ┌─────────────────┐                  │                   //
//                                       │ Frozen   [_cr_] ├──────────────────┘                   //
//                                       └────┬────────────┘                                      //
//                                            │     ^                                             //
//                                            v     │                                             //
//                                       ┌──────────┴──────┐                                      //
//           new colo group╶────────────>│ Active   [_crw] │                                      //
//                                       └─────────────────┘                                      //
//
//
// And a state machine of the entire transfer, with source(s) on the left and destination(s) on the
// right. Note that there is always at least one [c**] tablet, and [**w] never exists alongside a
// separate [c**].
//
//    ┌──────────────────────────┐                                                                //
//    │           Copy           │                                                                //
//    ├──────────────────────────┤                                                                //
//    │ Active [crw] │ Hydrating ├───────────────┐                                                //
//    └─────────────┬────────────┘               │                                                //
//                  │                            │                                                //
//                  v                            v                                                //
//    ┌──────────────────────────┐    ┌────────────────────────┐                                  //
//    │         Catchup          │    │         Aborted        │                                  //
//    ├──────────────────────────┤    ├────────────────────────┤                                  //
//    │ Frozen [cr_] │ Hydrating ├───>│ Active [crw] │ Defunct │                                  //
//    └─────────────┬────────────┘    └────────────────────────┘                                  //
//                  │                            ^                                                //
//                  v                            │                                                //
//   ┌─────────────────────────────┐             │                                                //
//   │            Synced           │             │                                                //
//   ├─────────────────────────────┤             │                                                //
//   │ Frozen [cr_] │ Frozen [cr_] ├─────────────┘                                                //
//   └──────────────┬──────────────┘                                                              //
//                  │                                                                             //
//                  v                                                                             //
//      ┌────────────────────────┐                                                                //
//      │         Handoff        │                                                                //
//      ├────────────────────────┤                                                                //
//      │ Defunct │ Frozen [cr_] │                                                                //
//      └───────────┬────────────┘                                                                //
//                  │                                                                             //
//                  v                                                                             //
//      ┌────────────────────────┐                                                                //
//      │        Complete        │                                                                //
//      ├────────────────────────┤                                                                //
//      │ Defunct │ Active [crw] │                                                                //
//      └────────────────────────┘                                                                //
//        * Transfer Succeeded! *                                                                 //
#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub(crate) enum TabletState {
    Defunct,
    Hydrating,
    Active,
    Frozen,
}

impl From<TabletState> for pb::internal::TabletState {
    fn from(value: TabletState) -> Self {
        match value {
            TabletState::Defunct => pb::internal::TabletState::Defunct,
            TabletState::Hydrating => pb::internal::TabletState::Hydrating,
            TabletState::Active => pb::internal::TabletState::Active,
            TabletState::Frozen => pb::internal::TabletState::Frozen,
        }
    }
}

impl TryFrom<pb::internal::TabletState> for TabletState {
    type Error = anyhow::Error;

    fn try_from(value: pb::internal::TabletState) -> Result<Self, Self::Error> {
        Ok(match value {
            pb::internal::TabletState::Defunct => TabletState::Defunct,
            pb::internal::TabletState::Hydrating => TabletState::Hydrating,
            pb::internal::TabletState::Active => TabletState::Active,
            pb::internal::TabletState::Frozen => TabletState::Frozen,
            _ => return Err(anyhow!("unrecognized TabletState {:?}", value)),
        })
    }
}

impl TabletState {
    pub(crate) fn properties(self) -> TabletStateProperties {
        match self {
            TabletState::Defunct => TabletStateProperties::none(),
            TabletState::Hydrating => TabletStateProperties::Hydrating,
            TabletState::Active => {
                TabletStateProperties::Complete
                    | TabletStateProperties::Readable
                    | TabletStateProperties::Writable
            }
            TabletState::Frozen => {
                TabletStateProperties::Complete | TabletStateProperties::Readable
            }
        }
    }
}

#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub(crate) enum TransferState {
    //           src(s)     dst(s)
    Copy,     // Active     Hydrating
    Catchup,  // Frozen     Hydrating
    Synced,   // Frozen     Frozen
    Handoff,  // Defunct    Frozen
    Complete, // Defunct    Active
    Aborted,  // Active     Defunct
}

impl TransferState {
    pub(crate) fn tablet_states(&self) -> (TabletState, TabletState) {
        match self {
            TransferState::Copy => (TabletState::Active, TabletState::Hydrating),
            TransferState::Catchup => (TabletState::Frozen, TabletState::Hydrating),
            TransferState::Handoff => (TabletState::Defunct, TabletState::Frozen),
            TransferState::Synced => (TabletState::Frozen, TabletState::Frozen),
            TransferState::Complete => (TabletState::Defunct, TabletState::Active),
            TransferState::Aborted => (TabletState::Active, TabletState::Defunct),
        }
    }

    pub(crate) fn can_transition(&self, to: Self) -> bool {
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

#[bitmask(u8)]
#[bitmask_config(vec_debug)]
pub(crate) enum TabletStateProperties {
    // The tablet is hydrating with a transfer from another tablet.
    Hydrating = 0b00001000,
    // Tablet has a complete copy of the data.
    Complete = 0b00000100,
    // Tablet can be read from. Requires complete.
    Readable = 0b00000010,
    // Tablet can be written to. Requires complete.
    Writable = 0b00000001,
}
