use std::fmt::Debug;

use anyhow::anyhow;
use bitmask_enum::bitmask;

use crate::pb;

// State properties shown as [crw] for complete, readable, writable on states that have any.
//
// In a range transfer, the source tablet starts at Active and the destination starts at None. The
// goal is to get the source to None and the destination to Active.
//
//
// TODO: replace prepared with frozen
//                                           ┌──────┐                                             //
//                   ┌───────────────────────┤ None │<────────────────────────┐                   //
//                   │                       └───┬──┘                         │                   //
//                   │                           │                            │                   //
//                   │                           │                            │                   //
//                   │╴new colo group            v                            │                   //
//                   │                     ┌───────────┐                      │                   //
//                   │                     │ Hydrating ├──────────────────────┤                   //
//                   │                     └─────┬─────┘           │          │                   //
//                   │                           │               abort        │                   //
//                   │                           │                            │                   //
//                   │                           │╴src Frozen, all caught up  │                   //
//                   │                           v                            │                   //
//                   │                   ┌────────────────┐                   │                   //
//                   │                   │ Prepared [c__] ├───────────────────┤                   //
//                   │                   └───────┬────────┘        │          │                   //
//                   │                           │               abort        │                   //
//                   │                  src None╶│                            │                   //
//                   │                           │                            │                   //
//                   │                           v                            │                   //
//                   │                   ┌────────────────┐                   │                   //
//                   └──────────────────>│ Active   [crw] │                   │                   //
//                                       └────┬───────────┘                   │                   //
//                                            │     ^                         │                   //
//            dst Hydrating, nearly caught up╶│     │                         │                   //
//                                            │     │                         │                   //
//                                            │     │╴cancel transfer         │                   //
//                                            v     │                         │                   //
//                                       ┌──────────┴─────┐                   │                   //
//                                       │ Frozen   [cr_] ├───────────────────┘                   //
//                                       └────────────────┘         │                             //
//                                                            dst Prepared                        //
//
//
// And a state machine of the entire transfer, with souce on the left and destination on the right.
// Note that there is always at least one [c**] tablet, and [**w] never exists alongside a separate
// [c**].
//
// TODO: replace prepared with frozen, add transfer states
//            src         dst                                                                     //
//       ┌─────────────────────┐                                                                  //
//       │ Active [crw] │ None │                                                                  //
//       └──────────┬──────────┘                                                                  //
//                  │                                                                             //
//                  v                                                                             //
//    ┌──────────────────────────┐                                                                //
//    │ Active [crw] │ Hydrating ├──────────────────────────────────────────┐                     //
//    └──────────┬───────────────┘                                          │                     //
//               │     ^                                                    │                     //
//               v     │                                                    v                     //
//    ┌────────────────┴─────────┐    ┌─────────────────────┐    ┌─────────────────────┐          //
//    │ Frozen [cr_] │ Hydrating ├───>│ Frozen [cr_] │ None ├───>│ Active [crw] │ None │          //
//    └─────────────┬────────────┘    └─────────────────────┘    └─────────────────────┘          //
//                  │                            ^                  (Transfer Aborted)            //
//                  v                            │                                                //
//  ┌───────────────────────────────┐            │                                                //
//  │ Frozen [cr_] │ Prepared [c__] ├────────────┘                                                //
//  └───────────────┬───────────────┘                                                             //
//                  │                                                                             //
//                  v                                                                             //
//       ┌───────────────────────┐                                                                //
//       │ None │ Prepared [c__] │    Once destination reaches None, it is no longer              //
//       └──────────┬────────────┘    possible to abort                                           //
//                  │                                                                             //
//                  v                                                                             //
//        ┌─────────────────────┐                                                                 //
//        │ None │ Active [crw] │                                                                 //
//        └─────────────────────┘                                                                 //
//        * Transfer Succeeded! *                                                                 //
#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub(crate) enum TabletState {
    None,
    Hydrating,
    Prepared,
    Active,
    Frozen,
}

impl From<TabletState> for pb::internal::TabletState {
    fn from(value: TabletState) -> Self {
        match value {
            TabletState::None => pb::internal::TabletState::None,
            TabletState::Hydrating => pb::internal::TabletState::Hydrating,
            TabletState::Prepared => pb::internal::TabletState::Prepared,
            TabletState::Active => pb::internal::TabletState::Active,
            TabletState::Frozen => pb::internal::TabletState::Frozen,
        }
    }
}

impl TryFrom<pb::internal::TabletState> for TabletState {
    type Error = anyhow::Error;

    fn try_from(value: pb::internal::TabletState) -> Result<Self, Self::Error> {
        Ok(match value {
            pb::internal::TabletState::None => TabletState::None,
            pb::internal::TabletState::Hydrating => TabletState::Hydrating,
            pb::internal::TabletState::Prepared => TabletState::Prepared,
            pb::internal::TabletState::Active => TabletState::Active,
            pb::internal::TabletState::Frozen => TabletState::Frozen,
            _ => return Err(anyhow!("unrecognized TabletState {:?}", value)),
        })
    }
}

impl TabletState {
    pub(crate) fn can_transition(
        self,
        next: TabletState,
        transfer_states: Option<(TabletState, TabletState)>,
    ) -> bool {
        match (self, next, transfer_states) {
            // Only actually allowed when a colo group is created.
            // (TabletState::None, TabletState::Active, None) => true,

            // Transfer happy path for dst:
            (TabletState::None, TabletState::Hydrating, Some(_)) => true,
            (TabletState::Hydrating, TabletState::Prepared, Some((src_state, _))) => {
                src_state == TabletState::Frozen
            }
            (TabletState::Prepared, TabletState::Active, Some((src_state, _))) => {
                src_state == TabletState::None
            }

            // Transfer happy path for src:
            (TabletState::Active, TabletState::Frozen, Some((_, dst_state))) => {
                dst_state == TabletState::Hydrating
            }
            (TabletState::Frozen, TabletState::None, Some((_, dst_state))) => {
                dst_state == TabletState::Prepared
            }

            // Transfer cancel for dst:
            (TabletState::Hydrating, TabletState::None, Some(_)) => true,
            (TabletState::Prepared, TabletState::None, Some((src_state, _))) => {
                // Transfer is committed and must continue if src reaches None.
                src_state != TabletState::None
            }

            // Transfer cancel for src:
            (TabletState::Frozen, TabletState::Active, Some((_, dst_state))) => {
                dst_state != TabletState::Prepared
            }
            _ => false,
        }
    }

    pub(crate) fn properties(self) -> TabletStateProperties {
        match self {
            TabletState::None => TabletStateProperties::none(),
            TabletState::Hydrating => TabletStateProperties::none(),
            TabletState::Prepared => TabletStateProperties::Complete,
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
    Handoff,  // None       Frozen
    Complete, // None       Active
    Aborting, // Frozen     None
    Aborted,  // Active     None
}

pub(crate) enum TransferTabletTransition {
    Srcs(TabletState),
    Dsts(TabletState),
}

impl TransferState {
    pub(crate) fn tablet_states(&self) -> (TabletState, TabletState) {
        match self {
            TransferState::Copy => (TabletState::Active, TabletState::Hydrating),
            TransferState::Catchup => (TabletState::Frozen, TabletState::Hydrating),
            TransferState::Synced => (TabletState::Frozen, TabletState::Frozen),
            TransferState::Handoff => (TabletState::None, TabletState::Frozen),
            TransferState::Complete => (TabletState::None, TabletState::Active),
            TransferState::Aborting => (TabletState::Frozen, TabletState::None),
            TransferState::Aborted => (TabletState::Active, TabletState::None),
        }
    }

    pub(crate) fn can_transition(&self, to: &Self) -> bool {
        match (self, to) {
            (TransferState::Copy, TransferState::Catchup) => true,
            (TransferState::Catchup, TransferState::Synced) => true,
            (TransferState::Synced, TransferState::Handoff) => true,
            (TransferState::Handoff, TransferState::Complete) => true,

            (TransferState::Copy, TransferState::Aborted) => true,
            (TransferState::Catchup, TransferState::Aborted) => true,
            (TransferState::Synced, TransferState::Aborting) => true,
            (TransferState::Aborting, TransferState::Aborted) => true,

            _ => false,
        }
    }

    pub(crate) fn tablet_transition(&self, to: &Self) -> Option<TransferTabletTransition> {
        if !self.can_transition(to) {
            return None;
        }

        let (srcs_curr, dsts_curr) = self.tablet_states();
        let (srcs_next, dsts_next) = to.tablet_states();

        if srcs_curr != srcs_next && dsts_curr == dsts_next {
            return Some(TransferTabletTransition::Srcs(srcs_next));
        } else if srcs_curr == srcs_next && dsts_curr != dsts_next {
            return Some(TransferTabletTransition::Dsts(dsts_next));
        } else {
            return None;
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
            pb::internal::TransferState::Aborting => Self::Aborting,
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
            TransferState::Aborting => pb::internal::TransferState::Aborting,
            TransferState::Aborted => pb::internal::TransferState::Aborted,
        }
    }
}

#[bitmask(u8)]
#[bitmask_config(vec_debug)]
pub(crate) enum TabletStateProperties {
    // Tablet has a complete copy of the data.
    Complete = 0b00000100,
    // Tablet can be read from. Requires complete.
    Readable = 0b00000010,
    // Tablet can be written to. Requires complete.
    Writable = 0b00000001,
}
