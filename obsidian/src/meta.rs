use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::RwLock;

use bitmask_enum::bitmask;

use crate::obsidian::TabletId;
use crate::range::Range;
use crate::range::RangeSet;
use crate::types::KeyspaceId;
use crate::types::Timestamp;

pub(crate) trait Meta {
    fn create_keyspace(&self) -> anyhow::Result<KeyspaceId>;

    //fn sync(
    //    cursor: Timestamp,
    //) -> anyhow::Result<(Vec<(Timestamp, KeyspaceId, Vec<u8>, Mutation)>, Timestamp)>;

    fn transition(
        &self,
        tablet_id: TabletId,
        keyspace_id: KeyspaceId,
        range: Range<Vec<u8>>,
        expected_ts: Timestamp,
        next: TabletState,
    ) -> anyhow::Result<Timestamp>;

    fn start_transfer(
        &self,
        transfer_id: TransferId,
        srcs: Vec<TabletId>,
        dsts: Vec<TabletId>,
    ) -> anyhow::Result<()>;
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Copy)]
pub(crate) struct TransferId(uuid::Uuid);

// State properties shown as [crw] for complete, readable, writable on states that have any.
//
// In a range transfer, all source tablets start at Empty and all destinations start at
// Active. The goal is to get destinations to Active and sources to Inactive.
//
//                  ┌───────┐         ┌────────────────┐                                          //
//                  │ Empty ├─────────│ Hydrating      ├───────────────────────┐                  //
//                  └───┬───┘         └───────┬────────┘                       │                  //
//                      │                     │                                │                  //
//                      │                     │╴all src Frozen, caught up      │                  //
//                      │                     │                                │                  //
//                      │                     v                                │                  //
//                      │             ┌────────────────┐                       │                  //
//         new keyspace╶│             │ Prepared [c__] ├───────────────────────┤                  //
//                      │             └───────┬────────┘                       │                  //
//                      │                     │                                │                  //
//                      │    all src Inactive╶│                                │                  //
//                      │                     │                                │                  //
//                      │                     v                                │                  //
//                      │             ┌────────────────┐       cancel transfer╶│                  //
//                      └────────────>│ Active   [crw] │                       │                  //
//                                    └────┬───────────┘                       │                  //
//                                         │     ^                             │                  //
//    all dest Hydrating, nearly caught up╶│     │                             │                  //
//                                         │     │                             │                  //
//                                         │     │╴cancel transfer             │                  //
//                                         v     │                             │                  //
//                                    ┌──────────┴─────┐                       │                  //
//                                    │ Frozen   [cr_] │                       │                  //
//                                    └───────┬────────┘                       │                  //
//                                            │                                │                  //
//                          all dest Prepared╶│                                │                  //
//                                            │                                │                  //
//                                            v           all src Handoff      v                  //
//                                    ┌────────────────┐         ╷        ┌──────────┐            //
//                                    │ Handoff  [c__] ├─────────────────>│ Inactive │            //
//                                    └────────────────┘                  └────┬─────┘            //
//                                                                             │                  //
//                                                     retention window passes╶│                  //
//                                                                             │                  //
//                                                                             v                  //
//                                                                        ┌──────────┐            //
//                                                                        │ Dropped  │            //
//                                                                        └──────────┘            //
#[derive(Eq, PartialEq, Clone, Copy)]
pub(crate) enum TabletState {
    Empty,
    Hydrating,
    Prepared,
    Active,
    Frozen,
    Handoff,
    Inactive,
    Dropped,
}

impl TabletState {
    fn can_transition(
        self,
        next: TabletState,
        transfer_states: Option<(Vec<TabletState>, Vec<TabletState>)>,
    ) -> bool {
        fn all(states: &Vec<TabletState>, state: TabletState) -> bool {
            states.iter().all(|tablet_state| *tablet_state == state)
        }
        fn all_either(states: &Vec<TabletState>, a: TabletState, b: TabletState) -> bool {
            states
                .iter()
                .all(|tablet_state| *tablet_state == a || *tablet_state == b)
        }
        fn no(states: &Vec<TabletState>, a: TabletState, b: TabletState) -> bool {
            !states
                .iter()
                .any(|tablet_state| *tablet_state == a || *tablet_state == b)
        }

        match (self, next, transfer_states) {
            // Only actually allowed when the keyspace is brand new and has no tablets yet, but
            // that's handled separately.
            (TabletState::Empty, TabletState::Active, None) => true,
            (TabletState::Empty, TabletState::Hydrating, Some(_)) => true,
            (TabletState::Hydrating, TabletState::Inactive, Some(_)) => true,
            (TabletState::Hydrating, TabletState::Prepared, Some((src_states, _))) => {
                all(&src_states, TabletState::Frozen)
            }
            (TabletState::Prepared, TabletState::Active, Some((src_states, _))) => {
                all(&src_states, TabletState::Inactive)
            }
            (TabletState::Prepared, TabletState::Inactive, Some((src_states, _))) => {
                no(&src_states, TabletState::Handoff, TabletState::Inactive)
            }
            (TabletState::Active, TabletState::Frozen, Some((_, dst_states))) => {
                all(&dst_states, TabletState::Hydrating)
            }
            (TabletState::Frozen, TabletState::Active, Some((src_states, _))) => {
                no(&src_states, TabletState::Handoff, TabletState::Inactive)
            }
            (TabletState::Frozen, TabletState::Handoff, Some((_, dst_states))) => {
                all(&dst_states, TabletState::Prepared)
            }
            (TabletState::Handoff, TabletState::Inactive, Some((src_states, _))) => {
                all_either(&src_states, TabletState::Handoff, TabletState::Inactive)
            }
            (TabletState::Inactive, TabletState::Dropped, None) => true,
            _ => false,
        }
    }

    fn properties(self) -> TabletStateProperties {
        match self {
            TabletState::Empty => TabletStateProperties::none(),
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
            TabletState::Handoff => TabletStateProperties::Complete,
            TabletState::Inactive => TabletStateProperties::none(),
            TabletState::Dropped => TabletStateProperties::none(),
        }
    }
}

#[bitmask(u8)]
pub(crate) enum TabletStateProperties {
    // Tablet has a complete copy of the data.
    Complete,
    // Tablet can be read from. Requires complete.
    Readable,
    // Tablet can be written to. Requires complete.
    Writable,
}

pub(crate) struct MemMeta {
    inner: RwLock<MemMetaInner>,
}

struct MemMetaInner {
    keyspaces: BTreeMap<KeyspaceId, BTreeSet<TabletId>>,
    tablets: BTreeMap<
        TabletId,
        (
            KeyspaceId,
            Range<Vec<u8>>,
            Timestamp,
            Timestamp,
            TabletState,
        ),
    >,
    transfers: BTreeMap<TransferId, (Vec<TabletId>, Vec<TabletId>)>,
    transfer_locks: BTreeMap<TabletId, TransferId>,
}

impl Meta for MemMeta {
    fn create_keyspace(&self) -> anyhow::Result<KeyspaceId> {
        todo!();
    }

    fn transition(
        &self,
        tablet_id: TabletId,
        keyspace_id: KeyspaceId,
        range: Range<Vec<u8>>,
        expected_ts: Timestamp,
        next_state: TabletState,
    ) -> anyhow::Result<Timestamp> {
        let inner = self.inner.write().unwrap();

        let (prev_ts, curr_ts, curr_state) = match inner.tablets.get(&tablet_id) {
            Some((existing_keyspace_id, existing_range, prev_ts, curr_ts, curr_state)) => {
                if *existing_keyspace_id != keyspace_id {
                    return Err(anyhow::anyhow!("meta out of sync"));
                }
                if existing_range != &range {
                    return Err(anyhow::anyhow!("meta out of sync"));
                }
                (*prev_ts, *curr_ts, *curr_state)
            }
            None => {
                if expected_ts != Timestamp::ZERO {
                    return Err(anyhow::anyhow!(
                        "illegal transition: nonexistent tablet with expected_ts!=0"
                    ));
                }
                if next_state != TabletState::Active {
                    return Err(anyhow::anyhow!(
                        "illegal transition: expected_ts=0 with next_state!=Active"
                    ));
                }
                if !inner
                    .keyspaces
                    .get(&keyspace_id)
                    .ok_or_else(|| anyhow::anyhow!("keyspace does not exist"))?
                    .is_empty()
                {
                    return Err(anyhow::anyhow!(
                        "illegal transition: Empty->Active with non-empty keyspace"
                    ));
                }

                (Timestamp::MAX, Timestamp::ZERO, TabletState::Empty)
            }
        };

        if expected_ts == prev_ts {
            if curr_state == next_state {
                return Ok(curr_ts);
            } else {
                return Err(anyhow::anyhow!("meta out of sync"));
            }
        }

        if expected_ts != curr_ts {
            return Err(anyhow::anyhow!("meta out of sync"));
        }

        let transfer_states = match inner.transfer_locks.get(&tablet_id) {
            Some(transfer_id) => {
                let (srcs, dsts) = inner.transfers.get(&transfer_id).unwrap();
                let src_states = srcs
                    .iter()
                    .map(|tablet_id| inner.tablets.get(&tablet_id).unwrap().4)
                    .collect();
                let dst_states = dsts
                    .iter()
                    .map(|tablet_id| inner.tablets.get(&tablet_id).unwrap().4)
                    .collect();
                Some((src_states, dst_states))
            }
            None => None,
        };

        if !curr_state.can_transition(next_state, transfer_states) {
            return Err(anyhow::anyhow!(
                "illegal transition: not allowed by state machine"
            ));
        }

        let ranges_and_states: Vec<_> = inner
            .keyspaces
            .get(&keyspace_id)
            .unwrap()
            .iter()
            .map(|other_tablet_id| {
                if *other_tablet_id == tablet_id {
                    (&range, next_state)
                } else {
                    let (_, range, _, _, tablet_state) =
                        inner.tablets.get(other_tablet_id).unwrap();
                    (range, *tablet_state)
                }
            })
            .collect();

        let mut complete_range_set = RangeSet::new();
        let mut complete_not_writable = RangeSet::new();
        let mut writable = RangeSet::new();
        for (range, tablet_state) in ranges_and_states {
            if tablet_state
                .properties()
                .contains(TabletStateProperties::Complete)
            {
                if !tablet_state
                    .properties()
                    .contains(TabletStateProperties::Writable)
                {
                    complete_not_writable.add_range(range.clone());
                }
                complete_range_set.add_range(range.clone());
            }
            if tablet_state
                .properties()
                .contains(TabletStateProperties::Writable)
            {
                if RangeSet::from(range.clone()).intersects(&writable) {
                    return Err(anyhow::anyhow!(
                        "illegal transition: multiple tablets writable for some range"
                    ));
                }
                writable.add_range(range.clone());
            }
        }

        if !complete_range_set.is_covering() {
            return Err(anyhow::anyhow!(
                "illegal transition: no tablets complete for some range"
            ));
        }

        if writable.intersects(&complete_not_writable) {
            return Err(anyhow::anyhow!(
                "illegal transition: tablet writable for some range while other tablet complete"
            ));
        }

        // todo:
        // - check to see if this finished a transfer
        // - actually apply the transition

        todo!();
    }

    fn start_transfer(
        &self,
        transfer_id: TransferId,
        srcs: Vec<TabletId>,
        dsts: Vec<TabletId>,
    ) -> anyhow::Result<()> {
        todo!();
    }
}
