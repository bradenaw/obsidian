use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::RwLock;

use anyhow::anyhow;
use async_trait::async_trait;
use bitmask_enum::bitmask;

use crate::obsidian::TabletId;
use crate::range::Range;
use crate::range::RangeSet;
use crate::types::ColoGroupId;
use crate::types::InternalError;
use crate::types::KeyspaceId;
use crate::types::Timestamp;

#[async_trait]
pub(crate) trait Meta {
    async fn create_colo_group(&self) -> anyhow::Result<ColoGroupId>;
    async fn create_keyspace(&self, colo_group_id: ColoGroupId) -> anyhow::Result<KeyspaceId>;

    async fn transition(
        &self,
        tablet_id: TabletId,
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
        expected_ts: Timestamp,
        next: TabletState,
    ) -> Result<Timestamp, InternalError>;

    async fn start_transfer(
        &self,
        transfer_id: TransferId,
        srcs: Vec<TabletId>,
        dsts: Vec<TabletId>,
    ) -> anyhow::Result<()>;
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Copy)]
pub(crate) struct TransferId(uuid::Uuid);

impl std::fmt::Display for TransferId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::fmt::Debug for TransferId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "xfer:")?;
        std::fmt::Display::fmt(self, f)
    }
}

// State properties shown as [crw] for complete, readable, writable on states that have any.
//
// In a range transfer, all source tablets start at Empty and all destinations start at
// Active. The goal is to get destinations to Active and sources to Inactive.
//
//                  ┌───────┐           ┌───────────┐                                             //
//                  │ Empty ├──────────>│ Hydrating ├──────────────────────────┐                  //
//                  └───┬───┘           └─────┬─────┘                          │                  //
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
//                                         v     │                             v                  //
//                                    ┌──────────┴─────┐                  ┌──────────┐            //
//                                    │ Frozen   [cr_] ├─────────────────>│ Inactive │            //
//                                    └────────────────┘        │         └────┬─────┘            //
//                                                       all dest Prepared     │                  //
//                                                                             │                  //
//                                                     retention window passes╶│                  //
//                                                                             v                  //
//                                                                        ┌─────────┐             //
//                                                                        │ Dropped │             //
//                                                                        └─────────┘             //
//
//
// And a state machine of the entire transfer, with sources on the left and destinations on the
// right. Note that there is always at least one [c**] tablet, and [**w] never exists alongside a
// separate [c**]. This is an example of a move with one source and one destination. m:n transfers
// are similar.
//
//                                                                                                //
//      ┌──────────────────────┐                                                                  //
//      │ Active [crw] │ Empty │                                                                  //
//      └───────────┬──────────┘                                                                  //
//                  │                                                                             //
//                  v                                                                             //
//    ┌──────────────────────────┐                                                                //
//    │ Active [crw] │ Hydrating ├────────────────────────────────────────────────┐               //
//    └──────────┬───────────────┘                                                │               //
//               │     ^                                                          │               //
//               v     │                                                          v               //
//    ┌────────────────┴─────────┐    ┌─────────────────────────┐    ┌─────────────────────────┐  //
//    │ Frozen [cr_] │ Hydrating ├───>│ Frozen [cr_] │ Inactive ├───>│ Active [crw] │ Inactive │  //
//    └─────────────┬────────────┘    └─────────────────────────┘    └─────────────────────────┘  //
//                  │                               ^                    (Transfer Aborted)       //
//                  v                               │                                             //
//  ┌───────────────────────────────┐               │                                             //
//  │ Frozen [cr_] │ Prepared [c__] ├───────────────┘                                             //
//  └───────────────┬───────────────┘                                                             //
//                  │                                                                             //
//                  v                                                                             //
//     ┌───────────────────────────┐                                                              //
//     │ Inactive │ Prepared [c__] │    Once any destination reaches inactive, it is no longer    //
//     └────────────┬──────────────┘    possible to abort                                         //
//                  │                                                                             //
//                  v                                                                             //
//      ┌─────────────────────────┐                                                               //
//      │ Inactive │ Active [crw] │                                                               //
//      └─────────────────────────┘                                                               //
//        * Transfer Succeeded! *                                                                 //
#[derive(Eq, PartialEq, Clone, Copy)]
pub(crate) enum TabletState {
    Empty,
    Hydrating,
    Prepared,
    Active,
    Frozen,
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
        fn no(states: &Vec<TabletState>, a: TabletState) -> bool {
            !states.iter().any(|tablet_state| *tablet_state == a)
        }

        match (self, next, transfer_states) {
            // Only actually allowed when the keyspace is brand new and has no tablets yet, but
            // that's handled separately.
            (TabletState::Empty, TabletState::Active, None) => true,

            // Transfer happy path for dsts:
            (TabletState::Empty, TabletState::Hydrating, Some(_)) => true,
            (TabletState::Hydrating, TabletState::Prepared, Some((src_states, _))) => {
                all(&src_states, TabletState::Frozen)
            }
            (TabletState::Prepared, TabletState::Active, Some((src_states, _))) => {
                all(&src_states, TabletState::Inactive)
            }

            // Transfer happy path for srcs:
            (TabletState::Active, TabletState::Frozen, Some((_, dst_states))) => {
                all(&dst_states, TabletState::Hydrating)
            }
            (TabletState::Frozen, TabletState::Inactive, Some((src_states, dst_states))) => {
                all(&dst_states, TabletState::Prepared)
                    && all_either(&src_states, TabletState::Frozen, TabletState::Inactive)
            }

            // Transfer cancel for dsts:
            (TabletState::Hydrating, TabletState::Inactive, Some(_)) => true,
            (TabletState::Prepared, TabletState::Inactive, Some((src_states, _))) => {
                // Transfer is committed and must continue if any source reaches inactive.
                no(&src_states, TabletState::Inactive)
            }

            // Transfer cancel for srcs:
            (TabletState::Frozen, TabletState::Active, Some((src_states, dst_states))) => {
                // Transfer is committed and must continue if any source reaches inactive.
                no(&src_states, TabletState::Inactive) && no(&dst_states, TabletState::Prepared)
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
    keyspaces_by_group: BTreeMap<ColoGroupId, BTreeSet<KeyspaceId>>,

    tablets_by_group: BTreeMap<ColoGroupId, BTreeSet<TabletId>>,
    tablets: BTreeMap<
        TabletId,
        (
            ColoGroupId,
            Range<Vec<u8>>,
            Timestamp,
            Timestamp,
            TabletState,
        ),
    >,

    transfers: BTreeMap<TransferId, (Vec<TabletId>, Vec<TabletId>)>,
    transfer_locks: BTreeMap<TabletId, TransferId>,
    ts: Timestamp,
}

#[async_trait]
impl Meta for MemMeta {
    async fn create_colo_group(&self) -> anyhow::Result<ColoGroupId> {
        let mut inner = self.inner.write().unwrap();
        let highest_in_use = inner
            .keyspaces_by_group
            .last_key_value()
            .map(|(colo_group_id, _)| *colo_group_id)
            .unwrap_or(ColoGroupId(0));

        let colo_group_id = ColoGroupId(highest_in_use.0 + 1);

        if colo_group_id.is_reserved() {
            return Err(anyhow!(
                "cannot allocate any more colo groups: ID space exhausted"
            ));
        }

        inner
            .keyspaces_by_group
            .insert(colo_group_id, BTreeSet::new());

        Ok(colo_group_id)
    }

    async fn create_keyspace(&self, colo_group_id: ColoGroupId) -> anyhow::Result<KeyspaceId> {
        let mut inner = self.inner.write().unwrap();

        let keyspaces = inner
            .keyspaces_by_group
            .get_mut(&colo_group_id)
            .ok_or_else(|| anyhow!("cannot find {:?}", colo_group_id))?;

        let highest_in_use = keyspaces
            .last()
            .map(|keyspace_id| keyspace_id.1)
            .unwrap_or(0);

        let keyspace_id = KeyspaceId(colo_group_id, highest_in_use + 1);

        if !keyspace_id.is_userland() {
            return Err(anyhow!("cannot allocate keyspace ID: ID space exhausted"));
        }

        keyspaces.insert(keyspace_id);

        Ok(keyspace_id)
    }

    async fn transition(
        &self,
        tablet_id: TabletId,
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
        expected_ts: Timestamp,
        next_state: TabletState,
    ) -> Result<Timestamp, InternalError> {
        let mut inner = self.inner.write().unwrap();

        let (prev_ts, curr_ts, curr_state) = match inner.tablets.get(&tablet_id) {
            Some((existing_colo_group_id, existing_range, prev_ts, curr_ts, curr_state)) => {
                if *existing_colo_group_id != colo_group_id {
                    return Err(InternalError::TransitionFatal(anyhow!(
                        "mismatched keyspace_id"
                    )));
                }
                if existing_range != &range {
                    return Err(InternalError::TransitionFatal(anyhow!("mismatched range")));
                }
                (*prev_ts, *curr_ts, *curr_state)
            }
            None => {
                if expected_ts != Timestamp::ZERO {
                    return Err(InternalError::TransitionFatal(anyhow!(
                        "illegal transition: nonexistent tablet with expected_ts!=0",
                    )));
                }
                if next_state != TabletState::Active {
                    return Err(InternalError::TransitionFatal(anyhow!(
                        "illegal transition: expected_ts=0 with next_state!=Active"
                    )));
                }
                if !inner
                    .keyspaces_by_group
                    .get(&colo_group_id)
                    .ok_or_else(|| anyhow!("{:?} does not exist", colo_group_id))?
                    .is_empty()
                {
                    return Err(InternalError::TransitionFatal(anyhow!(
                        "illegal transition: Empty->Active with non-empty keyspace"
                    )));
                }

                (Timestamp::MAX, Timestamp::ZERO, TabletState::Empty)
            }
        };

        if expected_ts == prev_ts {
            if curr_state == next_state {
                return Ok(curr_ts);
            } else {
                return Err(InternalError::TransitionFatal(anyhow!(
                    "meta out of sync: {:?} already transitioned but to a different state",
                    tablet_id,
                )));
            }
        }
        if expected_ts != curr_ts {
            return Err(InternalError::TransitionFatal(anyhow!(
                "meta out of sync: timestamp mismatch"
            )));
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
            return Err(InternalError::TransitionRejected(anyhow!(
                "illegal transition: not allowed by state machine"
            )));
        }

        let ranges_and_states: Vec<_> = inner
            .tablets_by_group
            .get(&colo_group_id)
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
                    return Err(InternalError::TransitionRejected(anyhow!(
                        "illegal transition: multiple tablets writable for some range"
                    )));
                }
                writable.add_range(range.clone());
            }
        }

        if !complete_range_set.is_covering() {
            return Err(InternalError::TransitionRejected(anyhow!(
                "illegal transition: some range has no complete tablets"
            )));
        }

        if writable.intersects(&complete_not_writable) {
            return Err(InternalError::TransitionRejected(anyhow!(
                "illegal transition: some range has a writable tablet and a complete tablet"
            )));
        }

        inner.ts = Timestamp::now_after(inner.ts);
        let new_ts = inner.ts;

        if curr_state == TabletState::Empty {
            inner
                .tablets_by_group
                .get_mut(&colo_group_id)
                .unwrap()
                .insert(tablet_id);
        }
        inner.tablets.insert(
            tablet_id,
            (colo_group_id, range, curr_ts, new_ts, next_state),
        );

        if let Some(transfer_id) = inner.transfer_locks.get(&tablet_id).copied() {
            let (srcs, dsts) = inner.transfers.get(&transfer_id).unwrap().clone();

            let src_states: Vec<_> = srcs
                .iter()
                .map(|tablet_id| inner.tablets.get(&tablet_id).unwrap().4)
                .collect();
            let dst_states: Vec<_> = dsts
                .iter()
                .map(|tablet_id| inner.tablets.get(&tablet_id).unwrap().4)
                .collect();

            let transfer_completed = src_states
                .iter()
                .all(|tablet_state| *tablet_state == TabletState::Inactive)
                && dst_states
                    .iter()
                    .all(|tablet_state| *tablet_state == TabletState::Active);
            let transfer_aborted = src_states
                .iter()
                .all(|tablet_state| *tablet_state == TabletState::Active)
                && dst_states
                    .iter()
                    .all(|tablet_state| *tablet_state == TabletState::Inactive);

            if transfer_completed || transfer_aborted {
                for other_tablet_id in srcs.iter().chain(dsts.iter()) {
                    inner.transfer_locks.remove(&other_tablet_id);
                }
                inner.transfers.remove(&transfer_id);
            }
        }

        return Ok(new_ts);
    }

    async fn start_transfer(
        &self,
        transfer_id: TransferId,
        srcs: Vec<TabletId>,
        dsts: Vec<TabletId>,
    ) -> anyhow::Result<()> {
        let mut inner = self.inner.write().unwrap();

        if srcs.is_empty() {
            return Err(anyhow!("can't transfer with empty srcs"));
        }
        if dsts.is_empty() {
            return Err(anyhow!("can't transfer with empty dsts"));
        }

        for src in &srcs {
            if dsts.contains(&src) {
                return Err(anyhow!("{:?} appears in both srcs and dsts", src));
            }
        }
        for dst in &dsts {
            if srcs.contains(&dst) {
                return Err(anyhow!("{:?} appears in both srcs and dsts", dst));
            }
        }

        if inner.transfers.contains_key(&transfer_id) {
            return Err(anyhow!("{:?} already exists", transfer_id));
        }
        let mut maybe_colo_group_id = None;
        for tablet_id in srcs.iter().chain(dsts.iter()) {
            let colo_group_id = inner
                .tablets
                .get(&tablet_id)
                .ok_or_else(|| anyhow!("{:?} not found", tablet_id))?
                .0;

            if let Some(expected_colo_group_id) = maybe_colo_group_id {
                if colo_group_id != expected_colo_group_id {
                    return Err(anyhow!(
                        "can't transfer between tablets of different colo groups {:?} and {:?}",
                        colo_group_id,
                        expected_colo_group_id,
                    ));
                }
            } else {
                maybe_colo_group_id = Some(colo_group_id);
            }

            if inner.transfer_locks.contains_key(tablet_id) {
                return Err(anyhow!("transfer already active for {:?}", tablet_id));
            }
        }

        for tablet_id in srcs.iter().chain(dsts.iter()) {
            inner.transfer_locks.insert(*tablet_id, transfer_id);
        }
        inner.transfers.insert(transfer_id, (srcs, dsts));

        Ok(())
    }
}
