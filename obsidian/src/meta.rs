use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::RwLock;

use anyhow::anyhow;
use async_trait::async_trait;
use bitmask_enum::bitmask;
use rand::Rng;

use crate::range::Range;
use crate::range::RangeMap;
use crate::types::ColoGroupId;
use crate::types::InternalError;
use crate::types::KeyspaceId;
use crate::types::TabletId;
use crate::types::Timestamp;
use crate::types::TransferId;

#[async_trait]
pub(crate) trait Meta {
    async fn create_colo_group(&self) -> anyhow::Result<ColoGroupId>;
    async fn create_keyspace(&self, colo_group_id: ColoGroupId) -> anyhow::Result<KeyspaceId>;

    async fn transition(
        &self,
        tablet_id: TabletId,
        range: (ColoGroupId, Range<Vec<u8>>),
        next: RangeState,
    ) -> Result<(), InternalError>;

    async fn start_transfer(
        &self,
        transfer_id: TransferId,
        src_range: (ColoGroupId, Range<Vec<u8>>),
        dst: TabletId,
    ) -> anyhow::Result<()>;
}

// State properties shown as [crw] for complete, readable, writable on states that have any.
//
// In a range transfer, the source tablet starts at Active and the destination starts at None. The
// goal is to get the source to None and the destination to Active.
//
//                  ┌──────┐                             ┌───────────┐                            //
//                  │ None ├────────────────────────────>│ Hydrating │                            //
//                  └───┬──┘                             └┬────┬─────┘                            //
//                   ^  │ ^                               │    │                                  //
//                   │  │ ├───────────────────────────────┘    │╴src Frozen, all caught up        //
//                   │  │ │                                    │                                  //
//                   │  │ │                                    v                                  //
//                   │  │ │                            ┌────────────────┐                         //
//                   │  │ └────────────────────────────┤ Prepared [c__] │                         //
//                   │  │╴new colo group               └───────┬────────┘                         //
//                   │  │                                      │                                  //
//                   │  │                             src None╶│                                  //
//                   │  │                                      │                                  //
//                   │  │                                      v                                  //
//                   │  │                              ┌────────────────┐                         //
//                   │  └─────────────────────────────>│ Active   [crw] │                         //
//                   │                                 └────┬───────────┘                         //
//                   │                                      │     ^                               //
//                   │      dst Hydrating, nearly caught up╶│     │                               //
//                   │                                      │     │                               //
//                   │                                      │     │╴cancel transfer               //
//                   │                                      v     │                               //
//                   │                                 ┌──────────┴─────┐                         //
//                   └─────────────────────────────────┤ Frozen   [cr_] │                         //
//                           │                         └────────────────┘                         //
//                     dst Prepared                                                               //
//
//
// And a state machine of the entire transfer, with souce on the left and destination on the right.
// Note that there is always at least one [c**] tablet, and [**w] never exists alongside a separate
// [c**].
//
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
pub(crate) enum RangeState {
    None,
    Hydrating,
    Prepared,
    Active,
    Frozen,
}

impl RangeState {
    fn can_transition(
        self,
        next: RangeState,
        transfer_states: Option<(RangeState, RangeState)>,
    ) -> bool {
        match (self, next, transfer_states) {
            // Only actually allowed when a colo group is created.
            // (RangeState::None, RangeState::Active, None) => true,

            // Transfer happy path for dst:
            (RangeState::None, RangeState::Hydrating, Some(_)) => true,
            (RangeState::Hydrating, RangeState::Prepared, Some((src_state, _))) => {
                src_state == RangeState::Frozen
            }
            (RangeState::Prepared, RangeState::Active, Some((src_state, _))) => {
                src_state == RangeState::None
            }

            // Transfer happy path for src:
            (RangeState::Active, RangeState::Frozen, Some((_, dst_state))) => {
                dst_state == RangeState::Hydrating
            }
            (RangeState::Frozen, RangeState::None, Some((_, dst_state))) => {
                dst_state == RangeState::Prepared
            }

            // Transfer cancel for dst:
            (RangeState::Hydrating, RangeState::None, Some(_)) => true,
            (RangeState::Prepared, RangeState::None, Some((src_state, _))) => {
                // Transfer is committed and must continue if src reaches None.
                src_state != RangeState::None
            }

            // Transfer cancel for src:
            (RangeState::Frozen, RangeState::Active, Some((_, dst_state))) => {
                dst_state != RangeState::Prepared
            }
            _ => false,
        }
    }

    fn properties(self) -> RangeStateProperties {
        match self {
            RangeState::None => RangeStateProperties::none(),
            RangeState::Hydrating => RangeStateProperties::none(),
            RangeState::Prepared => RangeStateProperties::Complete,
            RangeState::Active => {
                RangeStateProperties::Complete
                    | RangeStateProperties::Readable
                    | RangeStateProperties::Writable
            }
            RangeState::Frozen => RangeStateProperties::Complete | RangeStateProperties::Readable,
        }
    }
}

#[bitmask(u8)]
pub(crate) enum RangeStateProperties {
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
    tablets: BTreeMap<TabletId, BTreeMap<ColoGroupId, RangeMap<Vec<u8>, RangeState>>>,
    transfers: BTreeMap<TransferId, (ColoGroupId, Range<Vec<u8>>, TabletId, TabletId)>,
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

        let tablet_id = inner
            .tablets
            .iter()
            .skip(rand::thread_rng().gen_range(0..inner.tablets.len()))
            .next()
            .ok_or_else(|| anyhow!("no tablets"))?
            .0;

        inner
            .keyspaces_by_group
            .insert(colo_group_id, BTreeSet::new());

        let range_states = RangeMap::new();
        range_states.set(Range::all(), RangeState::Active);

        // TODO: Hm, how do we tell the tablet about this?
        inner
            .tablets
            .get_mut(&tablet_id)
            .unwrap()
            .insert(colo_group_id, range_states);

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
        range: (ColoGroupId, Range<Vec<u8>>),
        next_state: RangeState,
    ) -> Result<(), InternalError> {
        let mut inner = self.inner.write().unwrap();

        let curr_state = inner
            .tablets
            .get(&tablet_id)
            .ok_or_else(|| anyhow!("{:?} not found", tablet_id))?
            .get(&range.0)
            .map(|range_states| {
                let states = range_states.get_range(range.1);
                if states.is_empty() {
                    Ok(RangeState::None)
                } else if states.len() > 1 {
                    Err(anyhow!("more than one state for range"))
                } else {
                    Ok(*states[0])
                }
            })
            .transpose()?
            .unwrap_or(RangeState::None);

        if curr_state == next_state {
            return Ok(());
        }

        let transfer = {
            let mut transfer = None;
            for (transfer_id, (other_colo_group_id, other_range, src_tablet_id, dst_tablet_id)) in
                inner.transfers.iter()
            {
                if *other_colo_group_id != range.0 {
                    continue;
                }
                if other_range != &range.1 {
                    continue;
                }

                let src_state = inner.tablet_range_state(src_tablet_id, range.0, range.1)?;
                let dst_state = inner.tablet_range_state(dst_tablet_id, range.0, range.1)?;

                transfer = Some((
                    transfer_id,
                    src_tablet_id,
                    src_state,
                    dst_tablet_id,
                    dst_state,
                ));
                break;
            }
            transfer
        };

        if !curr_state.can_transition(
            next_state,
            transfer.map(|(_, _, src_state, _, dst_state)| (src_state, dst_state)),
        ) {
            return Err(InternalError::TransitionRejected(anyhow!(
                "illegal transition: not allowed by state machine"
            )));
        }

        inner.ts = Timestamp::now_after(inner.ts);
        let new_ts = inner.ts;

        inner
            .tablets
            .entry(tablet_id)
            .or_insert_with(BTreeMap::new)
            .entry(range.0)
            .or_insert_with(RangeMap::new)
            .set(range.1, next_state);

        if let Some((transfer_id, src_tablet_id, src_start_state, dst_tablet_id, dst_start_state)) =
            transfer
        {
            let src_end_state = if *src_tablet_id == tablet_id {
                next_state
            } else {
                src_start_state
            };
            let dst_end_state = if *dst_tablet_id == tablet_id {
                next_state
            } else {
                dst_start_state
            };

            let transfer_completed =
                src_end_state == RangeState::None && dst_end_state == RangeState::Active;
            let transfer_aborted =
                src_end_state == RangeState::Active && dst_end_state == RangeState::None;

            if transfer_completed || transfer_aborted {
                inner.transfers.remove(&transfer_id);
            }
        }

        return Ok(());
    }

    async fn start_transfer(
        &self,
        transfer_id: TransferId,
        src_range: (ColoGroupId, Range<Vec<u8>>),
        dst: TabletId,
    ) -> anyhow::Result<()> {
        let mut inner = self.inner.write().unwrap();

        if inner.transfers.contains_key(&transfer_id) {
            return Err(anyhow!("{:?} already exists", transfer_id));
        }

        if src_range.1.is_empty() {
            return Err(anyhow!("can't transfer with empty src range"));
        }

        for (other_transfer_id, (colo_group_id, range, _, _)) in inner.transfers {
            if colo_group_id != src_range.0 {
                continue;
            }
            if !src_range.1.intersection(&range).is_empty() {
                return Err(anyhow!(
                    "range {:?} already undergoing {:?}",
                    range,
                    other_transfer_id
                ));
            }
        }

        let src_tablet_id = {
            let mut src_tablet_id = None;
            for (tablet_id, range_states_by_colo_group_id) in inner.tablets {
                if let Some(range_states) = range_states_by_colo_group_id.get(&src_range.0) {
                    if range_states.get_range(src_range.1) == vec![&RangeState::Active] {
                        src_tablet_id = Some(tablet_id);
                    }
                }
            }
            src_tablet_id.ok_or_else(|| anyhow!("no tablet holds all of {:?}", src_range))?
        };

        if src_tablet_id == dst {
            return Err(anyhow!("src and dst are both {:?}", dst));
        }

        inner
            .transfers
            .insert(transfer_id, (src_range.0, src_range.1, src_tablet_id, dst));

        Ok(())
    }
}

impl MemMetaInner {
    fn tablet_range_state(
        &self,
        tablet_id: &TabletId,
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
    ) -> anyhow::Result<RangeState> {
        if let Some(range_states) = self
            .tablets
            .get(&tablet_id)
            .ok_or_else(|| anyhow!("{:?} not found", tablet_id))?
            .get(&colo_group_id)
        {
            let states = range_states.get_range(range);
            if states.is_empty() {
                return Ok(RangeState::None);
            } else if states.len() > 1 {
                return Err(anyhow!(
                    "{:?} has multiple states for range {:?} {:?}",
                    tablet_id,
                    colo_group_id,
                    range
                ));
            }
            return Ok(*states[0]);
        }
        Ok(RangeState::None)
    }
}
