use std::convert::TryFrom;

use anyhow::anyhow;

use crate::meta::MetaState;
use crate::meta::TabletState;
use crate::pb;
use crate::ColoGroupId;
use crate::Range;
use crate::TransferId;

#[derive(Clone, Debug)]
pub(crate) struct TabletMetadata {
    pub(crate) colo_group_id: ColoGroupId,
    pub(crate) range: Range<Vec<u8>>,
    pub(crate) state: MetaState<TabletState>,
    pub(crate) transfer_id: Option<TransferId>,
}

impl TryFrom<pb::internal::TabletMetadata> for TabletMetadata {
    type Error = anyhow::Error;

    fn try_from(value_pb: pb::internal::TabletMetadata) -> Result<Self, Self::Error> {
        Ok(Self {
            colo_group_id: ColoGroupId(value_pb.colo_group_id),
            range: Range::try_from(value_pb.range.ok_or_else(|| anyhow!("missing range"))?)?,
            state: MetaState::from((
                TabletState::try_from(
                    pb::internal::TabletState::from_i32(value_pb.state)
                        .ok_or_else(|| anyhow!("missing state"))?,
                )?,
                match pb::internal::TabletState::from_i32(value_pb.next_state) {
                    Some(pb::internal::TabletState::Unknown) => None,
                    None => None,
                    Some(state_pb) => Some(TabletState::try_from(state_pb)?),
                },
            )),
            transfer_id: value_pb.transfer_id.map(TransferId::try_from).transpose()?,
        })
    }
}

impl From<TabletMetadata> for pb::internal::TabletMetadata {
    fn from(value: TabletMetadata) -> Self {
        Self {
            colo_group_id: value.colo_group_id.0,
            range: Some(value.range.into()),
            state: pb::internal::TabletState::from(*value.state.current()) as i32,
            next_state: value
                .state
                .next()
                .map(|state| pb::internal::TabletState::from(*state) as i32)
                .unwrap_or(0),
            transfer_id: value.transfer_id.map(TransferId::into),
        }
    }
}
