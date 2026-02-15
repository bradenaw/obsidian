use std::convert::TryFrom;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::anyhow;

use crate::meta::MetaState;
use crate::meta::TransferState;
use crate::pb;
use crate::TabletId;

#[derive(Clone, Debug)]
pub(crate) struct TransferMetadata {
    pub(crate) state: MetaState<TransferState>,
    pub(crate) srcs: Vec<TabletId>,
    pub(crate) dsts: Vec<TabletId>,
    pub(crate) timestamp: SystemTime,
}

impl TryFrom<pb::internal::TransferMetadata> for TransferMetadata {
    type Error = anyhow::Error;

    fn try_from(value_pb: pb::internal::TransferMetadata) -> Result<Self, Self::Error> {
        Ok(Self {
            state: MetaState::from((
                TransferState::try_from(
                    pb::internal::TransferState::from_i32(value_pb.state)
                        .ok_or_else(|| anyhow!("missing state"))?,
                )?,
                match pb::internal::TransferState::from_i32(value_pb.next_state) {
                    Some(pb::internal::TransferState::Unknown) => None,
                    None => None,
                    Some(state_pb) => Some(TransferState::try_from(state_pb)?),
                },
            )),
            srcs: value_pb
                .srcs
                .into_iter()
                .map(TabletId::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            dsts: value_pb
                .dsts
                .into_iter()
                .map(TabletId::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            timestamp: SystemTime::UNIX_EPOCH
                .checked_add(Duration::from_millis(value_pb.timestamp_ms))
                .unwrap(),
        })
    }
}

impl From<TransferMetadata> for pb::internal::TransferMetadata {
    fn from(value: TransferMetadata) -> Self {
        Self {
            state: pb::internal::TransferState::from(*value.state.current()) as i32,
            next_state: value
                .state
                .next()
                .map(|state| pb::internal::TransferState::from(*state) as i32)
                .unwrap_or(0),
            srcs: value
                .srcs
                .into_iter()
                .map(pb::internal::TabletId::from)
                .collect(),
            dsts: value
                .dsts
                .into_iter()
                .map(pb::internal::TabletId::from)
                .collect(),
            timestamp_ms: value
                .timestamp
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }
}
