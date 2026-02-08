use crate::meta::MetaValue;
use crate::pb;
use crate::NodeId;

pub(crate) struct ShardMetadata {
    pub assigned_node_id: Option<NodeId>,
}

impl MetaValue for ShardMetadata {
    type PB = pb::internal::ShardMetadata;
}

impl TryFrom<pb::internal::ShardMetadata> for ShardMetadata {
    type Error = anyhow::Error;

    fn try_from(value_pb: pb::internal::ShardMetadata) -> Result<Self, Self::Error> {
        Ok(Self {
            assigned_node_id: value_pb
                .assigned_node_id
                .map(NodeId::try_from)
                .transpose()?,
        })
    }
}

impl From<ShardMetadata> for pb::internal::ShardMetadata {
    fn from(value: ShardMetadata) -> Self {
        Self {
            assigned_node_id: value.assigned_node_id.map(NodeId::into),
        }
    }
}
