use std::collections::HashSet;

use crate::pb;
use crate::NodeId;

#[derive(Clone, Debug)]
pub(crate) struct ShardMetadata {
    pub assigned_node_ids: HashSet<NodeId>,
}

impl TryFrom<pb::internal::ShardMetadata> for ShardMetadata {
    type Error = anyhow::Error;

    fn try_from(value_pb: pb::internal::ShardMetadata) -> Result<Self, Self::Error> {
        Ok(Self {
            assigned_node_ids: value_pb
                .assigned_node_ids
                .into_iter()
                .map(NodeId::try_from)
                .collect::<anyhow::Result<HashSet<_>>>()?,
        })
    }
}

impl From<ShardMetadata> for pb::internal::ShardMetadata {
    fn from(value: ShardMetadata) -> Self {
        Self {
            assigned_node_ids: value
                .assigned_node_ids
                .into_iter()
                .map(NodeId::into)
                .collect(),
        }
    }
}
