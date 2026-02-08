use std::fmt::Debug;
use std::fmt::Display;

use anyhow::anyhow;
use uuid::Uuid;

use crate::pb;
use crate::types::uuid_from_proto;
use crate::types::uuid_to_proto;

#[derive(Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct NodeId {
    pub hostname: String,
    pub port: u16,
    pub uuid: Uuid,
}

impl Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}/{}", self.hostname, self.port, self.uuid)
    }
}

impl Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node:{}", self)
    }
}

impl From<NodeId> for pb::internal::NodeId {
    fn from(value: NodeId) -> Self {
        Self {
            hostname: value.hostname,
            port: value.port as u32,
            uuid: Some(uuid_to_proto(value.uuid)),
        }
    }
}

impl TryFrom<pb::internal::NodeId> for NodeId {
    type Error = anyhow::Error;

    fn try_from(value_pb: pb::internal::NodeId) -> Result<Self, Self::Error> {
        Ok(Self {
            hostname: value_pb.hostname,
            port: u16::try_from(value_pb.port)?,
            uuid: uuid_from_proto(value_pb.uuid.ok_or_else(|| anyhow!("missing field uuid"))?),
        })
    }
}
