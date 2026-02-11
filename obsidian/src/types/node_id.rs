use std::fmt::Debug;
use std::fmt::Display;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::LazyLock;

use anyhow::anyhow;
use regex::Regex;
use uuid::Uuid;

use crate::pb;
use crate::types::ip_addr_from_proto;
use crate::types::ip_addr_to_proto;
use crate::types::uuid_from_proto;
use crate::types::uuid_to_proto;

#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct NodeId {
    pub addr: IpAddr,
    pub port: u16,
    pub uuid: Uuid,
}

impl NodeId {
    pub fn new(addr: IpAddr, port: u16) -> Self {
        Self {
            addr,
            port,
            uuid: Uuid::now_v7(),
        }
    }
}
static NODE_ID_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(.+?):([0-9]+)/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})?")
        .unwrap()
});

impl FromStr for NodeId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (_, [addr_str, port_str, uuid_str]) = NODE_ID_PATTERN
            .captures(s)
            .ok_or_else(|| anyhow!("node ID must match {:?}: {:?}", NODE_ID_PATTERN, s))?
            .extract();

        let addr = IpAddr::from_str(addr_str)?;
        let port = u16::from_str(port_str)?;
        let uuid = Uuid::from_str(uuid_str)?;

        Ok(Self { addr, port, uuid })
    }
}

impl Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}/{}", self.addr, self.port, self.uuid)
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
            addr: Some(ip_addr_to_proto(value.addr)),
            port: value.port as u32,
            uuid: Some(uuid_to_proto(value.uuid)),
        }
    }
}

impl TryFrom<pb::internal::NodeId> for NodeId {
    type Error = anyhow::Error;

    fn try_from(value_pb: pb::internal::NodeId) -> Result<Self, Self::Error> {
        Ok(Self {
            addr: ip_addr_from_proto(value_pb.addr.ok_or_else(|| anyhow!("missing field addr"))?)?,
            port: u16::try_from(value_pb.port)?,
            uuid: uuid_from_proto(value_pb.uuid.ok_or_else(|| anyhow!("missing field uuid"))?),
        })
    }
}
