use std::sync::Arc;

use obsidian_external::NodeDiscovery;

use crate::runtime::Node;
use crate::NodeId;

pub trait Nodes: NodeDiscovery + Send + Sync {
    fn node(&self, node_id: NodeId) -> anyhow::Result<Arc<dyn Node>>;
}
