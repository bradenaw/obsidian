use std::sync::Arc;

use async_trait::async_trait;
use obsidian_common::NodeId;

use crate::discovery::Discovery;
use crate::runtime::Nodes;

#[async_trait]
pub(crate) trait TestNodes: Nodes {
    fn discovery(&self) -> Arc<Discovery>;
    async fn create_node(&mut self) -> anyhow::Result<NodeId>;
    fn remove_node(&mut self, node_id: NodeId);
}
