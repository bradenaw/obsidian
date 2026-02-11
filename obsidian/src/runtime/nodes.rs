use std::future::Future;
use std::sync::Arc;

use im::OrdSet;

use crate::runtime::Node;
use crate::NodeId;

pub(crate) trait Nodes: Send + Sync {
    fn node(&self, node_id: NodeId) -> anyhow::Result<Arc<dyn Node>>;

    fn node_ids(&self) -> (OrdSet<NodeId>, Box<dyn Future<Output = ()>>);
}
