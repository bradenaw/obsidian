use std::future::Future;

use im::OrdSet;

use crate::NodeId;

pub(crate) trait Discovery: Send + Sync {
    fn node_ids(&self) -> (OrdSet<NodeId>, Box<dyn Future<Output = ()> + Unpin + Send>);

    fn register(&self, node_id: NodeId) -> Box<dyn Registration>;
}

pub(crate) trait Registration: Send + Sync {}
