use std::future::Future;

use im::OrdSet;
use obsidian_common::NodeId;

pub trait NodeDiscovery {
    fn node_ids(&self) -> (OrdSet<NodeId>, Box<dyn Future<Output = ()> + Send + Unpin>);
}
