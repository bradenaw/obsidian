use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;

use im::OrdSet;
use obsidian_external::NodeDiscovery;
use obsidian_util::Watchable;

use crate::discovery::Discovery;
use crate::runtime::Node;
use crate::runtime::Nodes;
use crate::runtime::Shards;
use crate::test::TestNodeBuilder;
use crate::NodeId;

pub(crate) struct TestNodes {
    inner: Arc<TestNodesInner>,
    discovery: Arc<Discovery>,
}

struct TestNodesInner {
    node_builder: Box<dyn TestNodeBuilder>,
    routing: Mutex<HashMap<NodeId, Arc<dyn Node>>>,
    node_ids: Watchable<OrdSet<NodeId>>,
}

impl TestNodes {
    pub fn new(node_builder: Box<dyn TestNodeBuilder>) -> Self {
        let inner = Arc::new(TestNodesInner {
            node_builder,
            routing: Mutex::new(HashMap::new()),
            node_ids: Watchable::new(OrdSet::new()),
        });

        Self {
            inner: Arc::clone(&inner),
            discovery: Arc::new(Discovery::new(Arc::clone(&inner) as Arc<dyn Nodes>)),
        }
    }

    pub fn discovery(&self) -> Arc<Discovery> {
        Arc::clone(&self.discovery)
    }

    pub fn shards(&self) -> Arc<dyn Shards> {
        Arc::clone(&self.discovery) as Arc<dyn Shards>
    }

    pub async fn create_node(&self) -> anyhow::Result<NodeId> {
        let mut routing = self.inner.routing.lock().unwrap();

        let node = self
            .inner
            .node_builder
            .build(
                Arc::clone(&self.inner) as Arc<dyn Nodes>,
                self.discovery.meta(),
                self.shards(),
            )
            .await?;
        let node_id = node.id();

        routing.insert(node_id, node);
        let mut node_ids = self.inner.node_ids.get().0.clone();
        node_ids.insert(node_id);
        log::info!("{:?} created", node_id);
        log::info!("new set of nodes {:?}", node_ids);
        self.inner.node_ids.set(node_ids);

        Ok(node_id)
    }
}

impl Nodes for TestNodes {
    fn node(&self, node_id: NodeId) -> anyhow::Result<Arc<dyn Node>> {
        self.inner.node(node_id)
    }
}

impl NodeDiscovery for TestNodes {
    fn node_ids(&self) -> (OrdSet<NodeId>, Box<dyn Future<Output = ()> + Send + Unpin>) {
        self.inner.node_ids()
    }
}

impl Nodes for TestNodesInner {
    fn node(&self, node_id: NodeId) -> anyhow::Result<Arc<dyn Node>> {
        let routing = self.routing.lock().unwrap();
        let node_arc = routing
            .get(&node_id)
            .ok_or_else(|| anyhow::anyhow!("{:?} does not exist", node_id))?;

        Ok(Arc::clone(node_arc) as Arc<dyn Node>)
    }
}

impl NodeDiscovery for TestNodesInner {
    fn node_ids(&self) -> (OrdSet<NodeId>, Box<dyn Future<Output = ()> + Send + Unpin>) {
        let (node_ids, changed) = self.node_ids.get();
        (node_ids, Box::new(Box::pin(changed)))
    }
}
