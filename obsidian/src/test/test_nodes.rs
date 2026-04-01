use std::collections::HashMap;
use std::future::Future;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::sync::Arc;
use std::sync::Mutex;

use im::OrdSet;

use crate::discovery::Discovery;
use crate::election::Proposal;
use crate::meta::MetaSynced;
use crate::runtime::Journals;
use crate::runtime::Node;
use crate::runtime::Nodes;
use crate::runtime::Shards;
use crate::runtime::Storage;
use crate::util::Watchable;
use crate::JournalEntry;
use crate::NodeId;

pub(crate) struct TestNodes {
    inner: Arc<TestNodesInner>,
    discovery: Arc<Discovery>,
}

struct TestNodesInner {
    storage: Arc<dyn Storage>,
    journals: Arc<dyn Journals<Proposal<JournalEntry>>>,

    routing: Mutex<HashMap<NodeId, Arc<dyn Node>>>,
    node_ids: Watchable<OrdSet<NodeId>>,
}

impl TestNodes {
    pub fn new(
        storage: Arc<dyn Storage>,
        journals: Arc<dyn Journals<Proposal<JournalEntry>>>,
    ) -> Self {
        let inner = Arc::new(TestNodesInner {
            storage,
            journals,
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

        let node_id = NodeId::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 1);

        routing.insert(
            node_id,
            Arc::new(
                crate::node::Node::new(
                    node_id,
                    Arc::clone(&self.inner) as Arc<dyn Nodes>,
                    Arc::clone(&self.inner.storage),
                    self.discovery.meta(),
                    self.shards(),
                    Arc::new(MetaSynced::new(self.discovery.meta())),
                    Arc::clone(&self.inner.journals),
                )
                .await?,
            ) as Arc<dyn Node>,
        );
        let mut node_ids = self.inner.node_ids.get().0.clone();
        node_ids.insert(node_id);
        self.inner.node_ids.set(node_ids);

        Ok(node_id)
    }
}

impl Nodes for TestNodes {
    fn node(&self, node_id: NodeId) -> anyhow::Result<Arc<dyn Node>> {
        self.inner.node(node_id)
    }

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

    fn node_ids(&self) -> (OrdSet<NodeId>, Box<dyn Future<Output = ()> + Send + Unpin>) {
        let (node_ids, changed) = self.node_ids.get();
        (node_ids, Box::new(Box::pin(changed)))
    }
}
