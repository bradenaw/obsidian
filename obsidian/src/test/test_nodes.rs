use std::collections::HashMap;
use std::future::Future;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::sync::Arc;
use std::sync::Mutex;

use im::OrdSet;

use crate::meta::MetaSynced;
use crate::runtime::Meta;
use crate::runtime::Node;
use crate::runtime::Nodes;
use crate::runtime::Shards;
use crate::runtime::Storage;
use crate::runtime::Wals;
use crate::util::Watchable;
use crate::NodeId;

pub(super) struct TestNodes {
    inner: Arc<TestNodesInner>,
    shards: Arc<dyn Shards>,
}

struct TestNodesInner {
    storage: Arc<dyn Storage>,
    meta: Arc<dyn Meta>,
    wals: Arc<dyn Wals>,

    routing: Mutex<HashMap<NodeId, Arc<dyn Node>>>,
    node_ids: Watchable<OrdSet<NodeId>>,
}

impl TestNodes {
    pub fn new(storage: Arc<dyn Storage>, meta: Arc<dyn Meta>, wals: Arc<dyn Wals>) -> Self {
        let inner = Arc::new(TestNodesInner {
            storage,
            wals,
            meta: Arc::clone(&meta),
            routing: Mutex::new(HashMap::new()),
            node_ids: Watchable::new(OrdSet::new()),
        });

        Self {
            inner: Arc::clone(&inner),
            shards: Arc::new(crate::shards::Shards::new(
                Arc::new(MetaSynced::new(meta)),
                Arc::clone(&inner) as Arc<dyn Nodes>,
            )),
        }
    }

    pub fn shards(&self) -> Arc<dyn Shards> {
        Arc::clone(&self.shards)
    }

    pub async fn create_node(&self) -> anyhow::Result<NodeId> {
        let mut routing = self.inner.routing.lock().unwrap();

        let node_id = NodeId::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 1);

        routing.insert(
            node_id,
            Arc::new(
                crate::node::Node::new(
                    node_id,
                    Arc::clone(&self.inner.storage),
                    Arc::clone(&self.inner.meta),
                    Arc::clone(&self.shards),
                    Arc::new(MetaSynced::new(Arc::clone(&self.inner.meta))),
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

    fn node_ids(&self) -> (OrdSet<NodeId>, Box<dyn Future<Output = ()>>) {
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

    fn node_ids(&self) -> (OrdSet<NodeId>, Box<dyn Future<Output = ()>>) {
        let (node_ids, changed) = self.node_ids.get();
        (node_ids, Box::new(changed))
    }
}
