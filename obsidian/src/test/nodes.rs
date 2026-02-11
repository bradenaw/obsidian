use std::collections::HashMap;
use std::future::pending;
use std::future::Future;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::Weak;

use anyhow::anyhow;
use im::OrdSet;

use crate::meta::MetaSynced;
use crate::runtime::Meta;
use crate::runtime::Node;
use crate::runtime::Nodes;
use crate::runtime::Shards;
use crate::runtime::Storage;
use crate::test::MemWals;
use crate::util::Watchable;
use crate::NodeId;

pub(super) struct TestNodes<S> {
    storage: Arc<S>,
    meta: Arc<dyn Meta>,
    wals: MemWals,
    // This is always Some, it's just a RwLock<Option> because of the circular dependency during
    // construction: TestNodes needs a Shards to construct nodes with, Shards needs a Nodes to do
    // routing.
    shards: RwLock<Option<Arc<dyn Shards>>>,

    m: Mutex<HashMap<NodeId, Arc<dyn Node>>>,
    node_ids: Watchable<OrdSet<NodeId>>,
}

impl<S> TestNodes<S>
where
    S: Storage,
{
    pub fn new(storage: Arc<S>, meta: Arc<dyn Meta>) -> Arc<Self> {
        let nodes = Arc::new(Self {
            storage,
            meta: Arc::clone(&meta),
            wals: MemWals::new(),
            shards: RwLock::new(None),
            m: Mutex::new(HashMap::new()),
            node_ids: Watchable::new(OrdSet::new()),
        });

        {
            let mut shards = nodes.shards.write().unwrap();
            *shards = Some(Arc::new(crate::shards::Shards::new(
                Arc::new(MetaSynced::new(meta)),
                Arc::new(Arc::clone(&nodes)) as Arc<dyn Nodes>,
            )));
        }

        nodes
    }

    pub async fn create_node(self: &Arc<Self>) -> anyhow::Result<NodeId> {
        let mut m = self.m.lock().unwrap();

        let node_id = NodeId::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 1);
        let shards = self.shards.read().unwrap().as_ref().unwrap().clone();

        m.insert(
            node_id,
            Arc::new(
                crate::node::Node::new(
                    node_id,
                    Arc::clone(&self.meta),
                    Arc::new(MetaSynced::new(Arc::clone(&self.meta))),
                    shards,
                )
                .await?,
            ) as Arc<dyn Node>,
        );
        let mut node_ids = self.node_ids.get().0.clone();
        node_ids.insert(node_id);
        self.node_ids.set(node_ids);

        Ok(node_id)
    }
}

impl<S> Nodes for Arc<TestNodes<S>>
where
    S: Storage,
{
    fn node(&self, node_id: NodeId) -> anyhow::Result<Arc<dyn Node>> {
        let m = self.m.lock().unwrap();
        let node_arc = m
            .get(&node_id)
            .ok_or_else(|| anyhow::anyhow!("{:?} does not exist", node_id))?;

        Ok(Arc::clone(node_arc) as Arc<dyn Node>)
    }

    fn node_ids(&self) -> (OrdSet<NodeId>, Box<dyn Future<Output = ()>>) {
        let (node_ids, changed) = self.node_ids.get();
        (node_ids, Box::new(changed))
    }
}

impl<S> Nodes for Weak<TestNodes<S>>
where
    S: Storage,
{
    fn node(&self, node_id: NodeId) -> anyhow::Result<Arc<dyn Node>> {
        Weak::upgrade(self)
            .ok_or_else(|| anyhow!(""))?
            .node(node_id)
    }

    fn node_ids(&self) -> (OrdSet<NodeId>, Box<dyn Future<Output = ()>>) {
        if let Some(inner) = Weak::upgrade(self) {
            inner.node_ids()
        } else {
            (OrdSet::new(), Box::new(pending()))
        }
    }
}
