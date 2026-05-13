use std::collections::HashMap;
use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex;

use async_stream::stream;
use futures::Stream;
use futures::StreamExt;
use im::OrdSet;
use obsidian_common::ShardId;
use obsidian_external::NodeDiscovery;
use obsidian_util::spawn_owned;
use obsidian_util::Owned;
use obsidian_util::Watchable;
use obsidian_util::WeakView;
use tokio::sync::mpsc;

use crate::discovery::Discovery;
use crate::runtime::Meta;
use crate::runtime::Node;
use crate::runtime::Nodes;
use crate::runtime::ReplicaState;
use crate::runtime::Shard;
use crate::runtime::Shards;
use crate::runtime::Supervisor;
use crate::test::TestNodeBuilder;
use crate::NodeId;

pub(crate) struct TestNodes {
    inner: Arc<TestNodesInner>,
    discovery: Arc<Discovery>,
}

struct TestNodesInner {
    node_builder: Box<dyn TestNodeBuilder>,
    nodes: Mutex<HashMap<NodeId, Owned<Arc<dyn Node + 'static>>>>,
    node_ids: Watchable<OrdSet<NodeId>>,
}

impl TestNodes {
    pub fn new(node_builder: Box<dyn TestNodeBuilder>) -> Self {
        let inner = Arc::new(TestNodesInner {
            node_builder,
            nodes: Mutex::new(HashMap::new()),
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
        let mut nodes = self.inner.nodes.lock().unwrap();

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

        nodes.insert(node_id, Owned::new(node));
        let mut node_ids = self.inner.node_ids.get().0.clone();
        node_ids.insert(node_id);
        log::info!("{:?} created", node_id);
        log::info!("new set of nodes {:?}", node_ids);
        self.inner.node_ids.set(node_ids);

        Ok(node_id)
    }

    pub fn remove_node(&self, node_id: NodeId) {
        let mut nodes = self.inner.nodes.lock().unwrap();
        nodes.remove(&node_id);
        let mut node_ids = self.inner.node_ids.get().0.clone();
        node_ids.remove(&node_id);
        self.inner.node_ids.set(node_ids);
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
        let nodes = self.nodes.lock().unwrap();
        let node = nodes
            .get(&node_id)
            .ok_or_else(|| anyhow::anyhow!("{:?} does not exist", node_id))?;

        Ok(Arc::new(WeakNode {
            node_id: node_id,
            inner: Owned::weak(node),
        }) as Arc<dyn Node>)
    }
}

impl NodeDiscovery for TestNodesInner {
    fn node_ids(&self) -> (OrdSet<NodeId>, Box<dyn Future<Output = ()> + Send + Unpin>) {
        let (node_ids, changed) = self.node_ids.get();
        (node_ids, Box::new(Box::pin(changed)))
    }
}

struct WeakNode<N> {
    node_id: NodeId,
    inner: Arc<WeakView<N>>,
}

impl<N> Node for WeakNode<N>
where
    N: Node + 'static,
{
    fn id(&self) -> NodeId {
        self.node_id
    }

    fn shard(&self, shard_id: ShardId) -> anyhow::Result<Arc<dyn Shard>> {
        self.inner.or_closed_sync(|node| node.shard(shard_id))
    }

    fn meta(&self) -> anyhow::Result<Arc<dyn Meta>> {
        self.inner.or_closed_sync(|node| node.meta())
    }

    fn supervisor(&self) -> anyhow::Result<Arc<dyn Supervisor>> {
        self.inner.or_closed_sync(|node| node.supervisor())
    }

    fn shards_subscribe(
        &self,
    ) -> Box<dyn Stream<Item = anyhow::Result<HashMap<ShardId, ReplicaState>>> + Send + Unpin + '_>
    {
        let (send, mut recv) = mpsc::channel(1);

        let inner = Arc::clone(&self.inner);
        let join_handle = spawn_owned(async move {
            let result = inner
                .or_closed(async |node| {
                    let mut stream = node.shards_subscribe();
                    while let Some(item) = stream.next().await {
                        if send.send(item).await.is_err() {
                            break;
                        }
                    }
                    Ok::<_, anyhow::Error>(())
                })
                .await;
            if let Err(e) = result {
                let _ = send.send(Err(e));
            }
        });

        Box::new(Box::pin(stream! {
            while let Some(item) = recv.recv().await {
                yield item;
            }
            join_handle.cancel().await;
        }))
    }
}

impl Node for Arc<dyn Node> {
    fn id(&self) -> NodeId {
        self.deref().id()
    }

    fn shard(&self, shard_id: ShardId) -> anyhow::Result<Arc<dyn Shard>> {
        self.deref().shard(shard_id)
    }

    fn meta(&self) -> anyhow::Result<Arc<dyn Meta>> {
        self.deref().meta()
    }

    fn supervisor(&self) -> anyhow::Result<Arc<dyn Supervisor>> {
        self.deref().supervisor()
    }

    fn shards_subscribe(
        &self,
    ) -> Box<dyn Stream<Item = anyhow::Result<HashMap<ShardId, ReplicaState>>> + Send + Unpin + '_>
    {
        self.deref().shards_subscribe()
    }
}
