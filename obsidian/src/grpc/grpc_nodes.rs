use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::anyhow;
use futures::stream::FuturesUnordered;
use im::ordset::DiffItem;
use im::OrdSet;
use obsidian_common::NodeId;
use obsidian_external::NodeDiscovery;
use obsidian_pb as pb;
use obsidian_util::spawn_owned;
use obsidian_util::OwnedJoinHandle;
use obsidian_util::OwnedWithBackground;
use obsidian_util::Retry;
use tokio::select;
use tokio::sync::mpsc;

use crate::grpc::NodeClient;
use crate::runtime::Node;
use crate::runtime::Nodes;

pub(crate) struct GrpcNodes(OwnedWithBackground<GrpcNodesInner>);

impl GrpcNodes {
    pub fn new(node_discovery: Arc<dyn NodeDiscovery + Send + Sync>) -> GrpcNodes {
        let inner = OwnedWithBackground::new(GrpcNodesInner {
            node_discovery,
            clients: RwLock::new(HashMap::new()),
        });

        inner.spawn(async |inner| {
            inner.populate().await;
        });

        GrpcNodes(inner)
    }
}

impl Nodes for GrpcNodes {
    fn node(&self, node_id: NodeId) -> anyhow::Result<Arc<dyn Node>> {
        let clients = self.0.clients.read().unwrap();
        let client_arc = clients
            .get(&node_id)
            .ok_or_else(|| anyhow!("no client for {:?}", node_id))?;
        Ok(Arc::clone(&client_arc))
    }
}

impl NodeDiscovery for GrpcNodes {
    fn node_ids(&self) -> (OrdSet<NodeId>, Box<dyn Future<Output = ()> + Send + Unpin>) {
        self.0.node_discovery.node_ids()
    }
}

struct GrpcNodesInner {
    node_discovery: Arc<dyn NodeDiscovery + Send + Sync>,
    clients: RwLock<HashMap<NodeId, Arc<dyn Node>>>,
}

impl GrpcNodesInner {
    async fn populate(&self) {
        let prev = OrdSet::new();
        let mut connects: HashMap<NodeId, OwnedJoinHandle<Arc<dyn Node>>> = HashMap::new();
        let (finished_send, mut finished_recv) = mpsc::channel(1 /*buffer*/);

        loop {
            let (node_ids, node_ids_changed) = self.node_discovery.node_ids();

            for diff in prev.diff(&node_ids) {
                match diff {
                    DiffItem::Add(node_id) => {
                        let node_id = *node_id;
                        let finished_send = finished_send.clone();
                        connects.insert(
                            node_id,
                            spawn_owned(async move {
                                let client = Self::connect(node_id).await;
                                let _ = finished_send.send(node_id).await;
                                client
                            }),
                        );
                    }
                    DiffItem::Remove(node_id) => {
                        connects.remove(node_id);
                        let mut clients = self.clients.write().unwrap();
                        clients.remove(node_id);
                    }
                    DiffItem::Update { .. } => {}
                }
            }

            select! {
                _ = node_ids_changed => {},
                maybe_node_id = finished_recv.recv() => {
                    let node_id = maybe_node_id.unwrap();
                    if let Some(client_fut) = connects.remove(&node_id) {
                        let client = client_fut.await;
                        let mut clients = self.clients.write().unwrap();
                        clients.insert(node_id, client);
                    }
                }
            }
        }
    }

    async fn connect(node_id: NodeId) -> Arc<dyn Node> {
        let grpc_client = Retry::new()
            .indefinitely(&async || -> anyhow::Result<_> {
                // TODO: https etc
                let url = format!("http://{}:{}", node_id.addr, node_id.port);
                pb::internal::node_client::NodeClient::connect(url)
                    .await
                    .map_err(|e| e.into())
            })
            .await;

        Arc::new(NodeClient::new(node_id, grpc_client))
    }
}
