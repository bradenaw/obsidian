use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::anyhow;
use futures::FutureExt;
use futures::Stream;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::transport::server::TcpIncoming;

use crate::grpc::GatewayClient;
use crate::grpc::GatewayServer;
use crate::grpc::NodeClient;
use crate::grpc::NodeServer;
use crate::pb;
use crate::runtime;
use crate::runtime::Node;
use crate::runtime::ReplicaState;
use crate::NodeId;
use crate::Obsidian;
use crate::ShardId;

// Annoying macro indirection because the type of the parameter to add_service is a mess of
// generics.
//
// Spawns a tonic server on a random port and returns (url, shutdown). Dropping or sending to
// shutdown will stop the server.
macro_rules! spawn_server {
    ($server:expr) => {
        async {
            let (shutdown, on_shutdown) = oneshot::channel::<()>();
            let listener = TcpListener::bind("[::1]:0").await?;
            let addr = listener.local_addr()?;

            tokio::spawn(async {
                let serve = tonic::transport::Server::builder()
                    .add_service($server)
                    .serve_with_incoming_shutdown(
                        TcpIncoming::from_listener(
                            listener, true, /*nodelay*/
                            None, /*keepalive*/
                        )
                        .map_err(|e| anyhow!("{}", e))
                        .unwrap(),
                        on_shutdown.map(|_| ()),
                    );

                serve.await.unwrap()
            });

            let url = "http://".to_string() + &addr.to_string();

            Ok::<_, anyhow::Error>((url, shutdown))
        }
    };
}

pub(crate) async fn obsidian_grpc_bridge(
    obs: Arc<dyn Obsidian>,
) -> anyhow::Result<GrpcBridge<Arc<dyn Obsidian>>> {
    let (url, shutdown) = spawn_server!(pb::obsidian_server::ObsidianServer::new(
        GatewayServer::new(obs)
    ))
    .await?;

    Ok(GrpcBridge {
        client: Arc::new(GatewayClient::new(
            &pb::obsidian_client::ObsidianClient::connect(url).await?,
        )),
        shutdown: Some(shutdown),
    })
}

pub(crate) async fn node_grpc_bridge(
    node: Arc<dyn Node>,
) -> anyhow::Result<GrpcBridge<Arc<dyn Node>>> {
    let node_id = node.id();

    let (url, shutdown) = spawn_server!(pb::internal::node_server::NodeServer::new(
        NodeServer::new(node),
    ))
    .await?;

    Ok(GrpcBridge {
        client: Arc::new(NodeClient::new(
            node_id,
            pb::internal::node_client::NodeClient::connect(url).await?,
        )),
        shutdown: Some(shutdown),
    })
}

pub(crate) struct GrpcBridge<T> {
    client: T,
    shutdown: Option<oneshot::Sender<()>>,
}

impl<T> GrpcBridge<T> {
    pub fn new(client: T, shutdown: oneshot::Sender<()>) -> Self {
        Self {
            client,
            shutdown: Some(shutdown),
        }
    }
}

impl<T> Deref for GrpcBridge<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.client
    }
}

impl<T> Drop for GrpcBridge<T> {
    fn drop(&mut self) {
        // TODO: Not clear if there's a way to find out that the serve actually stopped and
        // unbound the port. The `serve` future appears not to end.
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

impl Node for GrpcBridge<Arc<dyn Node>> {
    fn id(&self) -> NodeId {
        self.client.id()
    }

    fn shard(&self, shard_id: ShardId) -> anyhow::Result<Arc<dyn runtime::Shard>> {
        self.client.shard(shard_id)
    }

    fn meta(&self) -> anyhow::Result<Arc<dyn runtime::Meta>> {
        self.client.meta()
    }

    fn supervisor(&self) -> anyhow::Result<Arc<dyn runtime::Supervisor>> {
        self.client.supervisor()
    }

    fn shards_subscribe(
        &self,
    ) -> Box<dyn Stream<Item = anyhow::Result<HashMap<ShardId, ReplicaState>>> + Send + Unpin + '_>
    {
        self.client.shards_subscribe()
    }
}
