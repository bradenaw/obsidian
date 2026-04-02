use std::ops::Deref;
use std::sync::Arc;

use anyhow::anyhow;
use futures::FutureExt;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::transport::server::TcpIncoming;

use crate::grpc::NodeClient;
use crate::grpc::NodeServer;
use crate::pb;
use crate::runtime::Node;

pub(crate) async fn node_grpc_bridge(
    node: Arc<dyn Node>,
) -> anyhow::Result<GrpcBridge<Arc<dyn Node>>> {
    let node_id = node.id();
    let (shutdown, on_shutdown) = oneshot::channel::<()>();
    let listener = TcpListener::bind("[::1]:0").await?;
    let addr = listener.local_addr()?;
    let serve = tonic::transport::Server::builder()
        .add_service(pb::internal::node_server::NodeServer::new(NodeServer::new(
            node,
        )))
        .serve_with_incoming_shutdown(
            TcpIncoming::from_listener(listener, true /*nodelay*/, None /*keepalive*/)
                .map_err(|e| anyhow!("{}", e))?,
            on_shutdown.map(|_| ()),
        );

    tokio::spawn(async { serve.await.unwrap() });

    let url = "http://".to_string() + &addr.to_string();

    let client = NodeClient::new(
        node_id,
        &pb::internal::node_client::NodeClient::connect(url).await?,
    );

    Ok(GrpcBridge {
        client: Arc::new(client),
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
