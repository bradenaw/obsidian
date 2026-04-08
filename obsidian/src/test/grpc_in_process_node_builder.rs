use std::sync::Arc;

use async_trait::async_trait;

use crate::runtime;
use crate::test::node_grpc_bridge;
use crate::test::InProcessNodeBuilder;
use crate::test::TestNodeBuilder;

/// Builds nodes that run inside this same process, but the returned Node impl speaks over gRPC.
pub(crate) struct GrpcInProcessNodeBuilder {
    inner: InProcessNodeBuilder,
}

impl GrpcInProcessNodeBuilder {
    pub fn new(inner: InProcessNodeBuilder) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl TestNodeBuilder for GrpcInProcessNodeBuilder {
    async fn build(
        &self,
        nodes: Arc<dyn runtime::Nodes>,
        meta: Arc<dyn runtime::Meta>,
        shards: Arc<dyn runtime::Shards>,
    ) -> anyhow::Result<Arc<dyn runtime::Node>> {
        let node = self.inner.build(nodes, meta, shards).await?;

        let node_grpc = node_grpc_bridge(node).await?;

        Ok(Arc::new(node_grpc) as Arc<dyn runtime::Node>)
    }
}
