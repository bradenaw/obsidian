use std::sync::Arc;

use async_trait::async_trait;

use crate::runtime;

#[async_trait]
pub(crate) trait TestNodeBuilder: Send + Sync + 'static {
    async fn build(
        &self,
        nodes: Arc<dyn runtime::Nodes>,
        meta: Arc<dyn runtime::Meta>,
        shards: Arc<dyn runtime::Shards>,
    ) -> anyhow::Result<Arc<dyn runtime::Node>>;
}
