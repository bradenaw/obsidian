use std::sync::Arc;

use async_trait::async_trait;

use crate::runtime::Tablet;
use crate::ShardId;
use crate::TabletId;
use crate::Timestamp;

#[async_trait]
pub(crate) trait Shard: Send + Sync {
    fn id(&self) -> ShardId;

    fn tablet(&self, tablet_id: TabletId) -> anyhow::Result<Arc<dyn Tablet>>;

    async fn wait_meta_sync(&self, ts: Timestamp) -> anyhow::Result<()>;
}
