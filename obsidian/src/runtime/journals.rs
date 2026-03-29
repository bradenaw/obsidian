use std::sync::Arc;

use async_trait::async_trait;

use crate::runtime::Journal;
use crate::ShardId;

#[async_trait]
pub(crate) trait Journals<E>: Send + Sync + 'static {
    async fn journal(&self, shard_id: ShardId) -> Arc<dyn Journal<E>>;
}
