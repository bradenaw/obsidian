use std::sync::Arc;

use async_trait::async_trait;
use obsidian_common::ShardId;

use crate::Journal;

#[async_trait]
pub trait Journals<E>: Send + Sync + 'static {
    async fn journal(&self, shard_id: ShardId) -> Arc<dyn Journal<E>>;
}
