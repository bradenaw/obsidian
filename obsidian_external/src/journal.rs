use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use obsidian_common::JournalSeq;

#[async_trait]
pub trait Journal<E>: Send + Sync + 'static {
    async fn append(&self, entry: E) -> anyhow::Result<JournalSeq>;

    fn tail(
        &self,
        first: JournalSeq,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<(JournalSeq, E)>> + Send + '_>>;

    async fn oldest_available(&self) -> anyhow::Result<JournalSeq>;

    async fn latest(&self) -> anyhow::Result<JournalSeq>;

    async fn trim(&self, before: JournalSeq) -> anyhow::Result<()>;
}
