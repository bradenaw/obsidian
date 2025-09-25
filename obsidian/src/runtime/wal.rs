use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;

use crate::WalEntry;
use crate::WalSeq;

#[async_trait]
pub(crate) trait Wal: Send + Sync + 'static {
    async fn append(&self, entry: WalEntry) -> anyhow::Result<WalSeq>;

    fn read(
        &self,
        first: WalSeq,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<(WalSeq, WalEntry)>> + Send + '_>>;

    fn tail(
        &self,
        first: WalSeq,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<(WalSeq, WalEntry)>> + Send + '_>>;

    async fn oldest_available(&self) -> anyhow::Result<WalSeq>;

    async fn trim(&self, before: WalSeq) -> anyhow::Result<()>;
}
