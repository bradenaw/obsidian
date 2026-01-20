use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;

use crate::WalSeq;

#[async_trait]
pub(crate) trait Journal<E>: Send + Sync + 'static {
    async fn append(&self, entry: E) -> anyhow::Result<WalSeq>;

    fn read(
        &self,
        first: WalSeq,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<(WalSeq, E)>> + Send + '_>>;

    fn tail(
        &self,
        first: WalSeq,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<(WalSeq, E)>> + Send + '_>>;

    async fn oldest_available(&self) -> anyhow::Result<WalSeq>;

    async fn trim(&self, before: WalSeq) -> anyhow::Result<()>;
}
