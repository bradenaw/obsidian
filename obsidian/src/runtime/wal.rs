use async_trait::async_trait;
use futures::Stream;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct WalSeq(pub u64);

#[async_trait]
pub(crate) trait Wal<E>: Send + Sync + 'static
where
    // TODO: TryFrom<bytes> + Into<bytes>
    E: Clone + Send + Sync + 'static,
{
    async fn append(&self, e: E) -> anyhow::Result<WalSeq>;

    fn read(
        &self,
        first: WalSeq,
    ) -> impl Stream<Item = anyhow::Result<(WalSeq, E)>> + Send + '_;

    fn tail(
        &self,
        first: WalSeq,
    ) -> impl Stream<Item = anyhow::Result<(WalSeq, E)>> + Send + '_;

    async fn oldest_available(&self) -> anyhow::Result<WalSeq>;

    async fn trim(&self, before: WalSeq) -> anyhow::Result<()>;
}
