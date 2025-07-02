mod cached_storage;
mod mem_storage;

use async_trait::async_trait;
use tokio::io::AsyncRead;

#[async_trait]
pub(crate) trait Storage {
    type R: FileReader + Clone + Sync + Send;

    async fn put<C: AsyncRead + Send>(&self, name: &str, content: C) -> anyhow::Result<()>;

    async fn delete(&self, name: &str) -> anyhow::Result<()>;

    async fn get(&self, name: &str) -> anyhow::Result<Self::R>;
}

#[async_trait]
pub(crate) trait FileReader {
    /// Fills `buf` with the bytes of the file starting at `offset`. Returns an error if the end of
    /// the file is reached before filling `buf`.
    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<()>;
    /// Returns the length of the file in bytes.
    async fn len(&self) -> anyhow::Result<u64>;
}

#[allow(unused_imports)]
pub(crate) use cached_storage::CachedStorage;
#[allow(unused_imports)]
pub(crate) use mem_storage::MemStorage;
