mod cached_storage;
mod mem_storage;

use async_trait::async_trait;
use tokio::io::AsyncRead;

use crate::util::AsyncReadExactAt;

#[async_trait]
pub(crate) trait Storage {
    type R: AsyncReadExactAt + Clone + Sync + Send;

    async fn put<C: AsyncRead + Send>(&self, name: &str, content: C) -> anyhow::Result<()>;

    async fn delete(&self, name: &str) -> anyhow::Result<()>;

    async fn get(&self, name: &str) -> anyhow::Result<Self::R>;
}

#[allow(unused_imports)]
pub(crate) use cached_storage::CachedStorage;
#[allow(unused_imports)]
pub(crate) use mem_storage::MemStorage;
