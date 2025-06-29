mod mem_storage;


use async_trait::async_trait;
use tokio::io::AsyncRead;

use crate::util::AsyncReadExactAt;

#[async_trait]
pub(crate) trait Storage {
    type R: AsyncReadExactAt;

    async fn put<W: AsyncRead + Send>(&self, name: &str, w: W) -> anyhow::Result<()>;

    async fn delete(&self, name: &str) -> anyhow::Result<()>;

    async fn get(&self, name: &str) -> anyhow::Result<Self::R>;
}


pub(crate) use mem_storage::MemStorage;
