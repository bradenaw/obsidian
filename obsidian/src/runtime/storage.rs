use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::AsyncWrite;

#[async_trait]
pub(crate) trait Storage: Sync + Send + 'static {
    type Writer: FileWriter;
    type Reader: FileReader;

    async fn put(&self, name: &str) -> anyhow::Result<Self::Writer>;

    async fn delete(&self, name: &str) -> anyhow::Result<()>;

    async fn get(&self, name: &str) -> anyhow::Result<Arc<Self::Reader>>;
}

pub(crate) trait FileWriter: AsyncWrite + Send + 'static {}

impl<T> FileWriter for T where T: AsyncWrite + Send + 'static {}

#[async_trait]
pub(crate) trait FileReader: Sync + Send + 'static {
    /// Fills `buf` with the bytes of the file starting at `offset`. Returns an error if the end of
    /// the file is reached before filling `buf`.
    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<()>;
    /// Returns the length of the file in bytes.
    fn len(&self) -> u64;
}
