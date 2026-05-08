use std::io;

use async_trait::async_trait;

#[async_trait]
pub trait FileWriter: Send + Sync + 'static {
    async fn write_all(&mut self, src: &[u8]) -> io::Result<()>;
    async fn shutdown(&mut self) -> io::Result<()>;
}
