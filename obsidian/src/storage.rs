use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;

use crate::AsyncReadExactAt;

#[async_trait]
pub(crate) trait Storage {
    type R: AsyncReadExactAt;

    async fn put<W: AsyncRead + Send>(&self, name: &str, w: W) -> anyhow::Result<()>;

    async fn delete(&self, name: &str) -> anyhow::Result<()>;

    async fn get(&self, name: &str) -> anyhow::Result<Self::R>;
}

pub(crate) struct MemStorage {
    inner: Mutex<MemStorageInner>,
}

struct MemStorageInner {
    files: HashMap<String, Arc<Vec<u8>>>,
}

impl MemStorage {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(MemStorageInner {
                files: HashMap::new(),
            }),
        }
    }
}

#[async_trait]
impl Storage for MemStorage {
    type R = Arc<Vec<u8>>;

    async fn put<W: AsyncRead + Send>(&self, name: &str, w: W) -> anyhow::Result<()> {
        let mut buf = Vec::new();
        Box::pin(w).read_to_end(&mut buf).await?;
        self.inner
            .lock()
            .unwrap()
            .files
            .insert(name.to_string(), Arc::new(buf));

        Ok(())
    }

    async fn get(&self, name: &str) -> anyhow::Result<Self::R> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .files
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("{} not found", name))?
            .clone())
    }

    async fn delete(&self, name: &str) -> anyhow::Result<()> {
        self.inner.lock().unwrap().files.remove(name);
        Ok(())
    }
}
