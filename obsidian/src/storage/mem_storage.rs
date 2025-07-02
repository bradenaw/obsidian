use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use anyhow::anyhow;
use async_trait::async_trait;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;

use crate::storage::FileReader;
use crate::storage::Storage;

pub(crate) struct MemStorage {
    inner: Mutex<MemStorageInner>,
}

struct MemStorageInner {
    files: HashMap<String, Arc<Vec<u8>>>,
}

impl MemStorage {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(MemStorageInner {
                files: HashMap::new(),
            }),
        }
    }
}

#[async_trait]
impl Storage for MemStorage {
    type R = MemFile;

    async fn put<C: AsyncRead + Send>(&self, name: &str, content: C) -> anyhow::Result<()> {
        let mut buf = Vec::new();
        Box::pin(content).read_to_end(&mut buf).await?;
        self.inner
            .lock()
            .unwrap()
            .files
            .insert(name.to_string(), Arc::new(buf));

        Ok(())
    }

    async fn get(&self, name: &str) -> anyhow::Result<Self::R> {
        let inner = self.inner.lock().unwrap();
        let file_content = inner
            .files
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("{} not found", name))?;

        Ok(MemFile {
            content: Arc::downgrade(file_content),
            len: file_content.len() as u64,
        })
    }

    async fn delete(&self, name: &str) -> anyhow::Result<()> {
        self.inner.lock().unwrap().files.remove(name);
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct MemFile {
    content: Weak<Vec<u8>>,
    len: u64,
}

impl MemFile {
    fn content_or(&self) -> anyhow::Result<Arc<Vec<u8>>> {
        self.content
            .upgrade()
            .ok_or_else(|| anyhow!("file already deleted"))
    }
}

#[async_trait]
impl FileReader for MemFile {
    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<()> {
        let content = self.content_or()?;
        if (offset as u64) + (buf.len() as u64) > self.len {
            return Err(
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "unexpected eof").into(),
            );
        }
        Ok(buf.copy_from_slice(&content[(offset as usize)..(offset as usize) + buf.len()]))
    }

    async fn len(&self) -> anyhow::Result<u64> {
        self.content_or()?;
        Ok(self.len)
    }
}
