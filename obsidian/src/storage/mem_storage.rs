use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::task::Poll;

use anyhow::anyhow;
use async_trait::async_trait;
use tokio::io::AsyncWrite;

use crate::runtime::FileReader;
use crate::runtime::Storage;

#[derive(Clone)]
pub(crate) struct MemStorage {
    inner: Arc<Mutex<MemStorageInner>>,
}

struct MemStorageInner {
    files: HashMap<String, Arc<Vec<u8>>>,
}

impl MemStorage {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MemStorageInner {
                files: HashMap::new(),
            })),
        }
    }
}

#[async_trait]
impl Storage for MemStorage {
    type Writer = MemFileWriter;
    type Reader = MemFile;

    async fn put(&self, name: &str) -> anyhow::Result<Self::Writer> {
        Ok(MemFileWriter {
            parent: Arc::clone(&self.inner),
            name: name.to_string(),
            content: Vec::new(),
        })
    }

    async fn get(&self, name: &str) -> anyhow::Result<Self::Reader> {
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

    fn len(&self) -> u64 {
        self.len
    }
}

pub(crate) struct MemFileWriter {
    name: String,
    content: Vec<u8>,
    parent: Arc<Mutex<MemStorageInner>>,
}

impl AsyncWrite for MemFileWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let self_ = Pin::get_mut(self);
        self_.content.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let mut parent = self.parent.lock().unwrap();
        parent
            .files
            .insert(self.name.clone(), Arc::new(self.content.clone()));
        Poll::Ready(Ok(()))
    }
}
