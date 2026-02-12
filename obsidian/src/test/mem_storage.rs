use std::collections::HashMap;
use std::collections::HashSet;
use std::pin::pin;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::task::Poll;

use anyhow::anyhow;
use async_trait::async_trait;
use tokio::io::AsyncWrite;

use crate::runtime::FileReader;
use crate::runtime::FileWriter;
use crate::runtime::Storage;
use crate::test::MemFileReader;
use crate::test::MemFileWriter;

#[derive(Clone)]
pub(crate) struct MemStorage {
    inner: Arc<Mutex<MemStorageInner>>,
}

struct MemStorageInner {
    files: HashMap<String, Arc<MemFileReader>>,
    names: HashSet<String>,
}

impl MemStorage {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MemStorageInner {
                files: HashMap::new(),
                names: HashSet::new(),
            })),
        }
    }
}

#[async_trait]
impl Storage for MemStorage {
    async fn put(&self, name: &str) -> anyhow::Result<Box<dyn FileWriter>> {
        let mut inner = self.inner.lock().unwrap();
        if inner.names.contains(name) {
            return Err(anyhow!("file {:?} already exists", name));
        }
        inner.names.insert(name.to_string());

        Ok(Box::new(MemStorageFileWriter {
            parent: Arc::clone(&self.inner),
            name: name.to_string(),
            inner: Some(MemFileWriter::new()),
        }))
    }

    async fn get(&self, name: &str) -> anyhow::Result<Arc<dyn FileReader>> {
        let inner = self.inner.lock().unwrap();
        let file_content = inner
            .files
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("{} not found", name))?;

        Ok(Arc::new(MemStorageFileReader {
            inner: Arc::downgrade(file_content),
            len: file_content.len() as u64,
        }))
    }

    async fn delete(&self, name: &str) -> anyhow::Result<()> {
        self.inner.lock().unwrap().files.remove(name);
        // Are names allowed to be reused?
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct MemStorageFileReader {
    // Indirect so that deletes of the file on the parent MemStorage cause the file to become
    // unavailable.
    inner: Weak<MemFileReader>,
    len: u64,
}

#[async_trait]
impl FileReader for MemStorageFileReader {
    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<()> {
        self.inner
            .upgrade()
            .ok_or_else(|| anyhow!("file already deleted"))?
            .read_exact_at(buf, offset)
            .await
    }

    fn len(&self) -> u64 {
        self.len
    }
}

struct MemStorageFileWriter {
    name: String,
    parent: Arc<Mutex<MemStorageInner>>,

    inner: Option<MemFileWriter>,
}

impl AsyncWrite for MemStorageFileWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let maybe_inner = &mut self.inner;
        match maybe_inner {
            Some(inner) => pin!(inner).poll_write(cx, buf),
            None => {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    anyhow!("writer already shutdown"),
                )));
            }
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let maybe_inner = &mut self.inner;
        match maybe_inner {
            Some(inner) => pin!(inner).poll_flush(cx),
            None => {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    anyhow!("writer already shutdown"),
                )));
            }
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.inner.take() {
            Some(mut inner) => match pin!(&mut inner).poll_flush(cx) {
                Poll::Ready(Ok(())) => {
                    let mut parent = self.parent.lock().unwrap();
                    parent
                        .files
                        .insert(self.name.clone(), Arc::new(inner.into_reader()));
                    Poll::Ready(Ok(()))
                }
                x => return x,
            },
            None => {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    anyhow!("writer already shutdown"),
                )));
            }
        }
    }
}
