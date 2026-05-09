use std::collections::HashMap;
use std::collections::HashSet;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use anyhow::anyhow;
use async_trait::async_trait;

use crate::mem::MemFileReader;
use crate::mem::MemFileWriter;
use crate::FileName;
use crate::FileReader;
use crate::FileWriter;
use crate::Storage;

#[derive(Clone)]
pub struct MemStorage {
    inner: Arc<Mutex<MemStorageInner>>,
}

struct MemStorageInner {
    files: HashMap<FileName, Arc<MemFileReader>>,
    names: HashSet<FileName>,
}

impl MemStorage {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MemStorageInner {
                files: HashMap::new(),
                names: HashSet::new(),
            })),
        }
    }
}

impl Default for MemStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Storage for MemStorage {
    async fn put(&self, name: FileName) -> anyhow::Result<Box<dyn FileWriter>> {
        let mut inner = self.inner.lock().unwrap();
        if inner.names.contains(&name) {
            return Err(anyhow!("file {:?} already exists", name));
        }
        inner.names.insert(name.clone());

        Ok(Box::new(MemStorageFileWriter {
            parent: Arc::clone(&self.inner),
            name,
            inner: Some(MemFileWriter::new()),
        }))
    }

    async fn get(&self, name: FileName) -> anyhow::Result<Arc<dyn FileReader>> {
        let inner = self.inner.lock().unwrap();
        let file_content = inner
            .files
            .get(&name)
            .ok_or_else(|| anyhow::anyhow!("{:?} not found", name))?;

        Ok(Arc::new(MemStorageFileReader {
            inner: Arc::downgrade(file_content),
            len: file_content.len(),
        }))
    }

    async fn delete(&self, name: FileName) -> anyhow::Result<()> {
        self.inner.lock().unwrap().files.remove(&name);
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
    name: FileName,
    parent: Arc<Mutex<MemStorageInner>>,

    inner: Option<MemFileWriter>,
}

#[async_trait]
impl FileWriter for MemStorageFileWriter {
    async fn write_all(&mut self, src: &[u8]) -> io::Result<()> {
        self.inner
            .as_mut()
            .ok_or_else(|| io::Error::other(anyhow!("writer already closed")))?
            .write_all(src)
            .await
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        let inner = self.inner.take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, anyhow!("writer already closed"))
        })?;
        let mut parent = self.parent.lock().unwrap();
        parent
            .files
            .insert(self.name.clone(), Arc::new(inner.into_reader()));
        Ok(())
    }
}
