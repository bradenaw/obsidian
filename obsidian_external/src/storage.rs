use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;
use obsidian_common::RunId;

use crate::FileReader;
use crate::FileWriter;

#[async_trait]
pub trait Storage: Sync + Send + 'static {
    async fn put(&self, name: FileName) -> anyhow::Result<Box<dyn FileWriter>>;

    async fn delete(&self, name: FileName) -> anyhow::Result<()>;

    async fn get(&self, name: FileName) -> anyhow::Result<Arc<dyn FileReader>>;

    /// List all of the files in storage. Files that are put() concurrently may or may not appear.
    fn list(&self) -> Pin<Box<dyn Stream<Item = anyhow::Result<FileName>>>>;
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum FileName {
    Run(RunId),
}

#[async_trait]
impl Storage for Arc<dyn Storage> {
    async fn put(&self, name: FileName) -> anyhow::Result<Box<dyn FileWriter>> {
        self.deref().put(name).await
    }

    async fn delete(&self, name: FileName) -> anyhow::Result<()> {
        self.deref().delete(name).await
    }

    async fn get(&self, name: FileName) -> anyhow::Result<Arc<dyn FileReader>> {
        self.deref().get(name).await
    }

    fn list(&self) -> Pin<Box<dyn Stream<Item = anyhow::Result<FileName>>>> {
        self.deref().list()
    }
}
