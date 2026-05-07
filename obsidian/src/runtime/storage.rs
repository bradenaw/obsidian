use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
pub(crate) use obsidian_olf::FileReader;
pub(crate) use obsidian_olf::FileWriter;

use crate::RunId;

#[async_trait]
pub(crate) trait Storage: Sync + Send + 'static {
    async fn put(&self, name: FileName) -> anyhow::Result<Box<dyn FileWriter>>;

    async fn delete(&self, name: FileName) -> anyhow::Result<()>;

    async fn get(&self, name: FileName) -> anyhow::Result<Arc<dyn FileReader>>;
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum FileName {
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
}
