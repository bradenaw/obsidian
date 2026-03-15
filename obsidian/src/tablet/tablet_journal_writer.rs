use async_trait::async_trait;

use crate::WalEntry;

#[async_trait]
pub(crate) trait TabletJournalWriter: Send + Sync + 'static {
    async fn append(&self, entry: WalEntry) -> anyhow::Result<()>;
}
