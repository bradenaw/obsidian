use async_trait::async_trait;

use crate::TabletJournalEntry;

#[async_trait]
pub(crate) trait TabletJournalWriter: Send + Sync + 'static {
    async fn append(&self, entry: TabletJournalEntry) -> anyhow::Result<()>;
}
