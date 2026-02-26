use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;

use crate::replica::shard_journal::TabletJournal;
use crate::runtime::Wal;
use crate::WalEntry;
use crate::WalSeq;

pub(crate) struct TabletJournalReader {
}

impl TabletJournalReader {
    pub fn new(inner: Arc<TabletJournal>) -> Self {
        todo!();
    }
}

#[async_trait]
impl Wal for TabletJournalReader {
    async fn append(&self, entry: WalEntry) -> anyhow::Result<WalSeq> {
        todo!();
    }

    fn read(
        &self,
        first: WalSeq,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<(WalSeq, WalEntry)>> + Send + '_>> {
        todo!();
    }

    fn tail(
        &self,
        first: WalSeq,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<(WalSeq, WalEntry)>> + Send + '_>> {
        todo!();
    }

    async fn oldest_available(&self) -> anyhow::Result<WalSeq> {
        todo!();
    }

    async fn trim(&self, before: WalSeq) -> anyhow::Result<()> {
        todo!();
    }
}
