use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;

use futures::Stream;
use tokio::sync::Notify;

use crate::replica::tablet_journal::TabletJournalReader;
use crate::TabletId;
use crate::WalEntry;
use crate::WalSeq;

pub(crate) struct ShardEntry {
    pub tablet_id: TabletId,
    pub entry: WalEntry,
}

pub(crate) struct ShardJournal(Arc<RwLock<ShardJournalInner>>);

pub(super) struct ShardJournalInner {
    tablet_journals: HashMap<TabletId, Arc<TabletJournal>>,
}

pub(super) struct TabletJournal {
    notify: Notify,
    entries: RwLock<Vec<(WalSeq, WalEntry)>>,
}

impl ShardJournal {
    pub fn tablet_journal(&self, tablet_id: TabletId) -> TabletJournalReader {
        {
            let inner = self.0.read().unwrap();
            if let Some(tablet_journal) = inner.tablet_journals.get(&tablet_id) {
                return TabletJournalReader::new(Arc::clone(tablet_journal));
            }
        }

        {
            let mut inner = self.0.write().unwrap();
            return TabletJournalReader::new(Arc::clone(
                inner
                    .tablet_journals
                    .entry(tablet_id)
                    .or_insert_with(|| Arc::new(TabletJournal::new())),
            ));
        }
    }

    pub fn process_entry(&self, seq: WalSeq, shard_entry: ShardEntry) {
        let mut inner = self.0.write().unwrap();
        inner
            .tablet_journals
            .entry(shard_entry.tablet_id)
            .or_insert_with(|| Arc::new(TabletJournal::new()))
            .append(seq, shard_entry.entry);
    }
}

impl TabletJournal {
    fn new() -> Self {
        Self {
            notify: Notify::new(),
            entries: RwLock::new(vec![]),
        }
    }

    fn append(&self, seq: WalSeq, entry: WalEntry) {
        let mut entries = self.entries.write().unwrap();
        entries.push((seq, entry));
        self.notify.notify_waiters();
    }
}
