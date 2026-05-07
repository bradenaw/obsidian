use crate::TabletId;
use crate::TabletJournalEntry;

#[derive(Clone)]
pub struct JournalEntry {
    pub tablet_id: TabletId,
    pub entry: TabletJournalEntry,
}
