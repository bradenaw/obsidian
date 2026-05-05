use anyhow::anyhow;
use prost::Message as _;

use crate::pb;
use crate::util::Decode;
use crate::util::Encode;
use crate::TabletId;
use crate::TabletJournalEntry;

#[derive(Clone)]
pub(crate) struct JournalEntry {
    pub tablet_id: TabletId,
    pub entry: TabletJournalEntry,
}

impl From<JournalEntry> for pb::internal::JournalEntry {
    fn from(value: JournalEntry) -> Self {
        Self {
            tablet_id: Some(value.tablet_id.into()),
            tablet_entry: Some(value.entry.into()),
        }
    }
}

impl TryFrom<pb::internal::JournalEntry> for JournalEntry {
    type Error = anyhow::Error;

    fn try_from(value: pb::internal::JournalEntry) -> Result<Self, Self::Error> {
        Ok(Self {
            tablet_id: TabletId::try_from(
                value
                    .tablet_id
                    .ok_or_else(|| anyhow!("missing tablet_id"))?,
            )?,
            entry: TabletJournalEntry::try_from(
                value
                    .tablet_entry
                    .ok_or_else(|| anyhow!("missing tablet_entry"))?,
            )?,
        })
    }
}

impl Encode for JournalEntry {
    fn encoded_size_estimate(&self) -> usize {
        0
    }

    fn encode(&self, w: &mut Vec<u8>) {
        pb::internal::JournalEntry::from(self.clone())
            .encode(w)
            .expect("only fails if the buffer is too small")
    }
}

impl Decode for JournalEntry {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        JournalEntry::try_from(pb::internal::JournalEntry::decode(b)?)
    }
}
