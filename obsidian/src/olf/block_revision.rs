use std::cmp::Ordering;
use std::fmt::Debug;

use obsidian_util::hexlify;

use crate::RevisionValue;
use crate::Timestamp;

// Distinct from crate::Revision because blocks aren't aware of keyspace IDs. Here the keys are
// just Vec<u8>.
#[derive(Clone, Eq, PartialEq)]
pub(super) struct BlockRevision {
    pub key: Vec<u8>,
    pub ts: Timestamp,
    pub value: RevisionValue,
}

impl PartialOrd for BlockRevision {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BlockRevision {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.cmp(&other.key) {
            Ordering::Equal => {}
            ord => return ord,
        }
        self.ts.cmp(&other.ts).reverse()
    }
}

impl Debug for BlockRevision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rev:[{}]@{}:{:?}",
            hexlify(&self.key),
            self.ts,
            self.value
        )
    }
}
