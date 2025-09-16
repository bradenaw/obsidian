use std::cmp::Ordering;
use std::fmt::Debug;

use crate::util::hexlify;
use crate::Key;
use crate::Timestamp;

#[derive(Eq, PartialEq, Clone)]
pub struct Revision {
    pub key: Key,
    pub ts: Timestamp,
    pub value: RevisionValue,
}

impl PartialOrd for Revision {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Revision {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.cmp(&other.key) {
            Ordering::Equal => {}
            ord => return ord,
        }
        self.ts.cmp(&other.ts).reverse()
    }
}

impl Debug for Revision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rev:{}/[{}]@{}:{:?}",
            self.key.0,
            hexlify(&self.key.1),
            self.ts,
            self.value
        )
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum RevisionValue {
    Regular(Vec<u8>),
    Tombstone,
}

impl RevisionValue {
    pub fn len(&self) -> usize {
        match self {
            RevisionValue::Regular(v) => v.len(),
            RevisionValue::Tombstone => 0,
        }
    }
}

impl Debug for RevisionValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RevisionValue::Regular(v) => write!(f, "[{}]", hexlify(v)),
            RevisionValue::Tombstone => write!(f, "<TOMBSTONE>"),
        }
    }
}
