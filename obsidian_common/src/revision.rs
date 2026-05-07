use std::cmp::Ordering;
use std::fmt::Debug;

use anyhow::anyhow;
use obsidian_pb as pb;
use obsidian_util::hexlify;

use crate::key::key_from_proto;
use crate::key::key_to_proto;
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

impl TryFrom<pb::Revision> for Revision {
    type Error = anyhow::Error;

    fn try_from(value: pb::Revision) -> Result<Self, Self::Error> {
        Ok(Revision {
            key: key_from_proto(value.key.ok_or_else(|| anyhow!("missing key"))?)?,
            ts: Timestamp::from_micros(value.ts),
            value: RevisionValue::try_from(value.value.ok_or_else(|| anyhow!("missing value"))?)?,
        })
    }
}

impl From<Revision> for pb::Revision {
    fn from(value: Revision) -> Self {
        pb::Revision {
            key: Some(key_to_proto(value.key)),
            ts: value.ts.as_micros(),
            value: Some(value.value.into()),
        }
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

impl TryFrom<pb::RevisionValue> for RevisionValue {
    type Error = anyhow::Error;

    fn try_from(value: pb::RevisionValue) -> Result<Self, Self::Error> {
        Ok(
            match value
                .value_type
                .ok_or_else(|| anyhow!("missing value_type"))?
            {
                pb::revision_value::ValueType::Regular(bytes) => RevisionValue::Regular(bytes),
                pb::revision_value::ValueType::Tombstone(_) => RevisionValue::Tombstone,
            },
        )
    }
}

impl From<RevisionValue> for pb::RevisionValue {
    fn from(value: RevisionValue) -> Self {
        pb::RevisionValue {
            value_type: Some(match value {
                RevisionValue::Regular(bytes) => pb::revision_value::ValueType::Regular(bytes),
                RevisionValue::Tombstone => pb::revision_value::ValueType::Tombstone(()),
            }),
        }
    }
}
