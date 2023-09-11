use std::cmp::Ordering;
use std::fmt::Debug;
use std::fmt::Display;
use std::time::Duration;
use std::time::SystemTime;

use thiserror::Error;

use crate::pb;
use crate::util::hexlify;

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Copy)]
pub struct Timestamp(pub(crate) u64);

impl Timestamp {
    pub const ZERO: Self = Timestamp(0);
    pub const MAX: Self = Timestamp(u64::MAX);

    pub fn now() -> Self {
        Timestamp::from_nanos(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("now before UNIX_EPOCH?")
                .as_nanos() as u64,
        )
    }

    pub fn now_after(other: Timestamp) -> Self {
        std::cmp::max(Timestamp(other.0 + 1), Self::now())
    }

    pub fn from_nanos(x: u64) -> Self {
        Timestamp(x)
    }

    pub fn as_nanos(&self) -> u64 {
        self.0
    }

    pub fn plus_one(&self) -> Timestamp {
        Timestamp(self.0 + 1)
    }

    pub fn minus_one(&self) -> Timestamp {
        Timestamp(self.0 - 1)
    }

    pub fn checked_duration_since(&self, earlier: Timestamp) -> Option<Duration> {
        self.0.checked_sub(earlier.0).map(Duration::from_nanos)
    }

    pub fn saturating_duration_since(&self, earlier: Timestamp) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Debug for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ts:")?;
        Display::fmt(self, f)
    }
}

#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct ColoGroupId(pub u32);

impl ColoGroupId {
    pub(crate) const META: Self = ColoGroupId(0xFFFFFFFF);
    pub(crate) const TABLET_META: Self = ColoGroupId(0xFFFFFFFE);
}

impl Display for ColoGroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            ColoGroupId::META => write!(f, "meta"),
            ColoGroupId::TABLET_META => write!(f, "tablet_meta"),
            _ => write!(f, "{}", self.0),
        }
    }
}

impl Debug for ColoGroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "colo:")?;
        Display::fmt(&self, f)
    }
}

#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct KeyspaceId(pub ColoGroupId, pub u32);

impl KeyspaceId {
    pub(crate) const META: Self = Self(ColoGroupId::META, 1);
    pub(crate) const TX_OUTCOMES: Self = Self(ColoGroupId::TABLET_META, 2);

    pub(crate) fn userland(&self) -> Option<KeyspaceId> {
        if !self.is_pending() && !self.is_precond() {
            return None;
        }
        Some(KeyspaceId(self.0, self.1 & 0x00FFFFFF))
    }

    pub(crate) fn is_userland(&self) -> bool {
        self.0 != ColoGroupId::TABLET_META && self.1 & 0xFF000000 == 0
    }

    pub(crate) fn pending(&self) -> Option<KeyspaceId> {
        if !self.is_userland() {
            return None;
        }
        Some(KeyspaceId(self.0, self.1 | 0x01000000))
    }

    pub(crate) fn is_pending(&self) -> bool {
        self.1 & 0xFF000000 == 0x01000000
    }

    pub(crate) fn precond(&self) -> Option<KeyspaceId> {
        if !self.is_userland() {
            return None;
        }
        Some(KeyspaceId(self.0, self.1 | 0x02000000))
    }

    pub(crate) fn is_precond(&self) -> bool {
        self.1 & 0xFF000000 == 0x02000000
    }
}

impl Display for KeyspaceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/", self.0)?;
        if *self == KeyspaceId::META {
            f.write_str("meta")?;
            return Ok(());
        }
        if *self == KeyspaceId::TX_OUTCOMES {
            f.write_str("tx_outcomes")?;
            return Ok(());
        }
        match self.userland() {
            Some(userland_keyspace_id) => {
                if self.is_precond() {
                    write!(f, "precond({})", userland_keyspace_id.1)?;
                } else if self.is_pending() {
                    write!(f, "pending({})", userland_keyspace_id.1)?;
                } else {
                    write!(f, "{}", self.1)?;
                }
            }
            None => {
                write!(f, "{}", self.1)?;
            }
        }
        Ok(())
    }
}

impl Debug for KeyspaceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ksp:")?;
        Display::fmt(&self, f)
    }
}

impl From<KeyspaceId> for pb::KeyspaceId {
    fn from(keyspace_id: KeyspaceId) -> Self {
        pb::KeyspaceId {
            colo_group_id: keyspace_id.0 .0,
            id: keyspace_id.1,
        }
    }
}

impl TryFrom<pb::KeyspaceId> for KeyspaceId {
    type Error = anyhow::Error;

    fn try_from(value: pb::KeyspaceId) -> Result<Self, Self::Error> {
        Ok(KeyspaceId(ColoGroupId(value.colo_group_id), value.id))
    }
}

#[derive(Eq, PartialEq, Clone)]
pub struct Record {
    pub key: Vec<u8>,
    pub ts: Timestamp,
    pub value: Value,
}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Record {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.cmp(&other.key) {
            Ordering::Equal => {}
            ord => return ord,
        }
        self.ts.cmp(&other.ts).reverse()
    }
}

impl Debug for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] @ {}: {:?}",
            hexlify(&self.key),
            self.ts,
            self.value
        )
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum Value {
    Regular(Vec<u8>),
    Tombstone,
}

impl Value {
    pub fn len(&self) -> usize {
        match self {
            Value::Regular(v) => v.len(),
            Value::Tombstone => 0,
        }
    }
}

impl Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Regular(v) => write!(f, "[{}]", hexlify(v)),
            Value::Tombstone => write!(f, "<TOMBSTONE>"),
        }
    }
}

#[derive(Eq, PartialEq, Copy, Clone)]
pub enum Direction {
    Asc,
    Desc,
}

impl Debug for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Direction::Asc => f.write_str("asc"),
            Direction::Desc => f.write_str("desc"),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Precondition {
    NotChangedSince(KeyspaceId, Vec<u8>, Timestamp),
}

impl Precondition {
    pub fn keyspace_id(&self) -> KeyspaceId {
        match self {
            Precondition::NotChangedSince(keyspace_id, _, _) => *keyspace_id,
        }
    }
    pub fn key(&self) -> &[u8] {
        match self {
            Precondition::NotChangedSince(_, key, _) => &key,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Mutation {
    Put(Vec<u8>),
    Delete,
}

impl Mutation {
    pub fn len(&self) -> usize {
        match self {
            Mutation::Put(v) => v.len(),
            Mutation::Delete => 0,
        }
    }
}

#[derive(Error, Debug)]
pub enum WriteError {
    #[error("precondition failed")]
    PreconditionFailed,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct ShardId(pub(crate) u32);

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum HistoryRange {
    All,
    Until(Timestamp),
    Between(Timestamp, Timestamp),
    Since(Timestamp),
}

impl HistoryRange {
    pub(crate) fn as_min_max(&self) -> (Timestamp, Timestamp) {
        match self {
            HistoryRange::All => (Timestamp::ZERO, Timestamp::MAX),
            HistoryRange::Until(max) => (Timestamp::ZERO, *max),
            HistoryRange::Between(min, max) => (*min, *max),
            HistoryRange::Since(min) => (*min, Timestamp::MAX),
        }
    }

    pub(crate) fn contains(&self, ts: Timestamp) -> bool {
        let (min, max) = self.as_min_max();
        min <= ts && ts <= max
    }

    pub(crate) fn intersects(&self, min: Timestamp, max: Timestamp) -> bool {
        let (self_min, self_max) = self.as_min_max();
        !(self_max < min || self_min > max)
    }
}
