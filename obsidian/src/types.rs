use std::cmp::Ordering;
use std::fmt::Debug;
use std::fmt::Display;
use std::time::Duration;
use std::time::SystemTime;

use thiserror::Error;

use crate::util::hexlify;

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Debug)]
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

#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct ColoGroupId(pub u32);

impl ColoGroupId {
    pub(crate) const META: Self = ColoGroupId(0xFFFFFFFF);

    pub(crate) fn is_reserved(&self) -> bool {
        *self != Self::META
    }
}

impl Display for ColoGroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Debug for ColoGroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "colo_group:")?;
        Display::fmt(&self.0, f)
    }
}

#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct KeyspaceId(pub ColoGroupId, pub u32);

impl Display for KeyspaceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.0, self.1)
    }
}

impl Debug for KeyspaceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "keyspace:")?;
        Display::fmt(&self.0, f)
    }
}

impl KeyspaceId {
    pub(crate) const TX_OUTCOMES: Self = Self(ColoGroupId::META, 0xFE000001);

    pub(crate) fn userland(&self) -> Option<KeyspaceId> {
        if !self.is_pending() && !self.is_precond() {
            return None;
        }
        Some(KeyspaceId(self.0, self.1 & 0x00FFFFFF))
    }

    pub(crate) fn is_userland(&self) -> bool {
        self.1 & 0xFF000000 == 0
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

#[derive(Error, Debug)]
pub enum InternalError {
    #[error(transparent)]
    TransitionRejected(anyhow::Error),
    #[error(transparent)]
    TransitionFatal(anyhow::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct ShardId(pub(crate) u32);

impl Display for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl Debug for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "shard:")?;
        std::fmt::Display::fmt(self, f)
    }
}

#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct TabletId(pub(crate) ShardId, pub(crate) u64);

impl TabletId {
    const META: Self = TabletId(ShardId(1), 1);

    pub(crate) fn shard_meta(shard_id: ShardId) -> Self {
        TabletId(shard_id, 1)
    }

    pub(crate) fn shard_id(&self) -> ShardId {
        self.0
    }
}

impl Display for TabletId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}/{}", self.0, self.1)
    }
}

impl Debug for TabletId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "tablet:")?;
        std::fmt::Display::fmt(self, f)
    }
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Copy)]
pub(crate) struct TransferId(uuid::Uuid);

impl Display for TransferId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        Display::fmt(&self.0, f)
    }
}

impl std::fmt::Debug for TransferId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "xfer:")?;
        Display::fmt(self, f)
    }
}
