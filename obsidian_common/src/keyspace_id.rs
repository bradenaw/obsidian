use std::fmt::Debug;
use std::fmt::Display;

use anyhow::anyhow;
use obsidian_pb as pb;

use crate::ColoGroupId;

#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
/// Only the bottom 24 bits of the second item are usable by users.
pub struct KeyspaceId(pub ColoGroupId, pub u32);

impl KeyspaceId {
    pub const META: Self = Self(ColoGroupId::META, 0xFF000000);
    pub const TX_OUTCOMES: Self = Self(ColoGroupId::SHARD_META, 0xFF000000);

    /// If this is a pending or precond keyspace, returns the associated data keyspace.
    pub fn data(&self) -> Option<KeyspaceId> {
        if !self.is_pending() && !self.is_precond() {
            return None;
        }
        Some(KeyspaceId(self.0, self.1 & 0x00FFFFFF))
    }

    pub fn is_data(&self) -> bool {
        matches!(self.keyspace_type(), Ok(KeyspaceType::Data))
    }

    /// If this is a data keyspace, returns the associated pending keyspace.
    pub fn pending(&self) -> Option<KeyspaceId> {
        if !self.is_data() {
            return None;
        }
        Some(KeyspaceId(self.0, self.1 | 0x01000000))
    }

    pub fn is_pending(&self) -> bool {
        matches!(self.keyspace_type(), Ok(KeyspaceType::Pending))
    }

    /// If this is a data keyspace, returns the associated precond keyspace.
    pub fn precond(&self) -> Option<KeyspaceId> {
        if !self.is_data() {
            return None;
        }
        Some(KeyspaceId(self.0, self.1 | 0x02000000))
    }

    pub fn is_precond(&self) -> bool {
        matches!(self.keyspace_type(), Ok(KeyspaceType::Precond))
    }

    pub fn is_meta(&self) -> bool {
        matches!(self.keyspace_type(), Ok(KeyspaceType::Meta))
    }

    pub fn keyspace_type(&self) -> anyhow::Result<KeyspaceType> {
        match (self.1 & 0xFF000000) >> 24 {
            0x00 => Ok(KeyspaceType::Data),
            0x01 => Ok(KeyspaceType::Pending),
            0x02 => Ok(KeyspaceType::Precond),
            0xFF => Ok(KeyspaceType::Meta),
            v => Err(anyhow!("unrecognized keyspace type {:02x}", v)),
        }
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
        match self.data() {
            Some(data_keyspace_id) => {
                if self.is_precond() {
                    write!(f, "precond({})", data_keyspace_id.1)?;
                } else if self.is_pending() {
                    write!(f, "pending({})", data_keyspace_id.1)?;
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

pub enum KeyspaceType {
    /// Data keyspaces hold the application data visible to the outside. Each data keyspace has an
    /// associated pending and precond keyspace that holds intermediate records for 2PC writes in
    /// progress.
    Data,
    /// Holds PendingMutation records for 2PC writes in progress to Data keyspaces.
    Pending,
    /// Holds PrecondLocks records for 2PC writes in progress to Data keyspaces.
    Precond,
    /// Metadata keyspaces can't participate in 2PC, so they do not have associated Pending/Precond
    /// keyspaces.
    Meta,
}
