use std::fmt::Debug;

use anyhow::anyhow;

use crate::pb;
use crate::Key;
use crate::KeyspaceId;
use crate::Timestamp;

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

impl TryFrom<pb::Precondition> for Precondition {
    type Error = anyhow::Error;

    fn try_from(value: pb::Precondition) -> Result<Self, Self::Error> {
        match value.precond_type {
            Some(pb::precondition::PrecondType::NotChangedSince(not_changed_since)) => {
                let (keyspace_id, key_bytes): Key = not_changed_since
                    .key
                    .ok_or_else(|| anyhow!("missing key"))?
                    .try_into()?;
                let ts = Timestamp::from_nanos(not_changed_since.ts);
                Ok(Precondition::NotChangedSince(keyspace_id, key_bytes, ts))
            }
            None => return Err(anyhow!("missing precond_type on Precondition")),
        }
    }
}
impl From<Precondition> for pb::Precondition {
    fn from(_: Precondition) -> Self {
        todo!()
    }
}
