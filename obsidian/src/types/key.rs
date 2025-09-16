use anyhow::anyhow;

use crate::pb;
use crate::KeyspaceId;

pub type Key = (KeyspaceId, Vec<u8>);

impl TryFrom<pb::Key> for Key {
    type Error = anyhow::Error;

    fn try_from(value: pb::Key) -> Result<Self, Self::Error> {
        let keyspace_id = KeyspaceId::try_from(
            value
                .keyspace_id
                .ok_or_else(|| anyhow!("missing keyspace_id"))?,
        )?;

        Ok((keyspace_id, value.bytes))
    }
}

impl From<Key> for pb::Key {
    fn from((keyspace_id, bytes): Key) -> Self {
        Self {
            keyspace_id: Some(keyspace_id.into()),
            bytes,
        }
    }
}
