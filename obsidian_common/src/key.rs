use anyhow::anyhow;
use obsidian_pb as pb;

use crate::KeyspaceId;

pub type Key = (KeyspaceId, Vec<u8>);

pub fn key_to_proto((keyspace_id, bytes): Key) -> pb::Key {
    pb::Key {
        keyspace_id: Some(keyspace_id.into()),
        bytes,
    }
}

pub fn key_from_proto(value: pb::Key) -> anyhow::Result<Key> {
    let keyspace_id = KeyspaceId::try_from(
        value
            .keyspace_id
            .ok_or_else(|| anyhow!("missing keyspace_id"))?,
    )?;

    Ok((keyspace_id, value.bytes))
}
