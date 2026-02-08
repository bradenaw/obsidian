use std::convert::TryFrom;

use anyhow::anyhow;

use crate::tuple_encoding::tuple_decode;
use crate::tuple_encoding::tuple_decode_prefix;
use crate::tuple_encoding::tuple_encode;
use crate::ColoGroupId;
use crate::KeyspaceId;
use crate::Range;
use crate::ShardId;
use crate::TabletId;
use crate::TransferId;

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) enum MetaKey {
    Sync,
    Shard(ShardId),
    ColoGroup(ColoGroupId),
    Keyspace(KeyspaceId),
    Tablet(TabletId),
    Transfer(TransferId),
}

impl MetaKey {
    // (PFX_SYNC) -> pb::internal::MetaTx
    const PFX_SYNC: u64 = 1;

    // (PFX_SHARDS, shard_id) => []
    const PFX_SHARDS: u64 = 2;

    // (PFX_COLO_GROUPS, colo_group_id) -> []
    const PFX_COLO_GROUPS: u64 = 3;

    // (PFX_KEYSPACES, keyspace_id) -> []
    const PFX_KEYSPACES: u64 = 4;

    // (PFX_TABLETS, tablet_id) -> pb::internal::TabletMetadata
    const PFX_TABLETS: u64 = 5;

    // (PFX_TRANSFERS, transfer_id) -> pb::internal::TransferMetadata
    const PFX_TRANSFERS: u64 = 6;

    pub(crate) fn encode(&self) -> Vec<u8> {
        match self {
            Self::Sync => tuple_encode(&(Self::PFX_SYNC,)),
            Self::Shard(shard_id) => tuple_encode(&(Self::PFX_SHARDS, shard_id.0 as u64)),
            Self::ColoGroup(colo_group_id) => {
                tuple_encode(&(Self::PFX_COLO_GROUPS, colo_group_id.0 as u64))
            }
            Self::Keyspace(keyspace_id) => tuple_encode(&(
                Self::PFX_KEYSPACES,
                keyspace_id.0 .0 as u64,
                keyspace_id.1 as u64,
            )),
            Self::Tablet(tablet_id) => {
                tuple_encode(&(Self::PFX_TABLETS, tablet_id.0 .0 as u64, tablet_id.1))
            }
            Self::Transfer(transfer_id) => tuple_encode(&(Self::PFX_TRANSFERS, transfer_id.0)),
        }
    }

    pub(crate) fn decode(b: &[u8]) -> anyhow::Result<Self> {
        let prefix = tuple_decode_prefix::<(u64,)>(b)?.0;
        match prefix {
            Self::PFX_SYNC => Ok(Self::Sync),
            Self::PFX_SHARDS => {
                let (_, shard_id_raw): (u64, u64) = tuple_decode(b)?;
                Ok(Self::Shard(ShardId(u32::try_from(shard_id_raw)?)))
            }
            Self::PFX_COLO_GROUPS => {
                let (_, colo_group_id_raw): (u64, u64) = tuple_decode(b)?;
                Ok(Self::ColoGroup(ColoGroupId(u32::try_from(
                    colo_group_id_raw,
                )?)))
            }
            Self::PFX_KEYSPACES => {
                let (_, colo_group_id_raw, keyspace_id_raw): (u64, u64, u64) = tuple_decode(b)?;
                Ok(Self::Keyspace(KeyspaceId(
                    ColoGroupId(u32::try_from(colo_group_id_raw)?),
                    u32::try_from(keyspace_id_raw)?,
                )))
            }
            Self::PFX_TABLETS => {
                let (_, shard_id_raw, tablet_id_raw): (u64, u64, u64) = tuple_decode(b)?;
                Ok(Self::Tablet(TabletId(
                    ShardId(u32::try_from(shard_id_raw)?),
                    tablet_id_raw,
                )))
            }
            Self::PFX_TRANSFERS => {
                let (_, transfer_id_raw): (u64, uuid::Uuid) = tuple_decode(b)?;
                let transfer_id = TransferId(transfer_id_raw);
                Ok(Self::Transfer(transfer_id))
            }
            _ => Err(anyhow!("unrecognized MetaKey prefix {}", prefix)),
        }
    }

    pub fn shards() -> Range<Vec<u8>> {
        Range::prefix(tuple_encode(&(Self::PFX_SHARDS,)))
    }

    pub fn colo_groups() -> Range<Vec<u8>> {
        Range::prefix(tuple_encode(&(Self::PFX_COLO_GROUPS,)))
    }

    pub fn keyspaces() -> Range<Vec<u8>> {
        Range::prefix(tuple_encode(&(Self::PFX_KEYSPACES,)))
    }

    pub fn tablets() -> Range<Vec<u8>> {
        Range::prefix(tuple_encode(&(Self::PFX_TABLETS,)))
    }
}
