use anyhow::anyhow;
use byteorder::BigEndian;
use byteorder::ByteOrder;
use obsidian_pb as pb;
use obsidian_util::hexlify;
use obsidian_util::Decode;
use obsidian_util::Encode;

use crate::Range;
use crate::ShardId;

#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct TabletId(pub ShardId, pub u64);

impl TabletId {
    pub const ENCODED_LEN: usize = ShardId::ENCODED_LEN + 8;
    pub const META: Self = TabletId(ShardId::META, u64::MAX);
    pub const SHARD_META_SEQ: u64 = u64::MAX - 1;

    pub fn shard_meta(shard_id: ShardId) -> Self {
        TabletId(shard_id, Self::SHARD_META_SEQ)
    }

    pub fn shard_meta_owned_range(shard_id: ShardId) -> Range<Vec<u8>> {
        if shard_id == ShardId(0) {
            return Range::empty();
        }
        Range::prefix(shard_id.encode_fixed().to_vec())
    }

    pub fn encode_fixed(&self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0u8; Self::ENCODED_LEN];
        let shard_id_encoded = self.0.encode_fixed();
        (&mut out[..ShardId::ENCODED_LEN]).copy_from_slice(&shard_id_encoded[..]);
        BigEndian::write_u64(&mut out[ShardId::ENCODED_LEN..], self.1);
        out
    }
}

impl Encode for TabletId {
    fn encoded_size_estimate(&self) -> usize {
        Self::ENCODED_LEN
    }

    fn encode(&self, w: &mut Vec<u8>) {
        w.extend_from_slice(&self.encode_fixed()[..]);
    }
}

impl Decode for TabletId {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() != 12 {
            return Err(anyhow!(
                "tablet ID must be 12 bytes, got {}: {}",
                b.len(),
                hexlify(b)
            ));
        }
        return Ok(TabletId(
            ShardId(BigEndian::read_u32(&b[0..4])),
            BigEndian::read_u64(&b[4..12]),
        ));
    }
}

impl From<TabletId> for pb::internal::TabletId {
    fn from(value: TabletId) -> Self {
        Self {
            shard_id: value.0 .0,
            id: value.1,
        }
    }
}

impl TryFrom<pb::internal::TabletId> for TabletId {
    type Error = anyhow::Error;

    fn try_from(value: pb::internal::TabletId) -> Result<Self, Self::Error> {
        Ok(Self(ShardId(value.shard_id), value.id))
    }
}

impl std::fmt::Display for TabletId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        if *self == TabletId::META {
            f.write_str("meta")
        } else if self.1 == TabletId::SHARD_META_SEQ {
            write!(f, "{}/shard_meta", self.0 .0)
        } else {
            write!(f, "{}/{}", self.0 .0, self.1)
        }
    }
}

impl std::fmt::Debug for TabletId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        f.write_str("tablet:")?;
        std::fmt::Display::fmt(self, f)
    }
}
