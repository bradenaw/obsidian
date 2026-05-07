use std::fmt::Debug;
use std::time::SystemTime;

use byteorder::BigEndian;
use byteorder::ByteOrder;
use obsidian_pb as pb;
use obsidian_util::hexlify;
use obsidian_util::Decode;
use obsidian_util::Encode;

use crate::ShardId;

#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct Txid {
    pub ts: u64,
    pub rand: [u8; 16],
    /// The shard that will host the TxOutcome for this transaction in its ShardMetaTablet.
    pub owner: ShardId,
}

impl Txid {
    pub const ENCODED_LEN: usize = 28;

    pub fn new(owner: ShardId) -> Self {
        Txid {
            ts: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            rand: rand::random(),
            owner,
        }
    }

    pub fn next(mut self) -> Self {
        self.rand = rand::random();
        self.ts -= 1;
        return self;
    }

    pub fn can_preempt(&self, other: &Txid) -> bool {
        self < other
    }

    pub fn owner(&self) -> ShardId {
        self.owner
    }

    pub fn encode_fixed(&self) -> [u8; Self::ENCODED_LEN] {
        // Encode with tablet ID first so that they're routed properly as a part of TABLET_META
        // when used as a key.
        let mut out = [0u8; Self::ENCODED_LEN];
        BigEndian::write_u32(&mut out[0..4], self.owner.0);
        BigEndian::write_u64(&mut out[4..12], self.ts);
        out[12..28].copy_from_slice(&self.rand[..]);
        out
    }
}

impl Encode for Txid {
    fn encoded_size_estimate(&self) -> usize {
        Self::ENCODED_LEN
    }

    fn encode(&self, w: &mut Vec<u8>) {
        w.extend_from_slice(&self.encode_fixed()[..]);
    }
}

impl Decode for Txid {
    fn decode(value: &[u8]) -> anyhow::Result<Self> {
        if value.len() != Txid::ENCODED_LEN {
            anyhow::bail!("txid not {} bytes", Txid::ENCODED_LEN);
        }
        let owner = ShardId(BigEndian::read_u32(&value[0..4]));
        let ts = BigEndian::read_u64(&value[4..12]);
        let mut rand = [0u8; 16];
        rand.copy_from_slice(&value[12..28]);

        Ok(Self { ts, rand, owner })
    }
}

impl Debug for Txid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tx:{}/{}/{}", self.ts, hexlify(&self.rand), self.owner.0,)
    }
}

impl TryFrom<pb::internal::Txid> for Txid {
    type Error = anyhow::Error;

    fn try_from(value: pb::internal::Txid) -> Result<Self, Self::Error> {
        let mut rand = [0u8; 16];
        BigEndian::write_u64(&mut rand[..8], value.rand0);
        BigEndian::write_u64(&mut rand[8..], value.rand1);
        Ok(Txid {
            ts: value.ts,
            rand,
            owner: ShardId(value.owner_shard_id),
        })
    }
}

impl From<Txid> for pb::internal::Txid {
    fn from(value: Txid) -> Self {
        pb::internal::Txid {
            ts: value.ts,
            rand0: BigEndian::read_u64(&value.rand[..8]),
            rand1: BigEndian::read_u64(&value.rand[8..]),
            owner_shard_id: value.owner.0,
        }
    }
}
