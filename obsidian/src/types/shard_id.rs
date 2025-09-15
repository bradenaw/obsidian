use std::fmt::Debug;
use std::fmt::Display;

use byteorder::BigEndian;
use byteorder::ByteOrder;

#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct ShardId(pub(crate) u32);

impl ShardId {
    pub(crate) const ENCODED_LEN: usize = 4;

    pub(crate) fn encode_fixed(&self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0u8; Self::ENCODED_LEN];
        BigEndian::write_u32(&mut out[..], self.0);
        out
    }
}

impl Display for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Debug for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shard:{}", self)
    }
}
