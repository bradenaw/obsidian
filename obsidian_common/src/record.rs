use std::fmt::Debug;

use anyhow::anyhow;
use obsidian_pb as pb;
use obsidian_util::hexlify;

use crate::key_from_proto;
use crate::key_to_proto;
use crate::Key;
use crate::Timestamp;

#[derive(Eq, PartialEq, Clone)]
pub struct Record {
    pub key: Key,
    pub ts: Timestamp,
    pub value: Vec<u8>,
}

impl TryFrom<pb::Record> for Record {
    type Error = anyhow::Error;

    fn try_from(value: pb::Record) -> Result<Self, Self::Error> {
        Ok(Self {
            key: key_from_proto(value.key.ok_or_else(|| anyhow!("missing key"))?)?,
            ts: Timestamp::from_micros(value.ts),
            value: value.value,
        })
    }
}

impl From<Record> for pb::Record {
    fn from(value: Record) -> Self {
        Self {
            key: Some(key_to_proto(value.key)),
            ts: value.ts.as_micros(),
            value: value.value,
        }
    }
}

impl Debug for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rec:{}/[{}]@{}:[{}]",
            self.key.0,
            hexlify(&self.key.1),
            self.ts,
            hexlify(&self.value),
        )
    }
}
