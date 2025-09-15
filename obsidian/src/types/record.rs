use std::fmt::Debug;

use anyhow::anyhow;

use crate::pb;
use crate::types::Key;
use crate::types::Timestamp;
use crate::util::hexlify;

#[derive(Eq, PartialEq, Clone)]
pub struct Record {
    pub key: Key,
    pub ts: Timestamp,
    pub value: Vec<u8>,
}

impl TryFrom<pb::Record> for Record {
    type Error = anyhow::Error;

    fn try_from(value: pb::Record) -> Result<Self, Self::Error> {
        let key: Key = value
            .key
            .ok_or_else(|| anyhow!("missing key"))?
            .try_into()?;
        let ts = Timestamp::from_nanos(value.ts);

        Ok(Self {
            key,
            ts,
            value: value.value,
        })
    }
}

impl From<Record> for pb::Record {
    fn from(value: Record) -> Self {
        Self {
            key: Some(value.key.into()),
            ts: value.ts.as_nanos(),
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
