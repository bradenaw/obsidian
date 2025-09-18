use std::fmt::Debug;
use std::fmt::Display;

use crate::pb;

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy)]
pub(crate) struct TransferId(pub(crate) uuid::Uuid);

impl TransferId {
    pub(crate) fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }
}

impl From<TransferId> for pb::internal::Uuid {
    fn from(value: TransferId) -> Self {
        let (high, low) = value.0.as_u64_pair();
        Self {
            high: high,
            low: low,
        }
    }
}

impl From<pb::internal::Uuid> for TransferId {
    fn from(value: pb::internal::Uuid) -> Self {
        Self(uuid::Uuid::from_u64_pair(value.high, value.low))
    }
}

impl Display for TransferId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        Display::fmt(&self.0, f)
    }
}

impl Debug for TransferId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "xfer:")?;
        Display::fmt(self, f)
    }
}
