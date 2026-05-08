use std::fmt::Debug;
use std::fmt::Display;

use obsidian_pb as pb;

use crate::uuid_from_proto;
use crate::uuid_to_proto;

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy)]
pub struct TransferId(pub uuid::Uuid);

impl TransferId {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }
}

impl From<TransferId> for pb::internal::Uuid {
    fn from(value: TransferId) -> Self {
        uuid_to_proto(value.0)
    }
}

impl From<pb::internal::Uuid> for TransferId {
    fn from(value: pb::internal::Uuid) -> Self {
        Self(uuid_from_proto(value))
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
