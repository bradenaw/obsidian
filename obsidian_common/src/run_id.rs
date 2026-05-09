use std::fmt::Debug;
use std::fmt::Display;

use obsidian_pb as pb;
use uuid::Uuid;

use crate::uuid_from_proto;
use crate::uuid_to_proto;

#[derive(Eq, PartialEq, Hash, Clone, Copy)]
pub struct RunId(Uuid);

impl RunId {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn encode_fixed(&self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out.copy_from_slice(self.0.as_bytes());
        out
    }
}

impl Display for RunId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl Debug for RunId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("run:")?;
        Display::fmt(self, f)
    }
}

impl From<Uuid> for RunId {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

impl From<pb::internal::Uuid> for RunId {
    fn from(value: pb::internal::Uuid) -> Self {
        RunId(uuid_from_proto(value))
    }
}

impl From<RunId> for pb::internal::Uuid {
    fn from(value: RunId) -> Self {
        uuid_to_proto(value.0)
    }
}
