use std::fmt::Debug;
use std::fmt::Display;

use uuid::Uuid;

#[derive(Eq, PartialEq, Hash, Clone, Copy)]
pub(crate) struct RunId(Uuid);

impl RunId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn encode_fixed(&self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out.copy_from_slice(self.0.as_bytes());
        out
    }

    pub fn to_string(&self) -> String {
        self.0.to_string()
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
