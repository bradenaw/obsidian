use std::fmt::Debug;
use std::fmt::Display;

#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct ColoGroupId(pub u32);

impl ColoGroupId {
    pub const META: Self = ColoGroupId(u32::MAX);
    pub const SHARD_META: Self = ColoGroupId(u32::MAX - 1);
    pub const INTERNAL_GC: Self = ColoGroupId(u32::MAX - 2);
}

impl Display for ColoGroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            ColoGroupId::META => write!(f, "meta"),
            ColoGroupId::SHARD_META => write!(f, "shard_meta"),
            ColoGroupId::INTERNAL_GC => write!(f, "internal_gc"),
            _ => write!(f, "{}", self.0),
        }
    }
}

impl Debug for ColoGroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cg:")?;
        Display::fmt(&self, f)
    }
}
