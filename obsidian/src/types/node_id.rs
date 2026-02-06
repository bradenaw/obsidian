use std::fmt::Debug;
use std::fmt::Display;

#[derive(Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct NodeId(pub String);

impl Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node:{}", self)
    }
}
