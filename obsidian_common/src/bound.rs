use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::ops::Deref;

use anyhow::anyhow;
use obsidian_pb as pb;
use obsidian_util::hexlify;

pub trait Key: Deref<Target = [u8]> + Clone + Eq + Ord {}

impl<T: Deref<Target = [u8]> + Clone + Eq + Ord> Key for T {}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Bound<K> {
    BeforeAll,
    Before(K),
    After(K),
    AfterPrefix(K),
    AfterAll,
}

impl<K: Key> Bound<K> {
    pub fn map<K2, F: FnOnce(K) -> K2>(self, f: F) -> Bound<K2> {
        match self {
            Bound::BeforeAll => Bound::BeforeAll,
            Bound::Before(v) => Bound::Before(f(v)),
            Bound::After(v) => Bound::After(f(v)),
            Bound::AfterPrefix(v) => Bound::AfterPrefix(f(v)),
            Bound::AfterAll => Bound::AfterAll,
        }
    }

    pub fn borrow(&self) -> Bound<&[u8]> {
        match self {
            Bound::BeforeAll => Bound::BeforeAll,
            Bound::Before(v) => Bound::Before(&v),
            Bound::After(v) => Bound::After(&v),
            Bound::AfterPrefix(v) => Bound::AfterPrefix(&v),
            Bound::AfterAll => Bound::AfterAll,
        }
    }
}

impl Bound<&[u8]> {
    pub fn to_vec(&self) -> Bound<Vec<u8>> {
        self.clone().map(Vec::from)
    }
}

impl<K: Key> Bound<K> {
    pub fn cmp_key<K2: Key>(&self, other: &K2) -> Ordering {
        match self {
            Bound::BeforeAll => Ordering::Less,
            Bound::Before(k) => {
                if k.deref() == other.deref() {
                    Ordering::Less
                } else {
                    k.deref().cmp(other.deref())
                }
            }
            Bound::After(k) => {
                if k.deref() == other.deref() {
                    Ordering::Greater
                } else {
                    k.deref().cmp(other.deref())
                }
            }
            Bound::AfterPrefix(k) => {
                if other.starts_with(k) {
                    Ordering::Greater
                } else {
                    k.deref().cmp(other.deref())
                }
            }
            Bound::AfterAll => Ordering::Greater,
        }
    }
}

impl<K: Key> Ord for Bound<K> {
    fn cmp(&self, other: &Bound<K>) -> Ordering {
        match (self, other) {
            (Bound::BeforeAll, Bound::BeforeAll) => Ordering::Equal,
            (Bound::BeforeAll, _) => Ordering::Less,
            (_, Bound::BeforeAll) => Ordering::Greater,
            (Bound::AfterAll, Bound::AfterAll) => Ordering::Equal,
            (Bound::AfterAll, _) => Ordering::Greater,
            (_, Bound::AfterAll) => Ordering::Less,
            (
                Bound::Before(self_k) | Bound::After(self_k),
                Bound::Before(other_k) | Bound::After(other_k),
            ) if self_k != other_k => self_k.cmp(other_k),
            (Bound::Before(_), Bound::Before(_)) => Ordering::Equal,
            (Bound::Before(_), Bound::After(_)) => Ordering::Less,
            (Bound::After(_), Bound::Before(_)) => Ordering::Greater,
            (Bound::After(_), Bound::After(_)) => Ordering::Equal,
            (Bound::AfterPrefix(self_k), Bound::AfterPrefix(other_k)) => {
                if self_k == other_k {
                    Ordering::Equal
                } else if self_k.starts_with(other_k) {
                    Ordering::Less
                } else if other_k.starts_with(self_k) {
                    Ordering::Greater
                } else {
                    self_k.cmp(other_k)
                }
            }
            (Bound::AfterPrefix(self_k), Bound::Before(other_k) | Bound::After(other_k)) => {
                if other_k.starts_with(self_k) {
                    Ordering::Greater
                } else {
                    self_k.cmp(other_k)
                }
            }
            (Bound::Before(self_k) | Bound::After(self_k), Bound::AfterPrefix(other_k)) => {
                if self_k.starts_with(other_k) {
                    Ordering::Less
                } else {
                    self_k.cmp(other_k)
                }
            }
        }
    }
}

impl<K: Key> PartialOrd for Bound<K> {
    fn partial_cmp(&self, other: &Bound<K>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: Deref<Target = [u8]>> Debug for Bound<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Bound::BeforeAll => write!(f, "before_all"),
            Bound::Before(v) => write!(f, "before({})", hexlify(v)),
            Bound::After(v) => write!(f, "after({})", hexlify(v)),
            Bound::AfterPrefix(v) => write!(f, "after_prefix({})", hexlify(v)),
            Bound::AfterAll => write!(f, "after_all"),
        }
    }
}

impl Into<pb::Bound> for Bound<Vec<u8>> {
    fn into(self) -> pb::Bound {
        pb::Bound {
            bound_type: Some(match self {
                Bound::BeforeAll => pb::bound::BoundType::BeforeAll(()),
                Bound::Before(k) => pb::bound::BoundType::Before(k),
                Bound::After(k) => pb::bound::BoundType::After(k),
                Bound::AfterPrefix(k) => pb::bound::BoundType::AfterPrefix(k),
                Bound::AfterAll => pb::bound::BoundType::AfterAll(()),
            }),
        }
    }
}

impl TryFrom<pb::Bound> for Bound<Vec<u8>> {
    type Error = anyhow::Error;

    fn try_from(bound_pb: pb::Bound) -> Result<Self, Self::Error> {
        if let Some(bound_type_pb) = bound_pb.bound_type {
            return Ok(match bound_type_pb {
                pb::bound::BoundType::BeforeAll(()) => Bound::BeforeAll,
                pb::bound::BoundType::Before(k) => Bound::Before(k),
                pb::bound::BoundType::After(k) => Bound::After(k),
                pb::bound::BoundType::AfterPrefix(k) => Bound::AfterPrefix(k),
                pb::bound::BoundType::AfterAll(()) => Bound::AfterAll,
            });
        }
        Err(anyhow!("missing bound_type"))
    }
}

#[cfg(test)]
mod tests {
    use super::Bound;

    #[test]
    fn test_bound_ord() {
        let bounds: Vec<Bound<Vec<u8>>> = vec![
            Bound::BeforeAll,
            Bound::Before(vec![]),
            Bound::After(vec![]),
            Bound::Before(vec![0]),
            Bound::After(vec![0]),
            Bound::Before(vec![0, 0]),
            Bound::After(vec![0, 0]),
            Bound::AfterPrefix(vec![0, 0]),
            Bound::Before(vec![0, 1]),
            Bound::After(vec![0, 1]),
            Bound::AfterPrefix(vec![0, 1]),
            Bound::AfterPrefix(vec![0]),
            Bound::AfterPrefix(vec![]),
            Bound::AfterAll,
        ];

        for i in 0..bounds.len() {
            for j in 0..bounds.len() {
                assert_eq!(
                    i.cmp(&j),
                    bounds[i].cmp(&bounds[j]),
                    "{:?} {:?} misordered",
                    bounds[i],
                    bounds[j],
                );
            }
        }
    }
}
