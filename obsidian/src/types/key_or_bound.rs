use std::cmp::Ordering;
use std::fmt::Debug;
use std::ops::Deref;

use crate::types::bound::Key;
use crate::util::hexlify;
use crate::Bound;

#[derive(Eq, PartialEq, Clone)]
pub enum KeyOrBound<K> {
    Key(K),
    Bound(Bound<K>),
}

impl<K> KeyOrBound<K> {
    pub fn as_key(self) -> Option<K> {
        match self {
            KeyOrBound::Key(k) => Some(k),
            _ => None,
        }
    }
}

impl<K: Key> Ord for KeyOrBound<K> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (KeyOrBound::Key(self_k), KeyOrBound::Key(other_k)) => self_k.cmp(other_k),
            (KeyOrBound::Key(self_k), KeyOrBound::Bound(other_b)) => {
                other_b.cmp_key(self_k).reverse()
            }
            (KeyOrBound::Bound(self_b), KeyOrBound::Key(other_k)) => self_b.cmp_key(other_k),
            (KeyOrBound::Bound(self_b), KeyOrBound::Bound(other_b)) => self_b.cmp(other_b),
        }
    }
}

impl<K: Key> PartialOrd for KeyOrBound<K> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: Deref<Target = [u8]>> Debug for KeyOrBound<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Key(k) => write!(f, "key({})", hexlify(k)),
            Self::Bound(k) => write!(f, "bound({:?})", k),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Bound;
    use super::KeyOrBound;

    #[test]
    fn test_key_or_bound_ord() {
        let items: Vec<KeyOrBound<Vec<u8>>> = vec![
            KeyOrBound::Bound(Bound::BeforeAll),
            KeyOrBound::Bound(Bound::Before(vec![])),
            KeyOrBound::Key(vec![]),
            KeyOrBound::Bound(Bound::After(vec![])),
            KeyOrBound::Bound(Bound::Before(vec![0])),
            KeyOrBound::Key(vec![0]),
            KeyOrBound::Bound(Bound::After(vec![0])),
            KeyOrBound::Bound(Bound::Before(vec![0, 0])),
            KeyOrBound::Key(vec![0, 0]),
            KeyOrBound::Bound(Bound::After(vec![0, 0])),
            KeyOrBound::Bound(Bound::AfterPrefix(vec![0, 0])),
            KeyOrBound::Bound(Bound::Before(vec![0, 1])),
            KeyOrBound::Key(vec![0, 1]),
            KeyOrBound::Bound(Bound::After(vec![0, 1])),
            KeyOrBound::Bound(Bound::AfterPrefix(vec![0, 1])),
            KeyOrBound::Bound(Bound::AfterPrefix(vec![0])),
            KeyOrBound::Bound(Bound::Before(vec![1])),
            KeyOrBound::Key(vec![1]),
            KeyOrBound::Bound(Bound::After(vec![1])),
            KeyOrBound::Bound(Bound::Before(vec![1, 0])),
            KeyOrBound::Key(vec![1, 0]),
            KeyOrBound::Bound(Bound::After(vec![1, 0])),
            KeyOrBound::Bound(Bound::AfterPrefix(vec![1, 0])),
            KeyOrBound::Bound(Bound::AfterPrefix(vec![1])),
            KeyOrBound::Bound(Bound::AfterPrefix(vec![])),
            KeyOrBound::Bound(Bound::AfterAll),
        ];

        for i in 0..items.len() {
            for j in 0..items.len() {
                assert_eq!(
                    i.cmp(&j),
                    items[i].cmp(&items[j]),
                    "{:?} {:?} misordered",
                    items[i],
                    items[j],
                );
            }
        }
    }
}
