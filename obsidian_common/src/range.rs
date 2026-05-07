use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::ops::Deref;

use anyhow::anyhow;
use obsidian_pb as pb;

use crate::bound::Key;
use crate::Bound;

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Range<K> {
    pub lower: Bound<K>,
    pub upper: Bound<K>,
}

impl<K: Key> Range<K> {
    pub fn empty() -> Self {
        Self {
            lower: Bound::BeforeAll,
            upper: Bound::BeforeAll,
        }
    }

    pub fn all() -> Self {
        Self {
            lower: Bound::BeforeAll,
            upper: Bound::AfterAll,
        }
    }

    pub fn prefix(pfx: K) -> Self {
        Self {
            lower: Bound::Before(pfx.clone()),
            upper: Bound::AfterPrefix(pfx),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.lower >= self.upper
    }

    pub fn contains<K2: Key>(&self, k: &K2) -> bool {
        self.lower.cmp_key(k) != Ordering::Greater && self.upper.cmp_key(k) != Ordering::Less
    }

    pub fn contains_bound<K2: Key>(&self, bound: &Bound<K2>) -> bool {
        self.lower.borrow() < bound.borrow() && self.upper.borrow() > bound.borrow()
    }

    /// Returns true if `self` contains all of the keys in `other`.
    pub fn contains_range<K2: Key>(&self, other: &Range<K2>) -> bool {
        self.lower.borrow() <= other.lower.borrow() && self.upper.borrow() >= other.upper.borrow()
    }

    pub fn intersection(&self, other: &Range<K>) -> Range<K> {
        Range {
            lower: std::cmp::max(&self.lower, &other.lower).clone(),
            upper: std::cmp::min(&self.upper, &other.upper).clone(),
        }
    }

    pub fn intersects<K2: Key>(&self, other: &Range<K2>) -> bool {
        std::cmp::max(self.lower.borrow(), other.lower.borrow())
            < std::cmp::min(self.upper.borrow(), other.upper.borrow())
    }

    pub fn split(&self, b: &Bound<K>) -> (Range<K>, Range<K>) {
        if !self.contains_bound(b) {
            return (self.clone(), Range::empty());
        }
        (
            Range {
                lower: self.lower.clone(),
                upper: b.clone(),
            },
            Range {
                lower: b.clone(),
                upper: self.upper.clone(),
            },
        )
    }

    pub fn adjacent(&self, other: &Range<K>) -> bool {
        self.lower == other.upper || self.upper == other.lower
    }

    pub fn to_std_ops_bounds(
        &self,
        max_key_len: usize,
    ) -> Option<(std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>)> {
        let range_bounds = (
            match &self.lower {
                Bound::BeforeAll => std::ops::Bound::Unbounded,
                Bound::Before(k) => std::ops::Bound::Included(k.to_vec()),
                Bound::After(k) => std::ops::Bound::Excluded(k.to_vec()),
                Bound::AfterPrefix(k) => std::ops::Bound::Excluded(
                    k.iter()
                        .cloned()
                        .chain((0..max_key_len.saturating_sub(k.len())).map(|_| 0xFFu8))
                        .collect(),
                ),
                Bound::AfterAll => {
                    return None;
                }
            },
            match &self.upper {
                Bound::BeforeAll => {
                    return None;
                }
                Bound::Before(k) => std::ops::Bound::Excluded(k.to_vec()),
                Bound::After(k) => std::ops::Bound::Included(k.to_vec()),
                Bound::AfterPrefix(k) => std::ops::Bound::Included(
                    k.iter()
                        .cloned()
                        .chain((0..max_key_len.saturating_sub(k.len())).map(|_| 0xFFu8))
                        .collect(),
                ),
                Bound::AfterAll => std::ops::Bound::Unbounded,
            },
        );

        // BTreeMap panics in these situations because they're nonsense, but we only produce them
        // when the range is in fact empty.
        match (&range_bounds.0, &range_bounds.1) {
            (std::ops::Bound::Excluded(s), std::ops::Bound::Excluded(e))
                if s.deref() == e.deref() =>
            {
                return None;
            }
            (
                std::ops::Bound::Included(s) | std::ops::Bound::Excluded(s),
                std::ops::Bound::Included(e) | std::ops::Bound::Excluded(e),
            ) if s.deref() > e.deref() => {
                return None;
            }
            _ => {}
        }

        Some(range_bounds)
    }
}

impl Range<&[u8]> {
    pub fn to_vec(&self) -> Range<Vec<u8>> {
        Range {
            lower: self.lower.to_vec(),
            upper: self.upper.to_vec(),
        }
    }
}

impl Range<Vec<u8>> {
    pub fn borrow(&self) -> Range<&[u8]> {
        Range {
            lower: self.lower.borrow(),
            upper: self.upper.borrow(),
        }
    }
}

impl<K: Deref<Target = [u8]>> Debug for Range<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({:?}, {:?})", self.lower, self.upper)
    }
}

impl Into<pb::Range> for Range<Vec<u8>> {
    fn into(self) -> pb::Range {
        return pb::Range {
            lower: Some(self.lower.into()),
            upper: Some(self.upper.into()),
        };
    }
}

impl TryFrom<pb::Range> for Range<Vec<u8>> {
    type Error = anyhow::Error;

    fn try_from(value: pb::Range) -> Result<Self, Self::Error> {
        if let (Some(lower_pb), Some(upper_pb)) = (value.lower, value.upper) {
            let lower = Bound::try_from(lower_pb)?;
            let upper = Bound::try_from(upper_pb)?;
            return Ok(Range { lower, upper });
        }
        Err(anyhow!("missing bound"))
    }
}

/// If ranges are contiguous, returns the bounds that lie between them.
pub fn ranges_to_splits(
    mut ranges: Vec<Range<Vec<u8>>>,
) -> anyhow::Result<Vec<Bound<Vec<u8>>>> {
    ranges.sort_unstable_by(|a, b| Ord::cmp(&a.lower, &b.lower));
    let mut out = Vec::with_capacity(ranges.len() - 1);
    let ranges_len = ranges.len();
    for (i, range) in ranges.into_iter().enumerate() {
        if out.len() > 0 && out[out.len() - 1] != range.lower {
            return Err(anyhow!(
                "can't range_to_splits, ranges not contiguous: gap at {:?} {:?}",
                out[out.len() - 1],
                range.lower
            ));
        }
        if i < ranges_len - 1 {
            out.push(range.upper);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::Range;
    use crate::Bound;

    #[test]
    fn test_range_contains() {
        let empty: Vec<u8> = vec![];
        assert!(!Range::<&[u8]> {
            lower: Bound::BeforeAll,
            upper: Bound::BeforeAll,
        }
        .contains(&empty));
    }
}
