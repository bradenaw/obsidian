use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::iter::IntoIterator;

use crate::bound::Key;
use crate::range_set::RangeByLowerBound;
use crate::Bound;
use crate::Range;
use crate::RangeSet;

#[derive(Clone)]
pub struct RangeMap<K, V> {
    // Ranges are always non-overlapping but may be adjacent.
    m: BTreeMap<RangeByLowerBound<K>, V>,
}

impl<K, V> Default for RangeMap<K, V>
where
    K: Key,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> RangeMap<K, V>
where
    K: Key,
{
    pub fn new() -> Self {
        Self { m: BTreeMap::new() }
    }

    pub fn insert(&mut self, range: Range<K>, value: V) {
        self.remove(&range);
        self.m.insert(RangeByLowerBound(range), value);
    }

    pub fn get(&self, k: &K) -> Option<&V> {
        if let Some((range, value)) = self.last_less_or_equal(&Bound::Before(k.clone())) {
            if range.contains(k) {
                return Some(value);
            }
        }
        None
    }

    pub fn remove(&mut self, range: &Range<K>) {
        let range_set = RangeSet::from(range.clone());
        let intersecting_ranges: Vec<_> = self
            .intersecting_ranges(range)
            .map(|(other_range, _)| other_range.clone())
            .collect();
        for other_range in intersecting_ranges {
            let diff = RangeSet::from(other_range.clone()).difference(&range_set);
            if diff.is_empty() {
                self.m.remove(&RangeByLowerBound(other_range.clone()));
            } else if let Some(other_range_remaining) = diff.contiguous() {
                let value = self.m.remove(&RangeByLowerBound(other_range.clone()));
                self.m.insert(
                    RangeByLowerBound(other_range_remaining.clone()),
                    value.unwrap(),
                );
            }
        }
    }

    pub fn intersecting_ranges<'a>(
        &'a self,
        range: &'a Range<K>,
    ) -> impl Iterator<Item = (&'a Range<K>, &'a V)> {
        let below = self
            .ranges_range(
                std::ops::Bound::Unbounded,
                std::ops::Bound::Excluded(&range.lower),
            )
            .rev()
            .take(1)
            .filter(|(other_range, _)| !range.intersection(other_range).is_empty());
        let above = self.ranges_range(
            std::ops::Bound::Included(&range.lower),
            std::ops::Bound::Excluded(&range.upper),
        );

        Iterator::chain(below, above)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Range<K>, &V)> {
        self.m.iter().map(|(range, value)| (&range.0, value))
    }

    fn ranges_range(
        &self,
        lower: std::ops::Bound<&Bound<K>>, // TODO(bw): this should probably be Bound<Bound<&K>>
        upper: std::ops::Bound<&Bound<K>>,
    ) -> impl DoubleEndedIterator<Item = (&Range<K>, &V)> {
        self.m
            .range::<Bound<K>, (std::ops::Bound<&Bound<K>>, std::ops::Bound<&Bound<K>>)>((
                lower, upper,
            ))
            .map(|(range, value)| (range.borrow(), value))
    }

    fn last_less_or_equal(&self, bound: &Bound<K>) -> Option<(&Range<K>, &V)> {
        self.ranges_range(std::ops::Bound::Unbounded, std::ops::Bound::Included(bound))
            .next_back()
    }
}

impl<K, V> IntoIterator for RangeMap<K, V> {
    type Item = (Range<K>, V);
    type IntoIter = RangeMapIntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        RangeMapIntoIter {
            inner: self.m.into_iter(),
        }
    }
}

pub struct RangeMapIntoIter<K, V> {
    inner: std::collections::btree_map::IntoIter<RangeByLowerBound<K>, V>,
}

impl<K, V> Iterator for RangeMapIntoIter<K, V> {
    type Item = (Range<K>, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| (k.0, v))
    }
}

#[cfg(test)]
mod tests {
    use super::RangeMap;
    use crate::Bound;
    use crate::Range;

    #[test]
    fn test_range_map() {
        let mut m = RangeMap::new();

        let one = vec![1u8];
        let two = vec![2u8];
        let three = vec![3u8];
        let four = vec![4u8];

        assert_eq!(m.get(&one), None);

        m.insert(
            Range {
                lower: Bound::Before(two.clone()),
                upper: Bound::After(three.clone()),
            },
            5,
        );
        assert_eq!(m.get(&one), None);
        assert_eq!(m.get(&two), Some(&5));
        assert_eq!(m.get(&three), Some(&5));
        assert_eq!(m.get(&four), None);

        m.insert(
            Range {
                lower: Bound::Before(one.clone()),
                upper: Bound::After(two.clone()),
            },
            7,
        );

        assert_eq!(m.get(&one), Some(&7));
        assert_eq!(m.get(&two), Some(&7));
        assert_eq!(m.get(&three), Some(&5));
        assert_eq!(m.get(&four), None);

        m.insert(
            Range {
                lower: Bound::Before(three.clone()),
                upper: Bound::After(four.clone()),
            },
            15,
        );

        assert_eq!(m.get(&one), Some(&7));
        assert_eq!(m.get(&two), Some(&7));
        assert_eq!(m.get(&three), Some(&15));
        assert_eq!(m.get(&four), Some(&15));

        m.remove(&Range {
            lower: Bound::Before(two.clone()),
            upper: Bound::After(three.clone()),
        });

        assert_eq!(m.get(&one), Some(&7));
        assert_eq!(m.get(&two), None);
        assert_eq!(m.get(&three), None);
        assert_eq!(m.get(&four), Some(&15));
    }
}
