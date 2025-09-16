use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::iter::IntoIterator;

use crate::bound::Key;
use crate::util::binary_search_by_idx;
use crate::Bound;
use crate::Range;
use crate::RangeByLowerBound;
use crate::RangeSet;

#[derive(Clone)]
pub(crate) struct RangeMap<K, V> {
    // Ranges are always non-overlapping but may be adjacent.
    m: BTreeMap<RangeByLowerBound<K>, V>,
}

impl<K, V> RangeMap<K, V>
where
    K: Key,
{
    pub(crate) fn new() -> Self {
        Self { m: BTreeMap::new() }
    }

    pub(crate) fn insert(&mut self, range: Range<K>, value: V) {
        self.remove(&range);
        self.m.insert(RangeByLowerBound(range), value);
    }

    pub(crate) fn get(&self, k: &K) -> Option<&V> {
        if let Some((range, value)) = self.last_less_or_equal(&Bound::Before(k.clone())) {
            if range.contains(k) {
                return Some(value);
            }
        }
        None
    }

    pub(crate) fn remove(&mut self, range: &Range<K>) {
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

    pub(crate) fn intersecting_ranges<'a>(
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

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&Range<K>, &V)> {
        self.m.iter().map(|(range, value)| (&range.0, value))
    }

    fn ranges_range(
        &self,
        lower: std::ops::Bound<&Bound<K>>, // TODO(bw): this should probably be Bound<Bound<&K>>
        upper: std::ops::Bound<&Bound<K>>,
    ) -> impl Iterator<Item = (&Range<K>, &V)> + DoubleEndedIterator {
        self.m
            .range::<Bound<K>, (std::ops::Bound<&Bound<K>>, std::ops::Bound<&Bound<K>>)>((
                lower, upper,
            ))
            .map(|(range, value)| (range.borrow(), value))
    }

    fn last_less_or_equal(&self, bound: &Bound<K>) -> Option<(&Range<K>, &V)> {
        self.ranges_range(
            std::ops::Bound::Unbounded,
            std::ops::Bound::Included(&bound),
        )
        .next_back()
    }

    fn first_greater(&self, bound: &Bound<K>) -> Option<(&Range<K>, &V)> {
        self.ranges_range(
            std::ops::Bound::Excluded(&bound),
            std::ops::Bound::Unbounded,
        )
        .next()
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

pub(crate) struct RangeMapIntoIter<K, V> {
    inner: std::collections::btree_map::IntoIter<RangeByLowerBound<K>, V>,
}

impl<K, V> Iterator for RangeMapIntoIter<K, V> {
    type Item = (Range<K>, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| (k.0, v))
    }
}

pub(crate) fn intersect_in_ranges_by_key<'a, T: 'a, F: Fn(&'a T) -> Range<Vec<u8>>>(
    range: Range<&[u8]>,
    ranges: &'a [T],
    f: F,
) -> &'a [T] {
    let start_idx = match binary_search_by_idx(ranges.len(), range.to_vec().lower, |idx| {
        f(&ranges[idx]).upper
    }) {
        Ok(idx) => idx + 1,
        Err(idx) => idx,
    };

    let end_idx = binary_search_by_idx(ranges.len(), range.to_vec().upper, |idx| {
        f(&ranges[idx]).lower
    })
    .unwrap_or_else(core::convert::identity);

    &ranges[start_idx..end_idx]
}

#[cfg(test)]
mod tests {
    use super::intersect_in_ranges_by_key;
    use super::RangeMap;
    use crate::Bound;
    use crate::Range;

    #[test]
    fn test_intersect_in_ranges_by_key() {
        let ranges = [
            Range {
                lower: Bound::Before(&[0x00][..]),
                upper: Bound::After(&[0x00]),
            },
            Range {
                lower: Bound::After(&[0x00]),
                upper: Bound::After(&[0x01]),
            },
            Range {
                lower: Bound::Before(&[0x02]),
                upper: Bound::AfterPrefix(&[0x02]),
            },
        ];

        assert_eq!(
            intersect_in_ranges_by_key(Range::all(), &ranges[..], Range::to_vec),
            &ranges[..],
            "Range::all() overlaps everything",
        );

        assert_eq!(
            intersect_in_ranges_by_key(ranges[0], &ranges[..], Range::to_vec),
            &ranges[0..1],
            "range in list only overlaps itself",
        );
        assert_eq!(
            intersect_in_ranges_by_key(ranges[1], &ranges[..], Range::to_vec),
            &ranges[1..2],
            "range in list only overlaps itself",
        );
        assert_eq!(
            intersect_in_ranges_by_key(ranges[2], &ranges[..], Range::to_vec),
            &ranges[2..3],
            "range in list only overlaps itself",
        );

        assert!(
            intersect_in_ranges_by_key(
                Range {
                    lower: Bound::BeforeAll,
                    upper: Bound::Before(&[0x00]),
                },
                &ranges[..],
                Range::to_vec,
            )
            .is_empty(),
            "exact gap between ranges contains nothing",
        );
        assert!(
            intersect_in_ranges_by_key(
                Range {
                    lower: Bound::After(&[0x01]),
                    upper: Bound::Before(&[0x02]),
                },
                &ranges[..],
                Range::to_vec,
            )
            .is_empty(),
            "exact gap between ranges contains nothing",
        );
        assert!(
            intersect_in_ranges_by_key(
                Range {
                    lower: Bound::AfterPrefix(&[0x02]),
                    upper: Bound::AfterAll,
                },
                &ranges[..],
                Range::to_vec,
            )
            .is_empty(),
            "exact gap between ranges contains nothing",
        );

        assert!(
            intersect_in_ranges_by_key(
                Range {
                    lower: Bound::BeforeAll,
                    upper: Bound::Before(&[]),
                },
                &ranges[..],
                Range::to_vec,
            )
            .is_empty(),
            "within gap between ranges contains nothing",
        );
        assert!(
            intersect_in_ranges_by_key(
                Range {
                    lower: Bound::After(&[0x01, 0x01]),
                    upper: Bound::Before(&[0x01, 0x02]),
                },
                &ranges[..],
                Range::to_vec,
            )
            .is_empty(),
            "within gap between ranges contains nothing",
        );
        assert!(
            intersect_in_ranges_by_key(
                Range {
                    lower: Bound::After(&[0x03]),
                    upper: Bound::AfterAll,
                },
                &ranges[..],
                Range::to_vec,
            )
            .is_empty(),
            "within gap between ranges contains nothing",
        );

        assert_eq!(
            intersect_in_ranges_by_key(
                Range {
                    lower: Bound::After(&[0x00, 0x00]),
                    upper: Bound::After(&[0x02]),
                },
                &ranges[..],
                Range::to_vec,
            ),
            &ranges[1..],
        );
    }

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
