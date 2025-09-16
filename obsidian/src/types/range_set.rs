use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::iter::FromIterator;
use std::iter::IntoIterator;
use std::ops::Deref;

use crate::types::bound::Key;
use crate::types::Bound;
use crate::types::Range;

#[derive(Clone)]
pub struct RangeSet<K> {
    ranges: BTreeSet<RangeByLowerBound<K>>,
}

impl<K: Key> RangeSet<K> {
    pub fn new() -> Self {
        Self {
            ranges: BTreeSet::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn contiguous(&self) -> Option<Range<K>> {
        if self.ranges.len() == 0 {
            Some(Range::empty())
        } else if self.ranges.len() == 1 {
            self.ranges.iter().map(|r| r.0.clone()).next()
        } else {
            None
        }
    }

    pub fn contains(&self, k: &K) -> bool {
        match self.last_less_or_equal(&Bound::Before(k.clone())) {
            Some(range) => range.contains(k),
            None => false,
        }
    }

    pub fn split(self, bound: Bound<K>) -> (RangeSet<K>, RangeSet<K>) {
        let mut below = vec![];
        let mut above = vec![];
        for range in self.into_iter() {
            if bound >= range.upper {
                below.push(range);
            } else if bound <= range.lower {
                above.push(range);
            } else if bound > range.lower && bound < range.upper {
                let (range_lower, range_upper) = range.split(&bound);
                below.push(range_lower);
                above.push(range_upper);
            }
        }

        (
            RangeSet::from_iter(below.into_iter()),
            RangeSet::from_iter(above.into_iter()),
        )
    }

    pub fn intersects(&self, other: &RangeSet<K>) -> bool {
        self.intersections(other).next().is_some()
    }

    pub fn union(&self, other: &RangeSet<K>) -> Self {
        let mut union = RangeSet::new();
        for range in self.iter() {
            union.add_range(range.clone());
        }
        for range in other.iter() {
            union.add_range(range.clone());
        }
        union
    }

    pub fn intersection(&self, other: &RangeSet<K>) -> Self {
        Self {
            ranges: self
                .intersections(other)
                .map(|range| RangeByLowerBound(range))
                .collect(),
        }
    }

    pub fn difference(&self, other: &RangeSet<K>) -> Self {
        let mut difference = self.clone();
        for range in other.iter() {
            difference.subtract_range(range.clone());
        }
        difference
    }

    pub fn iter(&self) -> impl Iterator<Item = &Range<K>> + '_ {
        self.ranges.iter().map(|range| range.borrow())
    }

    pub fn into_iter(self) -> impl Iterator<Item = Range<K>> {
        self.ranges.into_iter().map(|range| range.into())
    }

    pub fn first(&self) -> Option<&Range<K>> {
        self.ranges.first().map(|range| &range.0)
    }

    pub fn last(&self) -> Option<&Range<K>> {
        self.ranges.last().map(|range| &range.0)
    }

    fn intersections<'a>(&'a self, other: &'a RangeSet<K>) -> impl Iterator<Item = Range<K>> + 'a {
        Intersections::new(self, other)
    }

    fn ranges_range(
        &self,
        lower: std::ops::Bound<&Bound<K>>, // TODO(bw): this should probably be Bound<Bound<&K>>
        upper: std::ops::Bound<&Bound<K>>,
    ) -> impl Iterator<Item = &Range<K>> + DoubleEndedIterator {
        self.ranges
            .range::<Bound<K>, (std::ops::Bound<&Bound<K>>, std::ops::Bound<&Bound<K>>)>((
                lower, upper,
            ))
            .map(|range| range.borrow())
    }

    fn last_less_or_equal(&self, bound: &Bound<K>) -> Option<&Range<K>> {
        self.ranges_range(
            std::ops::Bound::Unbounded,
            std::ops::Bound::Included(&bound),
        )
        .next_back()
    }

    fn first_greater(&self, bound: &Bound<K>) -> Option<&Range<K>> {
        self.ranges_range(
            std::ops::Bound::Excluded(&bound),
            std::ops::Bound::Unbounded,
        )
        .next()
    }

    fn overlapping_ranges<'a>(&'a self, range: &'a Range<K>) -> Vec<Range<K>> {
        let mut result: Vec<Range<K>> = Vec::new();
        match self
            .ranges_range(
                std::ops::Bound::Unbounded,
                std::ops::Bound::Included(&range.lower),
            )
            .next_back()
        {
            Some(next_below) => {
                if !range.intersection(next_below).is_empty() || range.adjacent(next_below) {
                    result.push(next_below.clone().into());
                }
            }
            None => {}
        };
        for overlapping_range in self.ranges_range(
            std::ops::Bound::Included(&range.lower),
            std::ops::Bound::Included(&range.upper),
        ) {
            result.push(overlapping_range.clone().into());
        }
        result
    }

    pub fn add_range(&mut self, mut range: Range<K>) {
        if range.is_empty() {
            return;
        }
        for overlapping_range in self.overlapping_ranges(&range) {
            range.lower = std::cmp::min(range.lower, overlapping_range.lower.clone());
            range.upper = std::cmp::max(range.upper, overlapping_range.upper.clone());
            self.ranges.remove(&overlapping_range.lower);
        }
        self.ranges.insert(RangeByLowerBound(range));
    }

    pub fn subtract_range(&mut self, range: Range<K>) {
        if range.is_empty() {
            return;
        }
        let overlapping_ranges = self.overlapping_ranges(&range);
        let prev = match overlapping_ranges.get(0) {
            Some(lowest) => Range {
                lower: lowest.lower.clone(),
                upper: range.lower,
            },
            None => Range::empty(),
        };
        let next = match overlapping_ranges.last() {
            Some(highest) => Range {
                lower: range.upper,
                upper: highest.upper.clone(),
            },
            None => Range::empty(),
        };

        for overlapping_range in overlapping_ranges {
            self.ranges.remove(&overlapping_range.lower);
        }
        if !prev.is_empty() {
            self.ranges.insert(RangeByLowerBound(prev));
        }
        if !next.is_empty() {
            self.ranges.insert(RangeByLowerBound(next));
        }
    }
}

impl<K: Key> From<Range<K>> for RangeSet<K> {
    fn from(range: Range<K>) -> RangeSet<K> {
        let mut result = RangeSet::new();
        if !range.is_empty() {
            result.add_range(range);
        }
        result
    }
}

impl<K> FromIterator<Range<K>> for RangeSet<K>
where
    K: Key,
{
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Range<K>>,
    {
        let mut result = RangeSet::new();
        for range in iter {
            result.add_range(range);
        }
        result
    }
}

impl<K: Deref<Target = [u8]>> Debug for RangeSet<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("{")?;
        for (i, range) in self.ranges.iter().enumerate() {
            if i != 0 {
                f.write_str(", ")?;
            }
            range.fmt(f)?;
        }
        f.write_str("}")?;
        Ok(())
    }
}

struct Intersections<'a, K: Key> {
    a: &'a RangeSet<K>,
    a_cursor: Option<&'a Range<K>>,
    b: &'a RangeSet<K>,
    b_cursor: Option<&'a Range<K>>,
}

impl<'a, K: Key> Intersections<'a, K> {
    fn new(a: &'a RangeSet<K>, b: &'a RangeSet<K>) -> Self {
        Self {
            a,
            a_cursor: a.iter().next(),
            b,
            b_cursor: b.iter().next(),
        }
    }
}

impl<'a, K: Key> Iterator for Intersections<'a, K> {
    type Item = Range<K>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match (self.a_cursor, self.b_cursor) {
                (Some(a_range), Some(b_range)) => {
                    let intersection = a_range.intersection(b_range);

                    if a_range.upper <= b_range.upper {
                        self.a_cursor = self.a.first_greater(&a_range.lower);
                        if let Some(a_cursor) = self.a_cursor {
                            self.b_cursor = match self.b.last_less_or_equal(&a_cursor.lower) {
                                new_b_cursor @ Some(_) => new_b_cursor,
                                None => self.b_cursor,
                            };
                        }
                    } else {
                        self.b_cursor = self.b.first_greater(&b_range.lower);
                        if let Some(b_cursor) = self.b_cursor {
                            self.a_cursor = match self.a.last_less_or_equal(&b_cursor.lower) {
                                new_a_cursor @ Some(_) => new_a_cursor,
                                None => self.a_cursor,
                            };
                        }
                    }

                    if !intersection.is_empty() {
                        return Some(intersection);
                    }
                }
                (None, _) | (_, None) => return None,
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct RangeByLowerBound<K>(pub Range<K>);
impl<K: Key> From<RangeByLowerBound<K>> for Range<K> {
    fn from(r: RangeByLowerBound<K>) -> Self {
        r.0
    }
}
impl<K: Key> Eq for RangeByLowerBound<K> {}
impl<K: Key> PartialEq for RangeByLowerBound<K> {
    fn eq(&self, other: &RangeByLowerBound<K>) -> bool {
        self.0.lower == other.0.lower
    }
}
impl<K: Key> Ord for RangeByLowerBound<K> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.lower.cmp(&other.0.lower)
    }
}
impl<K: Key> PartialOrd for RangeByLowerBound<K> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<K: Key> Borrow<Bound<K>> for RangeByLowerBound<K> {
    fn borrow(&self) -> &Bound<K> {
        &self.0.lower
    }
}
impl<K: Key> Borrow<Range<K>> for RangeByLowerBound<K> {
    fn borrow(&self) -> &Range<K> {
        &self.0
    }
}
impl<K: Deref<Target = [u8]>> Debug for RangeByLowerBound<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::RangeSet;
    use crate::types::Bound;
    use crate::types::Range;

    fn range_set_intersection(
        a_ranges: Vec<Range<Vec<u8>>>,
        b_ranges: Vec<Range<Vec<u8>>>,
        points: Vec<Vec<u8>>,
    ) {
        let a: RangeSet<_> = a_ranges.iter().cloned().collect();
        let b: RangeSet<_> = b_ranges.iter().cloned().collect();

        let a_intersect_b = a.intersection(&b);

        for point in points {
            let a_intersect_b_contains = a_intersect_b.contains(&point);
            let a_contains = a_ranges.iter().any(|range| range.contains(&point));
            let b_contains = b_ranges.iter().any(|range| range.contains(&point));
            assert_eq!(
                a_intersect_b_contains,
                a_contains && b_contains,
                "a_ranges = {:?}\na = {:?}\nb_ranges = {:?}\nb = {:?}\na_intersect_b = {:?}\npoint = {:?}\na_contains = {:?}\nb_contains = {:?}\na_intersect_b_contains = {:?}",
                a_ranges,
                a,
                b_ranges,
                b,
                a_intersect_b,
                point,
                a_contains,
                b_contains,
                a_intersect_b_contains,
            );
        }
    }

    #[test]
    fn test_range_set_intersection() {
        range_set_intersection(
            vec![
                Range {
                    lower: Bound::BeforeAll,
                    upper: Bound::Before(vec![]),
                },
                Range {
                    lower: Bound::After(vec![]),
                    upper: Bound::Before(vec![11]),
                },
            ],
            vec![Range {
                lower: Bound::After(vec![0]),
                upper: Bound::Before(vec![11]),
            }],
            vec![vec![1]],
        );
    }

    #[test]
    fn test_range_set_first_greater_last_less_or_equal() {
        let a = Range {
            lower: Bound::BeforeAll,
            upper: Bound::Before(vec![]),
        };
        let b = Range {
            lower: Bound::After(vec![]),
            upper: Bound::Before(vec![11]),
        };
        let rs1: RangeSet<_> = vec![a.clone(), b.clone()].into_iter().collect();
        let c = Range {
            lower: Bound::After(vec![0]),
            upper: Bound::Before(vec![11]),
        };
        let rs2: RangeSet<_> = vec![c.clone()].into_iter().collect();

        assert_eq!(
            rs1.first_greater(&Bound::BeforeAll).cloned(),
            Some(b.clone())
        );
        assert_eq!(
            rs1.first_greater(&Bound::Before(vec![])).cloned(),
            Some(b.clone())
        );
        assert_eq!(
            rs2.first_greater(&Bound::BeforeAll).cloned(),
            Some(c.clone())
        );
        assert_eq!(
            rs2.first_greater(&Bound::After(vec![])).cloned(),
            Some(c.clone())
        );

        assert_eq!(
            rs1.last_less_or_equal(&Bound::BeforeAll).cloned(),
            Some(a.clone())
        );
        assert_eq!(
            rs1.last_less_or_equal(&Bound::Before(vec![])).cloned(),
            Some(a)
        );
        assert_eq!(
            rs1.last_less_or_equal(&Bound::After(vec![])).cloned(),
            Some(b.clone())
        );
        assert_eq!(rs1.last_less_or_equal(&Bound::AfterAll).cloned(), Some(b));
        assert_eq!(rs2.last_less_or_equal(&Bound::After(vec![])).cloned(), None);
        assert_eq!(
            rs2.last_less_or_equal(&Bound::After(vec![0])).cloned(),
            Some(c.clone())
        );
        assert_eq!(rs2.last_less_or_equal(&Bound::AfterAll).cloned(), Some(c));
    }

    fn simple_key() -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(any::<u8>(), 0..2)
    }

    fn simple_bound() -> impl Strategy<Value = Bound<Vec<u8>>> {
        prop_oneof![
            Just(Bound::BeforeAll),
            simple_key().prop_map(Bound::Before),
            simple_key().prop_map(Bound::After),
            simple_key().prop_map(Bound::AfterPrefix),
            Just(Bound::AfterAll),
        ]
    }

    fn simple_range() -> impl Strategy<Value = Range<Vec<u8>>> {
        (simple_bound(), simple_bound()).prop_map(|(lower, upper)| Range { lower, upper })
    }

    proptest! {
        #[test]
        fn proptest_range_set_build(
            ranges in proptest::collection::vec(simple_range(), 1..100),
            points in proptest::collection::vec(simple_key(), 1..100),
        ) {
            let range_set: RangeSet<_> = ranges.iter().cloned().collect();
            for window in range_set.ranges.iter().collect::<Vec<_>>().windows(2) {
                if let [a, b] = *window {
                    // Make sure they're non-adjacent, non-intersecting.
                    assert!(a.0.upper < b.0.lower);
                }
            }
            for point in points {
                assert_eq!(
                    range_set.contains(&point),
                    ranges.iter().any(|range| range.contains(&point)),
                );
            }
        }

        #[test]
        fn proptest_range_set_union(
            a_ranges in proptest::collection::vec(simple_range(), 1..100),
            b_ranges in proptest::collection::vec(simple_range(), 1..100),
            points in proptest::collection::vec(simple_key(), 1..100),
        ) {
            let a: RangeSet<_> = a_ranges.iter().cloned().collect();
            let b: RangeSet<_> = b_ranges.iter().cloned().collect();

            let a_union_b = a.union(&b);

            for point in points {
                assert_eq!(
                    a_union_b.contains(&point),
                    a_ranges.iter().any(|range| range.contains(&point))
                        || b_ranges.iter().any(|range|range.contains(&point)),
                );
            }
        }

        #[test]
        fn proptest_range_set_intersection(
            a_ranges in proptest::collection::vec(simple_range(), 1..100),
            b_ranges in proptest::collection::vec(simple_range(), 1..100),
            points in proptest::collection::vec(simple_key(), 1..100),
        ) {
            range_set_intersection(a_ranges, b_ranges, points);
        }

        #[test]
        fn proptest_range_set_difference(
            a_ranges in proptest::collection::vec(simple_range(), 1..100),
            b_ranges in proptest::collection::vec(simple_range(), 1..100),
            points in proptest::collection::vec(simple_key(), 1..100),
        ) {
            let a: RangeSet<_> = a_ranges.iter().cloned().collect();
            let b: RangeSet<_> = b_ranges.iter().cloned().collect();

            let a_difference_b = a.difference(&b);

            for point in points {
                assert_eq!(
                    a_difference_b.contains(&point),
                    a_ranges.iter().any(|range| range.contains(&point))
                        && !b_ranges.iter().any(|range|range.contains(&point)),
                );
            }
        }
    }
}
