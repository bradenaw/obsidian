use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::iter::FromIterator;
use std::iter::IntoIterator;
use std::ops::Deref;

use anyhow::anyhow;

use crate::pb;
use crate::util::binary_search_by_idx;
use crate::util::hexlify;

pub(crate) trait Key: Deref<Target = [u8]> + Clone + Eq + Ord {}

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

#[derive(Clone)]
struct RangeByLowerBound<K>(Range<K>);
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
    use proptest::prelude::*;

    use super::intersect_in_ranges_by_key;
    use super::Bound;
    use super::KeyOrBound;
    use super::Range;
    use super::RangeMap;
    use super::RangeSet;

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

    #[test]
    fn test_range_contains() {
        let empty: Vec<u8> = vec![];
        assert!(!Range::<&[u8]> {
            lower: Bound::BeforeAll,
            upper: Bound::BeforeAll,
        }
        .contains(&empty));
    }

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
