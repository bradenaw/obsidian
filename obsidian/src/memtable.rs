use std::cmp;
use std::collections::BTreeMap;

use uuid::Uuid;

use crate::range::Bound;
use crate::range::Range;
use crate::types::Direction;
use crate::types::Record;
use crate::types::Timestamp;
use crate::types::Value;
use crate::util::IteratorEither;
use crate::wal;

impl Default for Memtable {
    fn default() -> Self {
        Memtable::new()
    }
}

pub(crate) struct Memtable {
    id: Uuid,
    size: u64,
    max_seqno: wal::SeqNo,
    kvs: BTreeMap<Vec<u8>, BTreeMap<Timestamp, Value>>,
    max_key_len: usize,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            size: 0,
            kvs: BTreeMap::new(),
            max_key_len: 0,
            max_seqno: wal::SeqNo(0),
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn max_seqno(&self) -> wal::SeqNo {
        self.max_seqno
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn get(&self, ts: Timestamp, k: &[u8]) -> Option<(Timestamp, Value)> {
        let (record_ts, record_v) = self.kvs.get(k)?.range(Timestamp::ZERO..=ts).next_back()?;
        Some((*record_ts, record_v.clone()))
    }

    pub fn insert(&mut self, seqno: wal::SeqNo, k: Vec<u8>, ts: Timestamp, v: Value) -> u64 {
        self.size += (k.len() + v.len() + 8) as u64;
        self.max_key_len = std::cmp::max(k.len(), self.max_key_len);
        self.kvs
            .entry(k)
            .or_insert(BTreeMap::default())
            .insert(ts, v);
        self.max_seqno = cmp::max(self.max_seqno, seqno);
        self.size
    }

    pub fn range(&self) -> Range<Vec<u8>> {
        match (self.kvs.first_key_value(), self.kvs.last_key_value()) {
            (Some((min_key, _)), Some((max_key, _))) => Range {
                lower: Bound::Before(min_key.clone()),
                upper: Bound::After(max_key.clone()),
            },
            _ => Range::empty(),
        }
    }

    pub fn scan(
        &self,
        ts: Timestamp,
        range: Range<&[u8]>,
        direction: Direction,
    ) -> impl Iterator<Item = Record> + Send + '_ {
        let range_bounds = (
            match range.lower {
                Bound::BeforeAll => std::ops::Bound::Unbounded,
                Bound::Before(k) => std::ops::Bound::Included(k.to_vec()),
                Bound::After(k) => std::ops::Bound::Excluded(k.to_vec()),
                Bound::AfterPrefix(k) => std::ops::Bound::Excluded(
                    k.iter()
                        .cloned()
                        .chain((0..self.max_key_len.saturating_sub(k.len())).map(|_| 0xFFu8))
                        .collect(),
                ),
                Bound::AfterAll => {
                    return IteratorEither::Right(std::iter::empty());
                }
            },
            match range.upper {
                Bound::BeforeAll => {
                    return IteratorEither::Right(std::iter::empty());
                }
                Bound::Before(k) => std::ops::Bound::Excluded(k.to_vec()),
                Bound::After(k) => std::ops::Bound::Included(k.to_vec()),
                Bound::AfterPrefix(k) => std::ops::Bound::Excluded(
                    k.iter()
                        .cloned()
                        .chain((0..self.max_key_len.saturating_sub(k.len())).map(|_| 0xFFu8))
                        .collect(),
                ),
                Bound::AfterAll => std::ops::Bound::Unbounded,
            },
        );

        // BTreeMap panics in these situations because they're nonsense, but we only produce them
        // when the range is in fact empty.
        match (&range_bounds.0, &range_bounds.1) {
            (std::ops::Bound::Excluded(s), std::ops::Bound::Excluded(e)) if s == e => {
                return IteratorEither::Right(std::iter::empty());
            }
            (
                std::ops::Bound::Included(s) | std::ops::Bound::Excluded(s),
                std::ops::Bound::Included(e) | std::ops::Bound::Excluded(e),
            ) if s > e => {
                return IteratorEither::Right(std::iter::empty());
            }
            _ => {}
        }

        let iter = self
            .kvs
            .range(range_bounds)
            .filter_map(move |(key, versions)| {
                let (record_ts, value) = versions.range(Timestamp::ZERO..=ts).next_back()?;
                Some(Record {
                    key: key.clone(),
                    ts: *record_ts,
                    value: value.clone(),
                })
            });

        match direction {
            Direction::Asc => IteratorEither::Left(IteratorEither::Left(iter)),
            Direction::Desc => IteratorEither::Left(IteratorEither::Right(iter.rev())),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = Record> + '_ {
        self.kvs
            .iter()
            .map(|(key, entries)| {
                entries.into_iter().rev().map(move |(ts, value)| Record {
                    key: key.clone(),
                    ts: *ts,
                    value: value.clone(),
                })
            })
            .flatten()
    }

    pub(crate) fn dump(&self) {
        println!("=== memtable ===");
        for (key, versions) in &self.kvs {
            for (ts, value) in versions {
                println!(
                    "  {:?}",
                    Record {
                        key: key.clone(),
                        ts: *ts,
                        value: value.clone()
                    }
                );
            }
        }
        println!("================");
    }
}
