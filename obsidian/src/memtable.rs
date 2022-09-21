use std::collections::BTreeMap;

use uuid::Uuid;

use crate::Bound;
use crate::Range;
use crate::Record;
use crate::Value;

impl Default for Memtable {
    fn default() -> Self {
        Memtable::new()
    }
}

pub(crate) struct Memtable {
    id: Uuid,
    size: u64,
    kvs: BTreeMap<Vec<u8>, BTreeMap<u64, Value>>,
    max_key_len: usize,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            size: 0,
            kvs: BTreeMap::new(),
            max_key_len: 0,
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn get(&self, ts: u64, k: &[u8]) -> Option<(u64, Value)> {
        let (record_ts, record_v) = self.kvs.get(k)?.range(0..=ts).next_back()?;
        Some((*record_ts, record_v.clone()))
    }

    pub fn put(&mut self, k: Vec<u8>, ts: u64, v: Vec<u8>) -> u64 {
        self.size += (k.len() + v.len() + 8) as u64;
        self.max_key_len = std::cmp::max(k.len(), self.max_key_len);
        self.kvs
            .entry(k)
            .or_insert(BTreeMap::default())
            .insert(ts, Value::Regular(v));
        self.size
    }

    pub fn range(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        Some((
            self.kvs.iter().next()?.0.clone(),
            self.kvs.iter().next_back()?.0.clone(),
        ))
    }

    pub fn scan_asc(
        &self,
        ts: u64,
        range: Range<&[u8]>,
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
                    return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Record> + Send>
                }
            },
            match range.upper {
                Bound::BeforeAll => {
                    return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Record> + Send>
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
                return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Record> + Send>;
            }
            (
                std::ops::Bound::Included(s) | std::ops::Bound::Excluded(s),
                std::ops::Bound::Included(e) | std::ops::Bound::Excluded(e),
            ) if s > e => {
                return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Record> + Send>;
            }
            _ => {}
        }

        Box::new(
            self.kvs
                .range(range_bounds)
                .filter_map(move |(key, versions)| {
                    let (record_ts, value) = versions.range(0..=ts).next_back()?;
                    Some(Record {
                        key: key.clone(),
                        ts: *record_ts,
                        value: value.clone(),
                    })
                }),
        ) as Box<dyn Iterator<Item = Record> + Send>
    }

    pub fn iter(&self) -> impl Iterator<Item = (Vec<u8>, u64, Value)> + '_ {
        self.kvs
            .iter()
            .map(|(key, entries)| {
                entries
                    .into_iter()
                    .rev()
                    .map(move |(ts, value)| (key.clone(), *ts, value.clone()))
            })
            .flatten()
    }
}
