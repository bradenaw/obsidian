use std::collections::BTreeMap;
use std::sync::RwLock;

use gen_iter::gen_iter;

use crate::Bound;
use crate::Range;
use crate::Record;
use crate::Value;

pub(crate) enum PutError {
    Sealed,
}

pub(crate) struct Memtable {
    // TODO: This should probably be something like crossbeam-skiplist, but this'll do for now.
    inner: RwLock<MemtableInner>,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(MemtableInner::new()),
        }
    }

    pub fn get(&self, ts: u64, k: &[u8]) -> Option<(u64, Value)> {
        self.inner.read().unwrap().get(ts, k)
    }

    pub fn scan_asc<'a>(
        &'a self,
        ts: u64,
        range: Range<&'a [u8]>,
    ) -> impl Iterator<Item = Record> + 'a {
        // TODO: This is an absolutely whack implementation, but I can't wrap my head around any
        // way to properly express mucking with an iterator that contains a borrow to inner through
        // a RwLockReadGuard. The guard only gives you the whole thing, and to make an iterator
        // that contains both the guard and the iterator over the thing inside it is a
        // self-referential struct.
        gen_iter!(move {
            let mut cursor = range.to_vec();
            loop {
                let record = match self.inner.read().unwrap().scan_asc(ts, cursor.borrow()).next() {
                    Some(record) => record,
                    None => return,
                };
                cursor.lower = Bound::After(record.key.clone());
                yield record;
            }
        })
    }

    pub fn range(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.inner.read().unwrap().range()
    }

    pub fn put(&self, k: Vec<u8>, ts: u64, v: Vec<u8>) -> Result<usize, PutError> {
        let mut inner = self.inner.write().unwrap();
        if inner.sealed {
            return Err(PutError::Sealed);
        }
        inner.put(k, ts, v);
        Ok(inner.size())
    }

    pub fn try_seal(&self) -> bool {
        let mut inner = self.inner.write().unwrap();
        if inner.sealed {
            return false;
        }
        inner.sealed = true;
        true
    }

    pub fn iter(&self) -> impl Iterator<Item = (Vec<u8>, u64, Value)> + '_ {
        self.inner.read().unwrap().iter()
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Memtable::new()
    }
}

struct MemtableInner {
    size: usize,
    kvs: BTreeMap<Vec<u8>, BTreeMap<u64, Value>>,
    max_key_len: usize,
    sealed: bool,
}

impl MemtableInner {
    fn new() -> Self {
        Self {
            size: 0,
            kvs: BTreeMap::new(),
            max_key_len: 0,
            sealed: false,
        }
    }

    fn size(&self) -> usize {
        self.size
    }

    fn get(&self, ts: u64, k: &[u8]) -> Option<(u64, Value)> {
        let (record_ts, record_v) = self.kvs.get(k)?.range(0..=ts).next_back()?;
        Some((*record_ts, record_v.clone()))
    }

    fn put(&mut self, k: Vec<u8>, ts: u64, v: Vec<u8>) {
        self.size += k.len() + v.len() + 8;
        self.max_key_len = std::cmp::max(k.len(), self.max_key_len);
        self.kvs
            .entry(k)
            .or_insert(BTreeMap::default())
            .insert(ts, Value::Regular(v));
    }

    fn range(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        Some((
            self.kvs.iter().next()?.0.clone(),
            self.kvs.iter().next_back()?.0.clone(),
        ))
    }

    fn scan_asc(&self, ts: u64, range: Range<&[u8]>) -> impl Iterator<Item = Record> + '_ {
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
                    return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Record>>
                }
            },
            match range.upper {
                Bound::BeforeAll => {
                    return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Record>>
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
                return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Record>>;
            }
            (
                std::ops::Bound::Included(s) | std::ops::Bound::Excluded(s),
                std::ops::Bound::Included(e) | std::ops::Bound::Excluded(e),
            ) if s > e => {
                return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Record>>;
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
        ) as Box<dyn Iterator<Item = Record>>
    }

    fn iter(&self) -> impl Iterator<Item = (Vec<u8>, u64, Value)> + '_ {
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
