use std::cmp;
use std::collections::BTreeMap;

use crate::lsm::util::LsmRevision;
use crate::lsm::RunId;
use crate::range::Bound;
use crate::range::Range;
use crate::types::Direction;
use crate::types::HistoryRange;
use crate::types::RevisionValue;
use crate::types::Timestamp;
use crate::util::hexlify;
use crate::util::IteratorEither;
use crate::wal;

impl Default for Memtable {
    fn default() -> Self {
        Memtable::new()
    }
}

pub(crate) struct Memtable {
    id: RunId,
    size: u64,
    max_seqno: wal::SeqNo,
    kvs: BTreeMap<Vec<u8>, BTreeMap<Timestamp, RevisionValue>>,
    max_key_len: usize,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            id: RunId::new(),
            size: 0,
            kvs: BTreeMap::new(),
            max_key_len: 0,
            max_seqno: wal::SeqNo(0),
        }
    }

    pub fn id(&self) -> RunId {
        self.id
    }

    pub fn max_seqno(&self) -> wal::SeqNo {
        self.max_seqno
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn get(&self, ts: Timestamp, k: &[u8]) -> Option<(Timestamp, RevisionValue)> {
        let (revision_ts, revision_v) = self.kvs.get(k)?.range(Timestamp::ZERO..=ts).next_back()?;
        Some((*revision_ts, revision_v.clone()))
    }

    pub fn insert(
        &mut self,
        seqno: wal::SeqNo,
        k: Vec<u8>,
        ts: Timestamp,
        v: RevisionValue,
    ) -> u64 {
        log::trace!("memtable {}: insert {}@{}", self.id, hexlify(&k[..]), ts);
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
    ) -> impl Iterator<Item = LsmRevision> + Send + '_ {
        match range.to_std_ops_bounds(self.max_key_len) {
            Some(range_bounds) => {
                let iter = self
                    .kvs
                    .range(range_bounds)
                    .filter_map(move |(key, versions)| {
                        let (revision_ts, value) =
                            versions.range(Timestamp::ZERO..=ts).next_back()?;
                        Some(LsmRevision {
                            key: key.clone(),
                            ts: *revision_ts,
                            value: value.clone(),
                        })
                    });

                match direction {
                    Direction::Asc => IteratorEither::Left(IteratorEither::Left(iter)),
                    Direction::Desc => IteratorEither::Left(IteratorEither::Right(iter.rev())),
                }
            }
            None => IteratorEither::Right(std::iter::empty()),
        }
    }

    pub fn history(
        &self,
        key: &[u8],
        range: HistoryRange,
        direction: Direction,
    ) -> impl Iterator<Item = (Timestamp, RevisionValue)> + Send + '_ {
        let versions = match self.kvs.get(key) {
            Some(versions) => versions,
            None => return IteratorEither::Right(std::iter::empty()),
        };

        let (min, max) = range.as_min_max();

        let in_range = versions
            .range(min..=max)
            .map(|(ts, value)| (*ts, value.clone()));
        match direction {
            Direction::Asc => IteratorEither::Left(IteratorEither::Left(in_range)),
            Direction::Desc => IteratorEither::Left(IteratorEither::Right(in_range.rev())),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = LsmRevision> + '_ {
        self.kvs
            .iter()
            .map(|(key, entries)| {
                entries
                    .into_iter()
                    .rev()
                    .map(move |(ts, value)| LsmRevision {
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
                    LsmRevision {
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
