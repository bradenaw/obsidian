use std::cmp;
use std::sync::RwLock;

use crossbeam_skiplist::SkipMap;

use crate::lsm::util::LsmRevision;
use crate::lsm::RunId;
use crate::runtime::WalSeq;
use crate::util::hexlify;
use crate::util::IteratorEither;
use crate::Bound;
use crate::Direction;
use crate::HistoryRange;
use crate::Range;
use crate::RevisionValue;
use crate::Timestamp;

impl Default for Memtable {
    fn default() -> Self {
        Memtable::new()
    }
}

pub(crate) struct Memtable {
    id: RunId,
    kvs: SkipMap<Vec<u8>, SkipMap<Timestamp, RevisionValue>>,
    stats: RwLock<MemtableStats>,
}

struct MemtableStats {
    size: u64,
    max_seqno: WalSeq,
    max_key_len: usize,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            id: RunId::new(),
            kvs: SkipMap::new(),
            stats: RwLock::new(MemtableStats {
                size: 0,
                max_key_len: 0,
                max_seqno: WalSeq(0),
            }),
        }
    }

    pub fn id(&self) -> RunId {
        self.id
    }

    pub fn max_seqno(&self) -> WalSeq {
        self.stats.read().unwrap().max_seqno
    }

    pub fn size(&self) -> u64 {
        self.stats.read().unwrap().size
    }

    pub fn is_empty(&self) -> bool {
        self.kvs.len() == 0
    }

    pub fn get(&self, ts: Timestamp, k: &[u8]) -> Option<(Timestamp, RevisionValue)> {
        let revisions = self.kvs.get(k)?;
        let entry = revisions.value().range(Timestamp::ZERO..=ts).next_back()?;
        let revision_ts = entry.key();
        let revision_v = entry.value();
        Some((*revision_ts, revision_v.clone()))
    }

    pub fn insert(&self, seqno: WalSeq, k: Vec<u8>, ts: Timestamp, v: RevisionValue) -> u64 {
        log::trace!("memtable {}: insert {}@{}", self.id, hexlify(&k[..]), ts);

        let mut stats = self.stats.write().unwrap();

        let key_len = k.len();
        let value_len = v.len();
        let (entry, key_cost) = match self.kvs.get(&k) {
            // If the key is already present we don't store it again, so don't count it as part of
            // the size.
            Some(entry) => (entry, 0),
            None => (self.kvs.insert(k, SkipMap::new()), key_len),
        };
        entry.value().insert(ts, v);

        stats.size += (key_cost + value_len + 8) as u64;
        stats.max_key_len = std::cmp::max(key_len, stats.max_key_len);
        stats.max_seqno = cmp::max(stats.max_seqno, seqno);

        stats.size
    }

    pub fn range(&self) -> Range<Vec<u8>> {
        match (self.kvs.front(), self.kvs.back()) {
            (Some(front_entry), Some(back_entry)) => Range {
                lower: Bound::Before(front_entry.key().clone()),
                upper: Bound::After(back_entry.key().clone()),
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
        // TODO: Atomic this so we don't need to lock.
        let max_key_len = self.stats.read().unwrap().max_key_len;
        match range.to_std_ops_bounds(max_key_len) {
            Some(range_bounds) => {
                let iter = self
                    .kvs
                    .range(range_bounds)
                    .filter_map(move |revisions_entry| {
                        let revision_entry = revisions_entry
                            .value()
                            .range(Timestamp::ZERO..=ts)
                            .next_back()?;
                        Some(LsmRevision {
                            key: revisions_entry.key().clone(),
                            ts: *revision_entry.key(),
                            value: revision_entry.value().clone(),
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
        let entry = match self.kvs.get(key) {
            Some(entry) => entry,
            None => return IteratorEither::Right(std::iter::empty()),
        };

        let (min, max) = range.as_min_max();

        return match direction {
            Direction::Asc => IteratorEither::Left(IteratorEither::Left(HistoryAscIterator {
                entry,
                cursor: min,
                max,
            })),
            Direction::Desc => IteratorEither::Left(IteratorEither::Right(HistoryDescIterator {
                entry,
                cursor: Some(max),
                min,
            })),
        };
    }

    pub fn iter(&self) -> impl Iterator<Item = LsmRevision> + '_ {
        self.kvs
            .iter()
            .map(|entry| {
                let key = entry.key().clone();

                self.history(&key, HistoryRange::All, Direction::Desc)
                    .map(move |(ts, value)| LsmRevision {
                        key: key.clone(),
                        ts,
                        value,
                    })
            })
            .flatten()
    }

    pub(crate) fn dump(&self) {
        println!("=== memtable ===");
        for revision in self.iter() {
            println!("  {:?}", revision);
        }
        println!("================");
    }
}

struct HistoryAscIterator<'a> {
    // This awkwardness is needed because the entry borrows out of the map, and an iterator over
    // the value here would borrow out of the entry, which would mean a self-borrow.
    //
    // It means that iteration here needs to keep searching every next() rather than just
    // traversing.
    entry: crossbeam_skiplist::map::Entry<'a, Vec<u8>, SkipMap<Timestamp, RevisionValue>>,
    cursor: Timestamp,
    max: Timestamp,
}

impl<'a> Iterator for HistoryAscIterator<'a> {
    type Item = (Timestamp, RevisionValue);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(entry) = self
            .entry
            .value()
            .lower_bound(std::ops::Bound::Included(&self.cursor))
        {
            let ts = *entry.key();
            let value = entry.value().clone();

            self.cursor = ts.plus_one();

            if ts > self.max {
                return None;
            }

            return Some((ts, value));
        }

        None
    }
}

struct HistoryDescIterator<'a> {
    entry: crossbeam_skiplist::map::Entry<'a, Vec<u8>, SkipMap<Timestamp, RevisionValue>>,
    cursor: Option<Timestamp>,
    min: Timestamp,
}

impl<'a> Iterator for HistoryDescIterator<'a> {
    type Item = (Timestamp, RevisionValue);

    fn next(&mut self) -> Option<Self::Item> {
        let cursor = self.cursor?;

        if let Some(entry) = self
            .entry
            .value()
            .upper_bound(std::ops::Bound::Included(&cursor))
        {
            let ts = *entry.key();
            if ts < self.min {
                return None;
            }

            // A thing we actually encounter because TxOutcomes are written at timestamp 0.
            if ts == Timestamp::ZERO {
                self.cursor = None;
            } else {
                self.cursor = Some(ts.minus_one());
            }

            let value = entry.value().clone();

            return Some((ts, value));
        }

        None
    }
}
