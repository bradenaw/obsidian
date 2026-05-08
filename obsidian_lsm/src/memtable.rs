use std::sync::RwLock;

use crossbeam_skiplist::SkipMap;
use obsidian_common::Bound;
use obsidian_common::Direction;
use obsidian_common::HistoryRange;
use obsidian_common::KeyspaceId;
use obsidian_common::Range;
use obsidian_common::Revision;
use obsidian_common::RevisionValue;
use obsidian_common::RunId;
use obsidian_common::Timestamp;
use obsidian_util::hexlify;
use obsidian_util::IteratorEither;

pub(crate) struct Memtable {
    run_id: RunId,
    keyspace_id: KeyspaceId,
    kvs: SkipMap<Vec<u8>, SkipMap<Timestamp, RevisionValue>>,
    stats: RwLock<MemtableStats>,
}

struct MemtableStats {
    size: u64,
    max_key_len: usize,
}

impl Memtable {
    pub fn new(keyspace_id: KeyspaceId) -> Self {
        Self {
            run_id: RunId::new(),
            keyspace_id,
            kvs: SkipMap::new(),
            stats: RwLock::new(MemtableStats {
                size: 0,
                max_key_len: 0,
            }),
        }
    }

    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    #[cfg(test)]
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

    pub fn insert(&self, k: Vec<u8>, ts: Timestamp, v: RevisionValue) -> u64 {
        log::trace!(
            "memtable {}: insert {}@{}",
            self.run_id,
            hexlify(&k[..]),
            ts
        );

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
    ) -> impl Iterator<Item = Revision> + Send + '_ {
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
                        Some(Revision {
                            key: (self.keyspace_id, revisions_entry.key().clone()),
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

        match direction {
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
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = Revision> + '_ {
        let keyspace_id = self.keyspace_id;
        self.kvs.iter().flat_map(move |entry| {
            let key = entry.key().clone();

            self.history(&key, HistoryRange::All, Direction::Desc)
                .map(move |(ts, value)| Revision {
                    key: (keyspace_id, key.clone()),
                    ts,
                    value,
                })
        })
    }

    #[cfg(test)]
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
