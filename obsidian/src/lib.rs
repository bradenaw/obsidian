#![allow(dead_code)]

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BinaryHeap;
use std::time::SystemTime;

use rand::Rng;

struct Lsm {
    last_ts: u64,
    l0: Memtable,
    l0_max_size: usize,
    // levels[0] is empty and unused, to make the naming easier.
    levels: Vec<Level>,
}

impl Lsm {
    fn new() -> Self {
        Self {
            last_ts: 0,
            l0_max_size: 64,
            l0: Memtable::new(),
            levels: (0..7).map(|_| Level::new()).collect(),
        }
    }

    fn get(&self, ts: u64, k: &[u8]) -> Option<Vec<u8>> {
        if let Some((_, v)) = self.l0.get(ts, k) {
            return match v {
                Value::Regular(v) => Some(v),
                Value::Tombstone => None,
            };
        }
        for level in &self.levels {
            if let Some((_, v)) = level.get(ts, k) {
                return match v {
                    Value::Regular(v) => Some(v),
                    Value::Tombstone => None,
                };
            }
        }
        None
    }

    fn put(&mut self, k: Vec<u8>, v: Vec<u8>) -> u64 {
        let ts = std::cmp::max(
            self.last_ts + 1,
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("now before UNIX_EPOCH?")
                .as_nanos() as u64,
        );
        self.last_ts = ts;
        self.l0.put(k, ts, v);
        if self.l0.size() > self.l0_max_size {
            self.compact_l0();

            for i in 1..self.levels.len() - 1 {
                if self.levels[i].size() <= self.l0_max_size * 10_usize.pow(i as u32) {
                    break;
                }
                self.compact_from(i);
            }
        }
        ts
    }

    fn compact_l0(&mut self) {
        let (min_key, max_key) = match self.l0.range() {
            Some(r) => r,
            // l0 is empty, nothing to do
            None => return,
        };

        let l0 = std::mem::take(&mut self.l0);

        self.compact_inner(1, min_key, max_key, l0.into_iter())
    }

    fn compact_from(&mut self, level: usize) {
        if self.levels[level].runs.is_empty() {
            return;
        }
        let idx = rand::thread_rng().gen_range(0..self.levels[level].runs.len());
        let run = self.levels[level].runs.remove(idx);
        let (min_key, max_key) = match run.range() {
            Some((min_key, max_key)) => (min_key, max_key),
            None => return,
        };
        self.compact_inner(level + 1, min_key, max_key, run.into_iter());
    }

    fn compact_inner(
        &mut self,
        into_level: usize,
        min_key: Vec<u8>,
        max_key: Vec<u8>,
        entries: impl Iterator<Item = (Vec<u8>, u64, Value)>,
    ) {
        let overlapping_runs = self.levels[into_level].take_overlapping_runs(min_key, max_key);

        let existing_iter = overlapping_runs
            .into_iter()
            .map(|run| run.into_iter())
            .flatten()
            .map(|(k, ts, v)| OrdEqByFirst((k, ts), v));

        let sorted = merge_sorted(vec![
            Box::new(existing_iter)
                as Box<dyn Iterator<Item = OrdEqByFirst<(Vec<u8>, u64), Value>>>,
            Box::new(entries.map(|(k, ts, v)| OrdEqByFirst((k, ts), v)))
                as Box<dyn Iterator<Item = _>>,
        ]);

        let mut runs = Vec::new();
        let mut curr: Vec<(Vec<u8>, u64, Value)> = Vec::new();
        let mut curr_size = 0;
        for OrdEqByFirst((k, ts), v) in sorted {
            let elem_size = k.len() + 8 + v.len();
            if curr.len() > 0
                && curr_size + elem_size > self.l0_max_size
                && curr.last().unwrap().0 != k
            {
                runs.push(Run::new(curr));
                curr = Vec::new();
                curr_size = 0;
            }
            curr.push((k, ts, v));
            curr_size += elem_size;
        }
        if curr.len() > 0 {
            runs.push(Run::new(curr));
        }

        self.levels[into_level].add_all(runs);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Value {
    Regular(Vec<u8>),
    Tombstone,
}

impl Value {
    fn len(&self) -> usize {
        match self {
            Value::Regular(v) => v.len(),
            Value::Tombstone => 0,
        }
    }
}

struct Memtable {
    size: usize,
    kvs: BTreeMap<Vec<u8>, BTreeMap<u64, Value>>,
}

impl Memtable {
    fn new() -> Self {
        Self {
            size: 0,
            kvs: BTreeMap::new(),
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

    fn into_iter(self) -> impl Iterator<Item = (Vec<u8>, u64, Value)> {
        self.kvs
            .into_iter()
            .map(|(key, entries)| {
                entries
                    .into_iter()
                    .map(move |(ts, value)| (key.clone(), ts, value))
            })
            .flatten()
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Memtable::new()
    }
}

struct Level {
    // In sorted order by range.
    runs: Vec<Run>,
}

impl Level {
    fn new() -> Self {
        Self { runs: vec![] }
    }

    fn get(&self, ts: u64, k: &[u8]) -> Option<(u64, Value)> {
        // TODO: binary search instead of linear
        for run in &self.runs {
            if let Some(r) = run.get(ts, k) {
                return Some(r);
            }
        }
        None
    }

    fn size(&self) -> usize {
        self.runs.iter().map(|run| run.size()).sum()
    }

    fn take_overlapping_runs(&mut self, min_key: Vec<u8>, max_key: Vec<u8>) -> Vec<Run> {
        let start_idx = match self
            .runs
            .binary_search_by_key(&min_key, |run| run.range().unwrap().1)
        {
            Ok(idx) => idx,
            Err(idx) => idx,
        };

        let end_idx = match self
            .runs
            .binary_search_by_key(&max_key, |run| run.range().unwrap().0)
        {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        self.runs.drain(start_idx..end_idx).collect()
    }

    fn add_all(&mut self, runs: Vec<Run>) {
        let idx = match self
            .runs
            .binary_search_by_key(&runs[0].range().unwrap().0, |run| run.range().unwrap().0)
        {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        self.runs.splice(idx..idx, runs).for_each(|_| {});
    }
}

struct Run {
    // Sorted by (k, ts).
    kvs: Vec<(Vec<u8>, u64, Value)>,
    size: usize,
}

impl Run {
    fn new(kvs: Vec<(Vec<u8>, u64, Value)>) -> Self {
        let size = kvs.iter().map(|(k, _, v)| k.len() + 8 + v.len()).sum();
        Self { kvs, size }
    }

    fn size(&self) -> usize {
        self.size
    }

    fn get(&self, ts: u64, k: &[u8]) -> Option<(u64, Value)> {
        let entry = match self
            .kvs
            .binary_search_by_key(&(k, ts), |entry| (&entry.0, entry.1))
        {
            Ok(idx) => &self.kvs[idx],
            Err(next_idx) if next_idx > 0 => &self.kvs[next_idx - 1],
            _ => return None,
        };
        if entry.0 != k {
            return None;
        }
        Some((entry.1, entry.2.clone()))
    }

    fn range(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        Some((self.kvs.first()?.0.clone(), self.kvs.last()?.0.clone()))
    }

    fn into_iter(self) -> impl Iterator<Item = (Vec<u8>, u64, Value)> {
        self.kvs.into_iter()
    }
}

pub fn merge_sorted<'a, T: Ord + 'a>(
    mut iters: Vec<impl Iterator<Item = T> + 'a>,
) -> impl Iterator<Item = T> + 'a {
    let mut h: BinaryHeap<(std::cmp::Reverse<T>, usize)> = BinaryHeap::new();
    h.reserve_exact(iters.len());
    for i in 0..iters.len() {
        if let Some(t) = iters[i].next() {
            h.push((std::cmp::Reverse(t), i));
        }
    }
    std::iter::from_fn(move || {
        let (t, i) = h.pop()?;
        if let Some(t) = iters[i].next() {
            h.push((std::cmp::Reverse(t), i));
        }
        Some(t.0)
    })
}

pub struct OrdEqByFirst<A, B>(pub A, pub B);

impl<A: Eq, B> Eq for OrdEqByFirst<A, B> {}
impl<A: Eq, B> PartialEq for OrdEqByFirst<A, B> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl<A: Ord, B> Ord for OrdEqByFirst<A, B> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}
impl<A: Ord, B> PartialOrd for OrdEqByFirst<A, B> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use crate::Lsm;
    use crate::Run;
    use crate::Value;

    #[test]
    fn test_put_get() {
        let mut lsm = Lsm::new();
        let k = b"abc";
        let not_k = b"def";
        let v = b"foo";
        let write_ts = lsm.put(k.to_vec(), v.to_vec());
        assert_eq!(lsm.get(write_ts - 1, k), None);
        assert_eq!(lsm.get(write_ts, k), Some(v.to_vec()));
        assert_eq!(lsm.get(write_ts + 1, k), Some(v.to_vec()));
        assert_eq!(lsm.get(write_ts - 1, not_k), None);
        assert_eq!(lsm.get(write_ts, not_k), None);
        assert_eq!(lsm.get(write_ts + 1, not_k), None);
    }

    #[test]
    fn test_run_get() {
        let run = Run::new(vec![
            (b"a".to_vec(), 10, Value::Regular(b"a10".to_vec())),
            (b"a".to_vec(), 15, Value::Regular(b"a15".to_vec())),
            (b"b".to_vec(), 10, Value::Regular(b"b10".to_vec())),
            (b"b".to_vec(), 15, Value::Regular(b"b15".to_vec())),
        ]);

        assert_eq!(run.get(9, b"a"), None);
        assert_eq!(
            run.get(10, b"a"),
            Some((10, Value::Regular(b"a10".to_vec())))
        );
        assert_eq!(
            run.get(11, b"a"),
            Some((10, Value::Regular(b"a10".to_vec())))
        );
        assert_eq!(
            run.get(16, b"a"),
            Some((15, Value::Regular(b"a15".to_vec())))
        );
        assert_eq!(run.get(9, b"b"), None);
        assert_eq!(
            run.get(17, b"b"),
            Some((15, Value::Regular(b"b15".to_vec())))
        );
    }

    #[test]
    fn test_compact_l0() {
        let mut lsm = Lsm::new();
        let mut map = BTreeMap::new();
        let mut last_ts = 0;
        let mut runs_in_l1 = 0;
        for _ in 0..10 {
            for i in 0..usize::MAX {
                let v = (i % 179) as u8;
                let put_ts = lsm.put(vec![i as u8], vec![v]);
                last_ts = std::cmp::max(put_ts, last_ts);
                map.insert(i as u8, v);

                // Insert until we trigger a compaction.
                if lsm.levels[1].runs.len() != runs_in_l1 {
                    runs_in_l1 = lsm.levels[1].runs.len();
                    break;
                }
            }

            for (k, v) in &map {
                assert_eq!(lsm.get(last_ts, &[*k]), Some(vec![*v]));
            }
        }
    }

    #[test]
    fn test_compact_l1() {
        let mut lsm = Lsm::new();
        let mut map = BTreeMap::new();
        let mut last_ts = 0;
        let mut runs_in_l2 = 0;
        for _ in 0..3 {
            for i in 0..usize::MAX {
                let v = (i % 179) as u8;
                let put_ts = lsm.put(vec![i as u8], vec![v]);
                last_ts = std::cmp::max(put_ts, last_ts);
                map.insert(i as u8, v);

                // Insert until we trigger a compaction.
                if lsm.levels[2].runs.len() != runs_in_l2 {
                    runs_in_l2 = lsm.levels[2].runs.len();
                    break;
                }
            }

            for (k, v) in &map {
                assert_eq!(lsm.get(last_ts, &[*k]), Some(vec![*v]));
            }
        }
    }
}
