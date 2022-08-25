#![allow(dead_code)]
#![feature(map_first_last)]
#![feature(result_into_ok_or_err)]

use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::collections::BinaryHeap;
use std::convert::TryFrom;
use std::time::SystemTime;

use anyhow::anyhow;
use byteorder::{ByteOrder, LittleEndian};
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
        let idx = match self
            .runs
            .binary_search_by_key(&k.to_vec(), |run| run.range().unwrap().1)
        {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        if idx >= self.runs.len() {
            return None;
        }
        self.runs[idx].get(ts, k)
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

const BLOCK_INDEX_HEADER_SIZE: usize = 21;

// Assumed that kvs values are in reverse order by timestamp.
fn encode_block(kvs: BTreeMap<Vec<u8>, Vec<(u64, Vec<u8>)>>) -> anyhow::Result<Vec<u8>> {
    let mut block = [0u8; 4].to_vec();

    let prefix: Vec<u8> = {
        let (first_key, _) = kvs
            .first_key_value()
            .ok_or_else(|| anyhow!("empty block"))?;
        let (last_key, _) = kvs.last_key_value().ok_or_else(|| anyhow!("empty block"))?;
        std::iter::zip(first_key, last_key)
            .take_while(|(a, b)| *a == *b)
            .map(|(a, _)| *a)
            .collect()
    };
    let (min_ts, bytes_per_ts_offset) = {
        let mut min_ts = u64::MAX;
        let mut max_ts = 0;
        for (_, versions) in &kvs {
            min_ts = std::cmp::min(
                min_ts,
                versions
                    .last()
                    .ok_or_else(|| anyhow!("key has no versions"))?
                    .0,
            );
            max_ts = std::cmp::max(
                max_ts,
                versions
                    .first()
                    .ok_or_else(|| anyhow!("key has no versions"))?
                    .0,
            );
        }
        (
            min_ts,
            (((64 - (max_ts - min_ts).leading_zeros()) + 7) / 8) as usize,
        )
    };

    let n_keys = kvs.len();
    let mut n_versions = 0;
    let mut suffixes = Vec::new();
    let mut suffix_offsets = Vec::new();
    let mut ts_value_offsets = Vec::new();
    for (key, versions) in kvs.iter() {
        let mut suffix_offsets_buf = [0u8; 4];
        LittleEndian::write_u16(&mut suffix_offsets_buf[..], u16::try_from(suffixes.len())?);
        LittleEndian::write_u16(&mut suffix_offsets_buf[2..], u16::try_from(n_versions)?);
        suffix_offsets.extend_from_slice(&suffix_offsets_buf[..]);
        suffixes.extend_from_slice(&key[prefix.len()..]);
        for (ts, value) in versions {
            let mut buf = [0u8; 10];
            LittleEndian::write_u64(&mut buf[..], ts - min_ts);
            LittleEndian::write_u16(
                &mut buf[bytes_per_ts_offset..],
                u16::try_from(block.len() - 4)?,
            );
            ts_value_offsets.extend_from_slice(&buf[..bytes_per_ts_offset + 2]);
            block.extend((&value).iter());
        }
        n_versions += versions.len();
    }
    let values_len = block.len() - 4;
    LittleEndian::write_u32(&mut block[0..4], values_len as u32);

    let mut header = [0u8; BLOCK_INDEX_HEADER_SIZE];
    LittleEndian::write_u32(&mut header[0..4], values_len as u32);
    LittleEndian::write_u16(&mut header[4..6], n_keys as u16);
    LittleEndian::write_u16(&mut header[6..8], prefix.len() as u16);
    LittleEndian::write_u16(&mut header[8..10], suffixes.len() as u16);
    LittleEndian::write_u16(&mut header[10..12], n_versions as u16);
    LittleEndian::write_u64(&mut header[12..20], min_ts);
    header[20] = bytes_per_ts_offset as u8;

    block.extend(&header[..]);
    block.extend(&prefix[..]);
    block.extend(&suffixes[..]);
    block.extend(&suffix_offsets[..]);
    block.extend(&ts_value_offsets[..]);

    Ok(block)
}

struct Block<'a> {
    values_len: usize,
    n_keys: usize,
    prefix: &'a [u8],
    suffixes_len: usize,
    n_versions: usize,
    min_ts: u64,
    ts_bytes: usize,
    b: &'a [u8],
}

impl<'a> Block<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        let values_len = LittleEndian::read_u32(&b[0..4]) as usize;
        println!("values_len = {}", values_len);
        let header_idx = values_len + 4;
        let header = &b[header_idx..header_idx + 21];

        let prefix_len = LittleEndian::read_u16(&header[6..8]) as usize;

        Self {
            b,
            values_len,
            n_keys: LittleEndian::read_u16(&header[4..6]) as usize,
            prefix: &b[header_idx + BLOCK_INDEX_HEADER_SIZE
                ..header_idx + BLOCK_INDEX_HEADER_SIZE + prefix_len],
            suffixes_len: LittleEndian::read_u16(&header[8..10]) as usize,
            n_versions: LittleEndian::read_u16(&header[10..12]) as usize,
            min_ts: LittleEndian::read_u64(&header[12..20]),
            ts_bytes: header[20] as usize,
        }
    }

    fn suffixes(&self) -> &[u8] {
        let start = 4 + self.values_len + BLOCK_INDEX_HEADER_SIZE + self.prefix.len();
        &self.b[start..start + self.suffixes_len]
    }

    fn suffix_offsets(&self) -> &[u8] {
        let start =
            4 + self.values_len + BLOCK_INDEX_HEADER_SIZE + self.prefix.len() + self.suffixes_len;
        &self.b[start..start + self.n_keys * 4]
    }

    fn ts_value_offsets(&self) -> &[u8] {
        let start = 4
            + self.values_len
            + BLOCK_INDEX_HEADER_SIZE
            + self.prefix.len()
            + self.suffixes_len
            + self.suffixes_len * 4;
        &self.b[start..start + self.n_versions * (self.ts_bytes + 2)]
    }

    fn ts_value_offset(&self, ts_value_offsets: &[u8], idx: usize) -> (u64, usize) {
        let width = self.ts_bytes + 2;
        let elem = &ts_value_offsets[width * idx..width * (idx + 1)];
        let mut ts_offset = [0u8; 8];
        ts_offset[..self.ts_bytes].copy_from_slice(&elem[..self.ts_bytes]);
        (
            LittleEndian::read_u64(&ts_offset[..]) + self.min_ts,
            LittleEndian::read_u16(&elem[elem.len() - 2..]) as usize,
        )
    }

    fn suffix<'b>(suffix_offsets: &[u8], suffixes: &'b [u8], idx: usize) -> &'b [u8] {
        let start = LittleEndian::read_u16(&suffix_offsets[idx * 4..idx * 4 + 2]) as usize;
        let end = if idx == suffix_offsets.len() / 4 - 1 {
            suffixes.len()
        } else {
            LittleEndian::read_u16(&suffix_offsets[(idx + 1) * 4..(idx + 1) * 4 + 2]) as usize
        };
        &suffixes[start..end]
    }

    pub fn get(&self, ts: u64, k: &[u8]) -> Option<(u64, Vec<u8>)> {
        if !k.starts_with(self.prefix) {
            return None;
        }
        let suffix = &k[self.prefix.len()..];
        let suffixes = self.suffixes();
        let suffix_offsets = self.suffix_offsets();

        let key_idx = binary_search_by_idx(self.n_keys, suffix, |idx| {
            Self::suffix(suffix_offsets, suffixes, idx)
        })
        .ok()?;
        let ts_value_offsets_start =
            LittleEndian::read_u16(&suffix_offsets[key_idx * 4 + 2..key_idx * 4 + 4]) as usize;
        let n_versions = if key_idx == self.n_keys - 1 {
            self.n_versions - ts_value_offsets_start
        } else {
            let ts_value_offsets_end = LittleEndian::read_u16(
                &suffix_offsets[(key_idx + 1) * 4 + 2..(key_idx + 1) * 4 + 4],
            ) as usize;
            ts_value_offsets_end - ts_value_offsets_start
        };
        println!("n_versions for key {} = {}", hexlify(k), n_versions);
        let ts_value_offsets = self.ts_value_offsets();
        let ts_val_idx = binary_search_by_idx(n_versions, Reverse(ts), |idx| {
            Reverse(
                self.ts_value_offset(ts_value_offsets, ts_value_offsets_start + idx)
                    .0,
            )
        })
        .into_ok_or_err();
        if ts_val_idx == n_versions {
            return None;
        }

        let (record_ts, value_start) =
            self.ts_value_offset(ts_value_offsets, ts_value_offsets_start + ts_val_idx);
        println!("n_versions = {}", self.n_versions);
        println!("ts_val_idx = {}", ts_val_idx);
        let value_end = if ts_value_offsets_start + ts_val_idx == self.n_versions - 1 {
            self.values_len
        } else {
            println!("idx of next = {}", ts_value_offsets_start + ts_val_idx + 1);
            self.ts_value_offset(ts_value_offsets, ts_value_offsets_start + ts_val_idx + 1)
                .1
        };
        Some((record_ts, self.b[4 + value_start..4 + value_end].to_vec()))
    }
}

fn binary_search_by_idx<K: Ord, F: Fn(usize) -> K>(n: usize, k: K, f: F) -> Result<usize, usize> {
    let mut lower = 0;
    let mut upper = n;
    while lower < upper {
        let mid = (lower + upper) / 2;
        println!("lower={} upper={} mid={}", lower, upper, mid);
        let at_mid = f(mid);
        match k.cmp(&at_mid) {
            Ordering::Equal => return Ok(mid),
            Ordering::Less => upper = mid,
            Ordering::Greater => lower = mid + 1,
        }
    }
    // XXX: not sure if correct
    Err(lower)
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use crate::binary_search_by_idx;
    use crate::encode_block;
    use crate::hexlify;
    use crate::Block;
    use crate::Lsm;
    use crate::Run;
    use crate::Value;

    use byteorder::{ByteOrder, LittleEndian};

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

    #[test]
    fn test_binary_search_by_key() {
        for n in 1..32 {
            for i in 0..n {
                assert_eq!(binary_search_by_idx(n, i, |x| x), Ok(i));
            }
        }
        for n in 1..32 {
            for i in 0..=n {
                assert_eq!(binary_search_by_idx(n, 2 * i, |x| 2 * x + 1), Err(i));
            }
        }
    }

    #[test]
    fn test_block() {
        let aa: Vec<u8> = "aa".into();
        let ab: Vec<u8> = "ab".into();
        let aa_279 = (279, "foo".into());
        let aa_265 = (265, "bar".into());
        let ab_341 = (341, "baz".into());
        let ab_302 = (302, "qux".into());
        let ab_290 = (290, "garply".into());
        let encoded = encode_block({
            let mut kvs = BTreeMap::new();
            kvs.insert(aa.clone(), vec![aa_279.clone(), aa_265.clone()]);
            kvs.insert(
                ab.clone(),
                vec![ab_341.clone(), ab_302.clone(), ab_290.clone()],
            );
            kvs
        })
        .unwrap();

        let block = Block::new(&encoded[..]);
        println!("{}", hexlify(&encoded[..]));
        println!(
            "{}^{}^",
            (0..8).map(|_| " ").collect::<String>(),
            (0..block.values_len * 2).map(|_| " ").collect::<String>()
        );

        println!("n_keys = {}", block.n_keys);
        println!("prefix = {}", hexlify(block.prefix));
        let suffix_offsets = block.suffix_offsets();
        println!("suffix_offsets = {}", hexlify(suffix_offsets));
        println!(
            "{}",
            suffix_offsets
                .chunks_exact(4)
                .map(|chunk| format!(
                    "  suffix_offset = {}, versions_idx = {}\n",
                    LittleEndian::read_u16(&chunk),
                    LittleEndian::read_u16(&chunk[2..]),
                ))
                .collect::<String>()
        );
        let suffixes = block.suffixes();
        println!("suffixes       = {}", hexlify(suffixes));
        println!("n_versions     = {}", block.n_versions);
        let ts_value_offsets = block.ts_value_offsets();
        for i in 0..block.n_versions {
            let (ts, value_offset) = block.ts_value_offset(ts_value_offsets, i);
            println!("  {} ts {} value @ {}", i, ts, value_offset);
        }
        assert_eq!(block.get(279, &aa[..]), Some(aa_279.clone()));
        assert_eq!(block.get(265, &aa[..]), Some(aa_265.clone()));
        assert_eq!(block.get(123, &aa[..]), None);

        assert_eq!(block.get(295, &aa[..]), Some(aa_279.clone()));
        assert_eq!(block.get(269, &aa[..]), Some(aa_265.clone()));

        assert_eq!(block.get(341, &ab[..]), Some(ab_341.clone()));
        assert_eq!(block.get(302, &ab[..]), Some(ab_302.clone()));
        assert_eq!(block.get(290, &ab[..]), Some(ab_290.clone()));
        assert_eq!(block.get(289, &ab[..]), None);

        assert_eq!(block.get(500, &ab[..]), Some(ab_341.clone()));
        assert_eq!(block.get(340, &ab[..]), Some(ab_302.clone()));
        assert_eq!(block.get(300, &ab[..]), Some(ab_290.clone()));
    }
}

fn hexlify(b: &[u8]) -> String {
    b.iter().map(|b| format!("{:02x}", b)).collect()
}
