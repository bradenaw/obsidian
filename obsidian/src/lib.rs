#![allow(dead_code)]
#![feature(map_first_last)]

use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::collections::BinaryHeap;
use std::convert::TryFrom;
use std::io::SeekFrom;
use std::marker::PhantomData;
use std::time::SystemTime;

use anyhow::anyhow;
use byteorder::ByteOrder;
use byteorder::LittleEndian;
use futures::pin_mut;
use futures::stream::Stream;
use futures::stream::StreamExt;
use rand::Rng;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeek;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

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

struct Block<'a> {
    values_len: usize,
    n_versions: usize,
    min_ts: u64,
    ts_bytes: usize,
    index: PrefixCompressedKV<u16>,
    versions_start: usize,
    b: &'a [u8],
}

impl<'a> Block<'a> {
    const BLOCK_INDEX_HEADER_SIZE: usize = 17;

    // Assumes that kvs values are in reverse order by timestamp.
    //
    // Returns the encoded block and the offset of the header within the block.
    pub fn encode(kvs: &BTreeMap<Vec<u8>, Vec<(u64, Value)>>) -> anyhow::Result<(Vec<u8>, usize)> {
        let (min_ts, bytes_per_ts_offset) = {
            let mut min_ts = u64::MAX;
            let mut max_ts = 0;
            for (_, versions) in kvs {
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
                // +8 instead of +7, since we pack an extra tombstone bit in there
                (((64 - (max_ts - min_ts).leading_zeros()) + 8) / 8) as usize,
            )
        };

        let mut index: BTreeMap<Vec<u8>, u16> = BTreeMap::new();
        let mut block = vec![];

        let mut n_versions = 0;
        let mut versions = Vec::new();
        for (key, key_versions) in kvs {
            index.insert(key.clone(), n_versions as u16);
            for (ts, value) in key_versions {
                let mut buf = [0u8; 10];
                let value_offset = block.len();
                let tombstone_bit = match value {
                    Value::Regular(value) => {
                        block.extend_from_slice(&value[..]);
                        0
                    }
                    Value::Tombstone => 1,
                };
                let ts_offset_and_tombstone = ((ts - min_ts) << 1) | tombstone_bit;
                assert!(ts_offset_and_tombstone < 1 << (8 * bytes_per_ts_offset));
                LittleEndian::write_u64(&mut buf[..], ts_offset_and_tombstone);
                LittleEndian::write_u16(
                    &mut buf[bytes_per_ts_offset..],
                    u16::try_from(value_offset)?,
                );
                versions.extend_from_slice(&buf[..bytes_per_ts_offset + 2]);
            }
            n_versions += key_versions.len();
        }
        let values_len = block.len();
        let encoded_index = PrefixCompressedKV::encode(&index);

        let mut header = [0u8; Self::BLOCK_INDEX_HEADER_SIZE];
        LittleEndian::write_u32(&mut header[0..4], values_len as u32);
        LittleEndian::write_u16(&mut header[4..6], n_versions as u16);
        LittleEndian::write_u64(&mut header[6..14], min_ts);
        LittleEndian::write_u16(&mut header[14..16], encoded_index.len() as u16);
        header[16] = bytes_per_ts_offset as u8;

        let header_idx = block.len();
        block.extend_from_slice(&header[..]);
        block.extend_from_slice(&encoded_index[..]);
        println!("Block::encode");
        println!("  versions = {}", hexlify(&versions));
        block.extend_from_slice(&versions[..]);

        Ok((block, header_idx))
    }

    pub fn open(b: &'a [u8], header_offset: usize) -> Self {
        let header = &b[header_offset..header_offset + Self::BLOCK_INDEX_HEADER_SIZE];

        let values_len = LittleEndian::read_u32(&header[0..4]) as usize;
        let n_versions = LittleEndian::read_u16(&header[4..6]) as usize;
        let min_ts = LittleEndian::read_u64(&header[6..14]);
        let index_len = LittleEndian::read_u16(&header[14..16]) as usize;
        let bytes_per_ts_offset = header[16] as usize;

        let index_start = header_offset + Self::BLOCK_INDEX_HEADER_SIZE;
        let index = PrefixCompressedKV::decode(b[index_start..index_start + index_len].into());
        let versions_start = index_start + index_len;

        Self {
            b,
            values_len,
            n_versions,
            min_ts,
            index,
            versions_start,
            ts_bytes: bytes_per_ts_offset,
        }
    }

    fn versions(&self) -> BlockVersions<'_> {
        BlockVersions {
            ts_bytes: self.ts_bytes,
            min_ts: self.min_ts,
            end_offset: self.values_len,
            b: &self.b[self.versions_start..],
        }
    }

    fn versions_for_key(&self, key_idx: usize) -> BlockVersions<'_> {
        let start_idx = self.index.get_value(key_idx) as usize;
        let end_idx = if key_idx == self.index.len() - 1 {
            self.n_versions
        } else {
            self.index.get_value(key_idx + 1) as usize
        };
        self.versions().slice(start_idx, end_idx)
    }

    pub fn get(&self, ts: u64, k: &[u8]) -> Option<(u64, Vec<u8>)> {
        let key_idx = self.index.search(k).ok()?;
        let key_versions = self.versions_for_key(key_idx);

        println!("n_versions for key {} = {}", hexlify(k), key_versions.len());
        let version_idx = binary_search_by_idx(key_versions.len(), Reverse(ts), |idx| {
            Reverse(key_versions.ts(idx))
        })
        .unwrap_or_else(core::convert::identity);
        if version_idx == key_versions.len() {
            return None;
        }
        let record_ts = key_versions.ts(version_idx);
        let (value_start, value_end) = key_versions.value_offsets(version_idx)?;
        Some((record_ts, self.b[value_start..value_end].to_vec()))
    }
}

struct BlockVersions<'a> {
    ts_bytes: usize,
    min_ts: u64,
    end_offset: usize,
    b: &'a [u8],
}

impl<'a> BlockVersions<'a> {
    fn len(&self) -> usize {
        self.b.len() / (self.ts_bytes + 2)
    }

    fn elem(&self, idx: usize) -> (u64, bool, usize) {
        let width = self.ts_bytes + 2;
        let elem = &self.b[width * idx..width * (idx + 1)];
        let mut ts_offset_buf = [0u8; 8];
        ts_offset_buf[..self.ts_bytes].copy_from_slice(&elem[..self.ts_bytes]);
        let ts_offset_and_tombstone = LittleEndian::read_u64(&ts_offset_buf[..]);
        let tombstone = ts_offset_and_tombstone & 1 == 1;
        let ts = (ts_offset_and_tombstone >> 1) + self.min_ts;
        (
            ts,
            tombstone,
            LittleEndian::read_u16(&elem[elem.len() - 2..]) as usize,
        )
    }

    fn slice(&self, start_idx: usize, end_idx: usize) -> BlockVersions<'a> {
        let width = self.ts_bytes + 2;
        let b = &self.b[start_idx * width..end_idx * width];
        let end_offset = if end_idx == self.len() {
            self.end_offset
        } else {
            self.elem(end_idx).2
        };
        BlockVersions {
            ts_bytes: self.ts_bytes,
            min_ts: self.min_ts,
            end_offset,
            b,
        }
    }

    fn ts(&self, idx: usize) -> u64 {
        self.elem(idx).0
    }

    fn value_offsets(&self, idx: usize) -> Option<(usize, usize)> {
        let (_, tombstone, start) = self.elem(idx);
        if tombstone {
            return None;
        }
        let end = if idx == self.len() - 1 {
            self.end_offset
        } else {
            self.elem(idx + 1).2
        };
        Some((start, end))
    }
}

struct Record {
    key: Vec<u8>,
    ts: u64,
    value: Value,
}

struct PrefixCompressedKV<V> {
    v: PhantomData<V>,
    offset_width: usize,
    prefix_len: usize,
    n: usize,
    suffixes_len: usize,
    data: Vec<u8>,
}

const PREFIX_COMPRESSED_KV_HEADER_SIZE: usize = 9;

impl<V: FixedSizeSerializable> PrefixCompressedKV<V> {
    fn encode(m: &BTreeMap<Vec<u8>, V>) -> Vec<u8> {
        let prefix: Vec<u8> = match (m.first_key_value(), m.last_key_value()) {
            (Some((first_key, _)), Some((last_key, _))) => {
                longest_shared_prefix(&first_key[..], &last_key[..])
            }
            _ => vec![],
        };
        let suffixes_len: usize = m.keys().map(|k| k.len() - prefix.len()).sum();

        let offset_width = if suffixes_len < 1 << 8 {
            1
        } else if suffixes_len < 1 << 16 {
            2
        } else {
            4
        };

        let offset_and_value_width = offset_width + V::size();
        let mut suffixes = Vec::with_capacity(suffixes_len);
        let mut offset_and_values = Vec::with_capacity(m.len() * offset_and_value_width);

        let mut offset_and_value = vec![0u8; std::cmp::max(4, offset_and_value_width)];
        for (k, v) in m {
            let offset = suffixes.len();

            suffixes.extend_from_slice(&k[prefix.len()..]);

            for i in 0..offset_and_value.len() {
                offset_and_value[i] = 0;
            }
            LittleEndian::write_u32(&mut offset_and_value[..], offset as u32);
            v.write(&mut offset_and_value[offset_width..]);
            offset_and_values.extend_from_slice(&offset_and_value[..offset_and_value_width]);
        }

        let mut header = [0u8; PREFIX_COMPRESSED_KV_HEADER_SIZE];
        header[0] = offset_width as u8;
        LittleEndian::write_u16(&mut header[1..3], m.len() as u16);
        LittleEndian::write_u16(&mut header[3..5], prefix.len() as u16);
        LittleEndian::write_u32(&mut header[5..9], suffixes.len() as u32);

        let mut out = Vec::with_capacity(
            header.len() + prefix.len() + offset_and_values.len() + suffixes.len(),
        );

        out.extend_from_slice(&header[..]);
        out.extend_from_slice(&prefix[..]);
        out.extend_from_slice(&offset_and_values[..]);
        out.extend_from_slice(&suffixes[..]);

        out
    }

    fn decode(data: Vec<u8>) -> Self {
        let header = &data[0..PREFIX_COMPRESSED_KV_HEADER_SIZE];
        let offset_width = header[0] as usize;
        let n = LittleEndian::read_u16(&header[1..3]) as usize;
        let prefix_len = LittleEndian::read_u16(&header[3..5]) as usize;
        let suffixes_len = LittleEndian::read_u32(&header[5..9]) as usize;

        Self {
            offset_width,
            n,
            prefix_len,
            suffixes_len,
            data,
            v: PhantomData,
        }
    }

    fn len(&self) -> usize {
        self.n
    }

    fn prefix(&self) -> &[u8] {
        &self.data
            [PREFIX_COMPRESSED_KV_HEADER_SIZE..PREFIX_COMPRESSED_KV_HEADER_SIZE + self.prefix_len]
    }

    fn suffixes(&self) -> &[u8] {
        let start = PREFIX_COMPRESSED_KV_HEADER_SIZE
            + self.prefix_len
            + self.n * (self.offset_width + V::size());
        &self.data[start..]
    }

    fn offset_and_values(&self) -> &[u8] {
        let start = PREFIX_COMPRESSED_KV_HEADER_SIZE + self.prefix_len;
        let end = start + self.n * (self.offset_width + V::size());
        &self.data[start..end]
    }

    fn search(&self, k: &[u8]) -> Result<usize, usize> {
        let prefix = self.prefix();
        if !k.starts_with(&prefix) {
            match k.cmp(&prefix) {
                Ordering::Equal => unreachable!(),
                Ordering::Less => return Err(0),
                Ordering::Greater => return Err(self.len()),
            }
        }
        let suffix = &k[prefix.len()..];
        binary_search_by_idx(self.len(), suffix, |idx| self.get_suffix(idx))
    }

    fn offset(&self, idx: usize) -> usize {
        let width = self.offset_width + V::size();
        let offset_start = idx * width;
        let offset_end = offset_start + self.offset_width;
        let mut offset: u32 = 0;
        for (i, b) in self.offset_and_values()[offset_start..offset_end]
            .iter()
            .enumerate()
        {
            offset |= (*b as u32) << (i * 8);
        }
        offset as usize
    }

    fn get_suffix(&self, idx: usize) -> &[u8] {
        let start = self.offset(idx);
        let end = if idx == self.len() - 1 {
            self.suffixes_len
        } else {
            self.offset(idx + 1)
        };
        &self.suffixes()[start..end]
    }

    fn get_key(&self, idx: usize) -> Vec<u8> {
        let prefix = self.prefix();
        let suffix = self.get_suffix(idx);
        let mut k = Vec::with_capacity(prefix.len() + suffix.len());
        k.extend_from_slice(prefix);
        k.extend_from_slice(suffix);
        k
    }

    fn get_value(&self, idx: usize) -> V {
        let width = self.offset_width + V::size();
        let offset_start = idx * width + self.offset_width;
        let offset_end = offset_start + V::size();
        V::read(&self.offset_and_values()[offset_start..offset_end])
    }
}

trait FixedSizeSerializable {
    fn size() -> usize;
    fn read(b: &[u8]) -> Self;
    fn write(&self, b: &mut [u8]);
}

impl FixedSizeSerializable for u16 {
    fn size() -> usize {
        2
    }
    fn read(b: &[u8]) -> Self {
        LittleEndian::read_u16(b)
    }
    fn write(&self, b: &mut [u8]) {
        LittleEndian::write_u16(b, *self);
    }
}

impl FixedSizeSerializable for u32 {
    fn size() -> usize {
        4
    }
    fn read(b: &[u8]) -> Self {
        LittleEndian::read_u32(b)
    }
    fn write(&self, b: &mut [u8]) {
        LittleEndian::write_u32(b, *self);
    }
}

struct LittleEndianU32(u32);

impl From<&[u8]> for LittleEndianU32 {
    fn from(b: &[u8]) -> Self {
        LittleEndianU32(LittleEndian::read_u32(b))
    }
}

struct RunFile<R> {
    r: R,

    size: usize,
    keyspace_id: u32,
    min_ts: u64,
    max_ts: u64,

    index: PrefixCompressedKV<u32>,

    min_key: Vec<u8>,
    max_key: Vec<u8>,
}

const INDEX_BLOCK_HEADER_SIZE: usize = 24;
const BLOCK_SIZE_LIMIT: usize = 32768;

impl<R> RunFile<R> {
    // Assumes S is in (key, rev(ts)) order, and assumes termination at a reasonable size limit.
    async fn write<W: AsyncWrite + Unpin, S: Stream<Item = anyhow::Result<Record>>>(
        w: &mut W,
        keyspace_id: u32,
        s: S,
    ) -> anyhow::Result<()> {
        async fn flush<W: AsyncWrite + Unpin>(
            w: &mut W,
            bytes_written: &mut usize,
            index: &mut BTreeMap<Vec<u8>, u32>,
            last_key: &mut Vec<u8>,
            buffer: &BTreeMap<Vec<u8>, Vec<(u64, Value)>>,
            continuation: bool,
        ) -> anyhow::Result<()> {
            if continuation {
                todo!();
            }

            let (first_key, last_key_) = match (index.first_key_value(), index.last_key_value()) {
                (Some((first_key, _)), Some((last_key, _))) => (first_key, last_key),
                _ => anyhow::bail!("empty block"),
            };
            *last_key = last_key_.clone();

            let (block, header_offset_in_block) = Block::encode(buffer)?;
            w.write_all(&block[..]).await?;
            let header_offset_in_file = *bytes_written + header_offset_in_block;

            index.insert(first_key.clone(), header_offset_in_file as u32);

            *bytes_written += block.len();

            Ok(())
        }

        pin_mut!(s);

        let mut buffer: BTreeMap<Vec<u8>, Vec<(u64, Value)>> = BTreeMap::new();
        let mut bytes_written = 0;
        let mut buffer_size = Block::BLOCK_INDEX_HEADER_SIZE;
        let mut prev_continuation = false;
        let mut index: BTreeMap<Vec<u8>, u32> = BTreeMap::new();
        let mut min_ts = u64::MAX;
        let mut max_ts = 0;
        let mut last_key = vec![];
        while let Some(record) = s.next().await.transpose()? {
            let record_size = {
                let key_len = if buffer.contains_key(&record.key) {
                    0
                } else {
                    record.key.len() + 4
                };
                key_len + 10 + record.value.len()
            };

            if !buffer.is_empty()
                && (buffer_size + record_size > BLOCK_SIZE_LIMIT
                    || (prev_continuation && !buffer.contains_key(&record.key)))
            {
                let mut this_key_versions = None;
                let mut continuation = false;
                if buffer.contains_key(&record.key) {
                    if buffer.len() == 1 {
                        // flush with continuation bit set
                        continuation = true;
                    } else {
                        // flush everything but last key
                        this_key_versions = buffer.remove(&record.key);
                    }
                }
                flush(
                    w,
                    &mut bytes_written,
                    &mut index,
                    &mut last_key,
                    &buffer,
                    continuation,
                )
                .await?;
                buffer.clear();
                buffer_size = 0;
                prev_continuation = continuation;
                if let Some(versions) = this_key_versions {
                    buffer_size = versions.iter().map(|(_, value)| 10 + value.len()).sum();
                    buffer.insert(record.key.clone(), versions);
                }
            }

            buffer
                .entry(record.key)
                .or_insert_with(Vec::new)
                .push((record.ts, record.value));
            buffer_size += record_size;

            min_ts = std::cmp::min(min_ts, record.ts);
            max_ts = std::cmp::max(max_ts, record.ts);
        }
        if !buffer.is_empty() {
            flush(
                w,
                &mut bytes_written,
                &mut index,
                &mut last_key,
                &buffer,
                false,
            )
            .await?;
        }

        index.insert(last_key.clone(), u32::MAX);

        let index_compressed = PrefixCompressedKV::encode(&index);

        let index_block_offset = bytes_written;
        let mut header = [0u8; INDEX_BLOCK_HEADER_SIZE];
        LittleEndian::write_u32(&mut header[0..4], keyspace_id);
        LittleEndian::write_u64(&mut header[4..12], min_ts);
        LittleEndian::write_u64(&mut header[12..20], max_ts);
        LittleEndian::write_u32(&mut header[20..24], index_compressed.len() as u32);
        w.write_all(&header[..]).await?;
        w.write_all(&index_compressed).await?;

        let mut index_block_offset_buf = [0u8; 4];
        LittleEndian::write_u32(&mut index_block_offset_buf[..], index_block_offset as u32);
        w.write_all(&index_block_offset_buf[..]).await?;

        Ok(())
    }
}

impl<R: AsyncRead + AsyncSeek + Unpin> RunFile<R> {
    async fn open(mut r: R, size: usize) -> anyhow::Result<Self> {
        r.seek(SeekFrom::End(-4)).await?;
        let index_block_offset = r.read_u32_le().await?;
        r.seek(SeekFrom::Start(index_block_offset as u64)).await?;
        let mut header = [0u8; INDEX_BLOCK_HEADER_SIZE];
        if r.read_exact(&mut header[..]).await? != header.len() {
            anyhow::bail!("incomplete header");
        }

        let keyspace_id = LittleEndian::read_u32(&header[0..4]);
        let min_ts = LittleEndian::read_u64(&header[4..12]);
        let max_ts = LittleEndian::read_u64(&header[12..20]);
        let index_len = LittleEndian::read_u32(&header[20..24]);

        let index = {
            let mut index_bytes = vec![0u8; index_len as usize];
            r.read_exact(&mut index_bytes[..]).await?;
            PrefixCompressedKV::decode(index_bytes)
        };

        let min_key = index.get_key(0);
        let max_key = index.get_key(index.len() - 1);

        Ok(Self {
            r,
            size,
            keyspace_id,
            min_ts,
            max_ts,
            index,

            min_key,
            max_key,
        })
    }

    fn size(&self) -> usize {
        self.size
    }

    fn get(&self, ts: u64, k: &[u8]) -> Option<(u64, Value)> {
        let block_header_start = self.index.search(k).ok()?;
        todo!();
    }

    fn range(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        Some((self.min_key.clone(), self.max_key.clone()))
    }

    fn into_stream(self) -> impl Stream<Item = anyhow::Result<Record>> {
        todo!();
        futures::stream::empty()
    }
}

fn binary_search_by_idx<K: Ord, F: Fn(usize) -> K>(n: usize, k: K, f: F) -> Result<usize, usize> {
    let mut lower = 0;
    let mut upper = n;
    while lower < upper {
        let mid = (lower + upper) / 2;
        let at_mid = f(mid);
        match k.cmp(&at_mid) {
            Ordering::Equal => return Ok(mid),
            Ordering::Less => upper = mid,
            Ordering::Greater => lower = mid + 1,
        }
    }
    Err(lower)
}

fn longest_shared_prefix(a: &[u8], b: &[u8]) -> Vec<u8> {
    std::iter::zip(a.iter(), b.iter())
        .take_while(|(a, b)| *a == *b)
        .map(|(a, _)| *a)
        .collect()
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use crate::binary_search_by_idx;
    use crate::hexlify;
    use crate::Block;
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
        let aa_279: Vec<u8> = "foo".into();
        let aa_265: Vec<u8> = "bar".into();
        let ab_341: Vec<u8> = "baz".into();
        let ab_302: Vec<u8> = "qux".into();
        let ab_290: Vec<u8> = "garply".into();
        let kvs = {
            let mut kvs = BTreeMap::new();
            kvs.insert(
                aa.clone(),
                vec![
                    (279, Value::Regular(aa_279.clone())),
                    (265, Value::Regular(aa_265.clone())),
                ],
            );
            kvs.insert(
                ab.clone(),
                vec![
                    (341, Value::Regular(ab_341.clone())),
                    (302, Value::Regular(ab_302.clone())),
                    (297, Value::Tombstone),
                    (290, Value::Regular(ab_290.clone())),
                ],
            );
            kvs
        };
        let (encoded, header_offset) = Block::encode(&kvs).unwrap();

        let block = Block::open(&encoded[..], header_offset);

        println!("encoded  = {}", hexlify(&encoded[..]));
        println!(
            "header   = {}",
            hexlify(&encoded[header_offset..header_offset + Block::BLOCK_INDEX_HEADER_SIZE])
        );
        //println!(
        //    "index    = {}"
        //    hexlify(&encoded[header_offset + Block::BLOCK_INDEX_HEADER_SIZE..header_offset+Block::BLOCK_INDEX_HEADER_SIZE+block.index_len]
        println!("versions = {}", hexlify(&encoded[block.versions_start..]));
        println!("prefix = {}", hexlify(block.index.prefix()));
        println!("n_versions = {}", block.n_versions);
        let versions = block.versions();
        for i in 0..block.n_versions {
            let value_str = match versions.value_offsets(i) {
                Some((value_start, value_end)) => format!("({}, {})", value_start, value_end),
                None => "<TOMBSTONE>".into(),
            };
            println!("  {} {} {}", i, versions.ts(i), value_str,);
        }
        println!("n_keys = {}", block.index.len());
        for i in 0..block.index.len() {
            println!(
                "  {} {} {}",
                i,
                hexlify(&block.index.get_key(i)),
                block.index.get_value(i)
            );
            //let key_versions = block.versions_for_key(i);
            //println!(
            //    "  {} {}  {} versions",
            //    i,
            //    hexlify(&block.index.get_key(i)),
            //    key_versions.len()
            //);
            //for j in 0..key_versions.len() {
            //    let value_str = match key_versions.value_offsets(j) {
            //        Some((value_start, value_end)) => format!("({}, {})", value_start, value_end),
            //        None => "<TOMBSTONE>".into(),
            //    };
            //    println!("    {} {} {}", j, key_versions.ts(j), value_str);
            //}
        }

        assert_eq!(block.get(279, &aa[..]), Some((279, aa_279.clone())));
        assert_eq!(block.get(265, &aa[..]), Some((265, aa_265.clone())));
        assert_eq!(block.get(123, &aa[..]), None);

        assert_eq!(block.get(295, &aa[..]), Some((279, aa_279.clone())));
        assert_eq!(block.get(269, &aa[..]), Some((265, aa_265.clone())));

        assert_eq!(block.get(341, &ab[..]), Some((341, ab_341.clone())));
        assert_eq!(block.get(302, &ab[..]), Some((302, ab_302.clone())));
        assert_eq!(block.get(297, &ab[..]), None);
        assert_eq!(block.get(290, &ab[..]), Some((290, ab_290.clone())));
        assert_eq!(block.get(289, &ab[..]), None);

        assert_eq!(block.get(500, &ab[..]), Some((341, ab_341.clone())));
        assert_eq!(block.get(340, &ab[..]), Some((302, ab_302.clone())));
        assert_eq!(block.get(300, &ab[..]), None);
        assert_eq!(block.get(296, &ab[..]), Some((290, ab_290.clone())));
    }
}

fn hexlify(b: &[u8]) -> String {
    b.iter().map(|b| format!("{:02x}", b)).collect()
}
