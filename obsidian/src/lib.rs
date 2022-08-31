#![allow(dead_code)]
#![feature(map_first_last)]

use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::collections::BinaryHeap;
use std::marker::PhantomData;
use std::time::SystemTime;

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use byteorder::ByteOrder;
use byteorder::LittleEndian;
use futures::pin_mut;
use futures::stream::Stream;
use futures::stream::StreamExt;
use rand::Rng;
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

struct Block<'a, R> {
    values_len: usize,
    n_versions: usize,
    min_ts: u64,
    ts_bytes: usize,
    offset_bytes: usize,
    index: PrefixCompressedKV<u16>,
    versions_bytes: Vec<u8>,
    header_offset: u64,
    r: &'a R,
}

const BLOCK_INDEX_HEADER_SIZE: usize = 18;

impl<'a, R> Block<'a, R> {
    // Assumes that kvs values are in reverse order by timestamp.
    //
    // Returns the encoded block and the offset of the header within the block.
    pub fn encode(kvs: &BTreeMap<Vec<u8>, Vec<(u64, Value)>>) -> anyhow::Result<(Vec<u8>, usize)> {
        let (min_ts, max_ts, values_len) = kvs
            .values()
            .map(|versions| {
                let min_ts = versions.last()?.0;
                let max_ts = versions.first()?.0;
                let values_len: usize = versions.iter().map(|(_, v)| v.len()).sum();
                Some((min_ts, max_ts, values_len))
            })
            .flatten()
            .reduce(
                |(min_ts0, max_ts0, values_len0), (min_ts1, max_ts1, values_len1)| {
                    (
                        std::cmp::min(min_ts0, min_ts1),
                        std::cmp::max(max_ts0, max_ts1),
                        values_len0 + values_len1,
                    )
                },
            )
            .ok_or_else(|| anyhow!("malformed block"))?;
        // Shift here for room for the tombstone bit.
        let bytes_per_ts_offset = std::cmp::max(byte_width((max_ts - min_ts) << 1), 1);
        let bytes_per_value_offset = std::cmp::max(byte_width(values_len as u64), 1);

        let mut index: BTreeMap<Vec<u8>, u16> = BTreeMap::new();
        let mut block = vec![];

        let mut n_versions = 0;
        let mut versions = Vec::new();
        for (key, key_versions) in kvs {
            index.insert(key.clone(), n_versions as u16);
            for (ts, value) in key_versions {
                let value_offset = block.len();
                let tombstone_bit = match value {
                    Value::Regular(value) => {
                        block.extend_from_slice(&value[..]);
                        0
                    }
                    Value::Tombstone => 1,
                };
                let ts_offset_and_tombstone = ((ts - min_ts) << 1) | tombstone_bit;

                let mut buf = [0u8; 16];
                LittleEndian::write_u64(&mut buf[..], ts_offset_and_tombstone);
                LittleEndian::write_u64(&mut buf[bytes_per_ts_offset..], value_offset as u64);
                versions.extend_from_slice(&buf[..bytes_per_ts_offset + bytes_per_value_offset]);
            }
            n_versions += key_versions.len();
        }
        let values_len = block.len();
        let encoded_index = PrefixCompressedKV::encode(&index);

        let mut header = [0u8; BLOCK_INDEX_HEADER_SIZE];
        LittleEndian::write_u32(&mut header[0..4], values_len as u32);
        LittleEndian::write_u16(&mut header[4..6], n_versions as u16);
        LittleEndian::write_u64(&mut header[6..14], min_ts);
        LittleEndian::write_u16(&mut header[14..16], encoded_index.len() as u16);
        header[16] = bytes_per_ts_offset as u8;
        header[17] = bytes_per_value_offset as u8;

        let header_idx = block.len();
        block.extend_from_slice(&header[..]);
        block.extend_from_slice(&encoded_index[..]);
        block.extend_from_slice(&versions[..]);

        Ok((block, header_idx))
    }
}

impl<'a, R: AsyncReadExactAt> Block<'a, R> {
    pub async fn open(r: &'a R, header_offset: u64) -> anyhow::Result<Block<'a, R>> {
        let mut header = [0u8; BLOCK_INDEX_HEADER_SIZE];

        r.read_exact_at(&mut header[..], header_offset).await?;

        let values_len = LittleEndian::read_u32(&header[0..4]) as usize;
        let n_versions = LittleEndian::read_u16(&header[4..6]) as usize;
        let min_ts = LittleEndian::read_u64(&header[6..14]);
        let index_len = LittleEndian::read_u16(&header[14..16]) as usize;
        let bytes_per_ts_offset = header[16] as usize;
        let bytes_per_value_offset = header[17] as usize;
        let versions_len = n_versions * (bytes_per_ts_offset + bytes_per_value_offset);

        let mut index_bytes = vec![0u8; index_len];
        r.read_exact_at(
            &mut index_bytes[..],
            header_offset + (BLOCK_INDEX_HEADER_SIZE as u64),
        )
        .await?;
        let index = PrefixCompressedKV::decode(index_bytes);

        let mut versions_bytes = vec![0u8; versions_len];
        r.read_exact_at(
            &mut versions_bytes[..],
            header_offset + (BLOCK_INDEX_HEADER_SIZE as u64) + (index_len as u64),
        )
        .await?;

        Ok(Self {
            r,
            values_len,
            n_versions,
            min_ts,
            index,
            versions_bytes,
            header_offset,
            ts_bytes: bytes_per_ts_offset,
            offset_bytes: bytes_per_value_offset,
        })
    }

    fn versions(&self) -> BlockVersions<'_> {
        BlockVersions {
            ts_bytes: self.ts_bytes,
            offset_bytes: self.offset_bytes,
            min_ts: self.min_ts,
            end_offset: self.values_len,
            b: &self.versions_bytes[..],
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

    async fn value<'b>(
        &'b self,
        versions: &BlockVersions<'b>,
        idx: usize,
    ) -> anyhow::Result<Value> {
        let (value_start, value_end) = match versions.value_offsets(idx) {
            Some(v) => v,
            None => return Ok(Value::Tombstone),
        };
        let value_len = value_end - value_start;

        let mut value = vec![0u8; value_len];
        self.r
            .read_exact_at(
                &mut value[..],
                self.header_offset - (self.values_len as u64) + (value_start as u64),
            )
            .await?;

        Ok(Value::Regular(value))
    }

    pub async fn get(&self, ts: u64, k: &[u8]) -> anyhow::Result<Option<(u64, Vec<u8>)>> {
        let key_idx = match self.index.search(k) {
            Ok(idx) => idx,
            Err(_) => return Ok(None),
        };
        let key_versions = self.versions_for_key(key_idx);

        let version_idx = binary_search_by_idx(key_versions.len(), Reverse(ts), |idx| {
            Reverse(key_versions.ts(idx))
        })
        .unwrap_or_else(core::convert::identity);
        if version_idx == key_versions.len() {
            return Ok(None);
        }
        let record_ts = key_versions.ts(version_idx);

        Ok(match self.value(&key_versions, version_idx).await? {
            Value::Regular(v) => Some((record_ts, v)),
            Value::Tombstone => None,
        })
    }
}

struct BlockVersions<'a> {
    ts_bytes: usize,
    offset_bytes: usize,
    min_ts: u64,
    end_offset: usize,
    b: &'a [u8],
}

impl<'a> BlockVersions<'a> {
    fn len(&self) -> usize {
        self.b.len() / (self.ts_bytes + self.offset_bytes)
    }

    fn elem(&self, idx: usize) -> (u64, bool, usize) {
        let width = self.ts_bytes + self.offset_bytes;
        let elem = &self.b[width * idx..width * (idx + 1)];
        let mut ts_offset_buf = [0u8; 8];
        ts_offset_buf[..self.ts_bytes].copy_from_slice(&elem[..self.ts_bytes]);
        let ts_offset_and_tombstone = LittleEndian::read_u64(&ts_offset_buf[..]);
        let tombstone = ts_offset_and_tombstone & 1 == 1;
        let ts = (ts_offset_and_tombstone >> 1) + self.min_ts;

        let mut value_offset_buf = [0u8; 8];
        value_offset_buf[..self.offset_bytes].copy_from_slice(&elem[self.ts_bytes..]);
        let value_offset = LittleEndian::read_u64(&value_offset_buf[..]) as usize;
        (ts, tombstone, value_offset)
    }

    fn slice(&self, start_idx: usize, end_idx: usize) -> BlockVersions<'a> {
        let width = self.ts_bytes + self.offset_bytes;
        let b = &self.b[start_idx * width..end_idx * width];
        let end_offset = if end_idx == self.len() {
            self.end_offset
        } else {
            self.elem(end_idx).2
        };
        BlockVersions {
            ts_bytes: self.ts_bytes,
            offset_bytes: self.offset_bytes,
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

#[derive(Clone)]
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

        let offset_width = std::cmp::max(byte_width(suffixes_len as u64), 1);

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

const INDEX_BLOCK_HEADER_SIZE: usize = 28;
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
        ) -> anyhow::Result<()> {
            let (first_key, last_key_) = match (buffer.first_key_value(), buffer.last_key_value()) {
                (Some((first_key, _)), Some((last_key, _))) => (first_key, last_key),
                _ => anyhow::bail!("empty block"),
            };
            *last_key = last_key_.clone();

            let (block, header_offset_in_block) = Block::<()>::encode(buffer)?;
            w.write_all(&block[..]).await?;
            let header_offset_in_file = *bytes_written + header_offset_in_block;

            index.insert(first_key.clone(), header_offset_in_file as u32);

            *bytes_written += block.len();

            Ok(())
        }

        pin_mut!(s);

        let mut buffer: BTreeMap<Vec<u8>, Vec<(u64, Value)>> = BTreeMap::new();
        let mut bytes_written = 0;
        let mut buffer_size = BLOCK_INDEX_HEADER_SIZE;
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
                && buffer_size + record_size > BLOCK_SIZE_LIMIT
                && !buffer.contains_key(&record.key)
            {
                flush(w, &mut bytes_written, &mut index, &mut last_key, &buffer).await?;
                buffer.clear();
                buffer_size = 0;
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
            flush(w, &mut bytes_written, &mut index, &mut last_key, &buffer).await?;
        }

        let index_compressed = PrefixCompressedKV::encode(&index);

        let index_block_offset = bytes_written;
        let mut header = [0u8; INDEX_BLOCK_HEADER_SIZE];
        LittleEndian::write_u32(&mut header[0..4], keyspace_id);
        LittleEndian::write_u64(&mut header[4..12], min_ts);
        LittleEndian::write_u64(&mut header[12..20], max_ts);
        LittleEndian::write_u32(&mut header[20..24], last_key.len() as u32);
        LittleEndian::write_u32(&mut header[24..28], index_compressed.len() as u32);
        w.write_all(&header[..]).await?;
        w.write_all(&last_key[..]).await?;
        w.write_all(&index_compressed).await?;

        let mut index_block_offset_buf = [0u8; 4];
        LittleEndian::write_u32(&mut index_block_offset_buf[..], index_block_offset as u32);
        w.write_all(&index_block_offset_buf[..]).await?;

        Ok(())
    }
}

impl<R: AsyncReadExactAt> RunFile<R> {
    async fn open(r: R, size: usize) -> anyhow::Result<Self> {
        let file_len = r.len().await?;
        let mut index_block_offset_buf = [0u8; 4];
        r.read_exact_at(&mut index_block_offset_buf[..], file_len - 4)
            .await?;
        let index_block_offset = LittleEndian::read_u32(&index_block_offset_buf[..]);

        let mut header = [0u8; INDEX_BLOCK_HEADER_SIZE];
        r.read_exact_at(&mut header[..], index_block_offset as u64)
            .await?;

        let keyspace_id = LittleEndian::read_u32(&header[0..4]);
        let min_ts = LittleEndian::read_u64(&header[4..12]);
        let max_ts = LittleEndian::read_u64(&header[12..20]);
        let max_key_len = LittleEndian::read_u32(&header[20..24]);
        let index_len = LittleEndian::read_u32(&header[24..28]);

        let max_key = {
            let mut max_key = vec![0u8; max_key_len as usize];
            r.read_exact_at(
                &mut max_key[..],
                (index_block_offset as u64) + (header.len() as u64),
            )
            .await?;
            max_key
        };

        let index = {
            let mut index_bytes = vec![0u8; index_len as usize];
            r.read_exact_at(
                &mut index_bytes[..],
                (index_block_offset as u64) + (header.len() as u64) + (max_key_len as u64),
            )
            .await?;
            PrefixCompressedKV::decode(index_bytes)
        };

        let min_key = index.get_key(0);

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

    async fn get(&self, ts: u64, k: &[u8]) -> anyhow::Result<Option<(u64, Vec<u8>)>> {
        let block_header_idx = match self.index.search(k) {
            Ok(idx) => idx,
            Err(idx) => {
                if idx == 0 {
                    return Ok(None);
                }
                idx - 1
            }
        };
        let block_header_offset = self.index.get_value(block_header_idx);
        let block = Block::open(&self.r, block_header_offset as u64).await?;
        block.get(ts, k).await
    }

    fn range(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        Some((self.min_key.clone(), self.max_key.clone()))
    }

    fn stream(&self) -> impl Stream<Item = anyhow::Result<Record>> + '_ {
        try_stream! {
            for i in 0..self.index.len() {
                let block_header_offset = self.index.get_value(i);
                let block = Block::open(&self.r, block_header_offset as u64).await?;
                for j in 0..block.index.len() {
                    let key = block.index.get_key(j);
                    let versions = block.versions_for_key(j);
                    for k in 0..versions.len() {
                        let ts = versions.ts(k);
                        let value = block.value(&versions, k).await?;
                        yield Record{key: key.clone(), ts, value};
                    }
                }
            }
        }
    }
}

#[async_trait]
trait AsyncReadExactAt {
    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<()>;
    async fn len(&self) -> anyhow::Result<u64>;
}

#[async_trait]
impl AsyncReadExactAt for Vec<u8> {
    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<()> {
        Ok(buf.copy_from_slice(&self[(offset as usize)..(offset as usize) + buf.len()]))
    }
    async fn len(&self) -> anyhow::Result<u64> {
        Ok(self.len() as u64)
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

// Returns the number of bytes needed to represent x.
fn byte_width(x: u64) -> usize {
    let bits_needed = 64 - x.leading_zeros();
    ((bits_needed + 7) / 8) as usize
}

#[cfg(test)]
mod test {
    use std::cmp::Reverse;
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use crate::binary_search_by_idx;
    use crate::hexlify;
    use crate::AsyncReadExactAt;
    use crate::Block;
    use crate::Lsm;
    use crate::Record;
    use crate::Run;
    use crate::RunFile;
    use crate::Value;
    use crate::BLOCK_INDEX_HEADER_SIZE;

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

    #[tokio::test]
    async fn test_block() -> anyhow::Result<()> {
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
        let (encoded, header_offset) = Block::<()>::encode(&kvs)?;

        let block = Block::open(&encoded, header_offset as u64).await?;

        println!("encoded  = {}", hexlify(&encoded[..]));
        println!(
            "header   = {}",
            hexlify(&encoded[header_offset..header_offset + BLOCK_INDEX_HEADER_SIZE])
        );
        // println!(
        //     "index    = {}",
        //     hexlify(
        //         &encoded[header_offset + BLOCK_INDEX_HEADER_SIZE
        //             ..header_offset + BLOCK_INDEX_HEADER_SIZE + block.index_len]
        //     ),
        // );
        println!("versions = {}", hexlify(&block.versions_bytes));
        println!("prefix = {}", hexlify(block.index.prefix()));
        println!("n_versions = {}", block.n_versions);
        let versions = block.versions();
        for i in 0..block.n_versions {
            let value_str = match versions.value_offsets(i) {
                Some((value_start, value_end)) => format!("({}, {})", value_start, value_end),
                None => "<TOMBSTONE>".into(),
            };
            println!("  {} {} {}", i, versions.ts(i), value_str);
        }
        //println!("n_keys = {}", block.index.len());
        //for i in 0..block.index.len() {
        //    println!(
        //        "  {} {} {}",
        //        i,
        //        hexlify(&block.index.get_key(i)),
        //        block.index.get_value(i)
        //    );
        //    //let key_versions = block.versions_for_key(i);
        //    //println!(
        //    //    "  {} {}  {} versions",
        //    //    i,
        //    //    hexlify(&block.index.get_key(i)),
        //    //    key_versions.len()
        //    //);
        //    //for j in 0..key_versions.len() {
        //    //    let value_str = match key_versions.value_offsets(j) {
        //    //        Some((value_start, value_end)) => format!("({}, {})", value_start, value_end),
        //    //        None => "<TOMBSTONE>".into(),
        //    //    };
        //    //    println!("    {} {} {}", j, key_versions.ts(j), value_str);
        //    //}
        //}

        assert_eq!(block.get(279, &aa[..]).await?, Some((279, aa_279.clone())));
        assert_eq!(block.get(265, &aa[..]).await?, Some((265, aa_265.clone())));
        assert_eq!(block.get(123, &aa[..]).await?, None);

        assert_eq!(block.get(295, &aa[..]).await?, Some((279, aa_279.clone())));
        assert_eq!(block.get(269, &aa[..]).await?, Some((265, aa_265.clone())));

        assert_eq!(block.get(341, &ab[..]).await?, Some((341, ab_341.clone())));
        assert_eq!(block.get(302, &ab[..]).await?, Some((302, ab_302.clone())));
        assert_eq!(block.get(297, &ab[..]).await?, None);
        assert_eq!(block.get(290, &ab[..]).await?, Some((290, ab_290.clone())));
        assert_eq!(block.get(289, &ab[..]).await?, None);

        assert_eq!(block.get(500, &ab[..]).await?, Some((341, ab_341.clone())));
        assert_eq!(block.get(340, &ab[..]).await?, Some((302, ab_302.clone())));
        assert_eq!(block.get(300, &ab[..]).await?, None);
        assert_eq!(block.get(296, &ab[..]).await?, Some((290, ab_290.clone())));

        Ok(())
    }

    #[tokio::test]
    async fn test_run_file() -> anyhow::Result<()> {
        fn rand_bytes(n: usize) -> Vec<u8> {
            let mut out = vec![0u8; n];
            rand::thread_rng().fill_bytes(&mut out);
            out
        }
        let records = vec![
            Record {
                key: b"prefixbar".to_vec(),
                ts: 20101,
                value: Value::Regular(rand_bytes(10_000)),
            },
            Record {
                key: b"prefixbar".to_vec(),
                ts: 19230,
                value: Value::Tombstone,
            },
            Record {
                key: b"prefixbar".to_vec(),
                ts: 10230,
                value: Value::Regular(rand_bytes(128)),
            },
            Record {
                key: b"prefixfoo".to_vec(),
                ts: 21925,
                value: Value::Regular(rand_bytes(10_000)),
            },
            Record {
                key: b"prefixfoo".to_vec(),
                ts: 12031,
                value: Value::Regular(rand_bytes(10_000)),
            },
        ];
        let mut v = vec![];
        RunFile::<()>::write(
            &mut v,
            1,
            futures::stream::iter(records.iter().map(|record| Ok(record.clone()))),
        )
        .await
        .unwrap();

        let v_len = v.len();
        let run = RunFile::open(v, v_len).await?;

        assert_eq!(run.min_ts, 10230);
        assert_eq!(run.max_ts, 21925);
        assert_eq!(run.min_key, b"prefixbar".to_vec());
        assert_eq!(run.max_key, b"prefixfoo".to_vec());

        for record in records {
            assert_eq!(
                run.get(record.ts, &record.key).await?,
                match record.value {
                    Value::Regular(v) => Some((record.ts, v)),
                    Value::Tombstone => None,
                }
            );
        }

        Ok(())
    }

    proptest! {
        #[test]
        fn proptest_run_file(m in proptest::collection::btree_map(
            (proptest::collection::vec(u8::arbitrary(), 0..2), 0..(1u64 << 63)),
            proptest::option::of(proptest::collection::vec(u8::arbitrary(), 0..128)),
            1..4096,
        )) {
            let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();

            rt.block_on(async {
                let mut records = m.into_iter().map(|((key, ts), maybe_value)| Record{
                    key, ts, value: match maybe_value {
                        Some(v) => Value::Regular(v),
                        None => Value::Tombstone,
                    },
                }).collect::<Vec<Record>>();
                records.sort_by_key(|record| (record.key.clone(), Reverse(record.ts)));

                let mut v = vec![];
                RunFile::<()>::write(
                    &mut v,
                    1,
                    futures::stream::iter(records.iter().map(|record| Ok(record.clone()))),
                ).await.unwrap();

                let v_len = v.len();
                let run = RunFile::open(v, v_len).await.unwrap();

                dump_run_file(&run).await.unwrap();

                for record in records {
                    println!("get({}, [{}])", record.ts, hexlify(&record.key[..]));
                    assert_eq!(run.get(record.ts, &record.key[..]).await.unwrap(), match record.value {
                        Value::Regular(value) => Some((record.ts, value)),
                        Value::Tombstone => None,
                    });
                }
            });
        }
    }

    async fn dump_run_file<R: AsyncReadExactAt>(run: &RunFile<R>) -> anyhow::Result<()> {
        println!("min_ts: {}", run.min_ts);
        println!("max_ts: {}", run.max_ts);
        println!("index");
        println!("prefix: [{}]", hexlify(run.index.prefix()));
        for i in 0..run.index.len() {
            println!(
                "  {} header offset {}",
                hexlify(&run.index.get_key(i)),
                run.index.get_value(i)
            );
        }
        println!("blocks");
        for i in 0..run.index.len() {
            println!("== block {} ======", i);
            println!("first key: [{}]", hexlify(&run.index.get_key(i)),);
            println!("header_offset: {}", run.index.get_value(i));
            let header_offset = run.index.get_value(i);
            let block = Block::open(&run.r, header_offset as u64).await?;
            dump_block(&block).await?;
        }
        Ok(())
    }
    async fn dump_block<'a, R: AsyncReadExactAt>(block: &Block<'a, R>) -> anyhow::Result<()> {
        println!("prefix: {}", hexlify(block.index.prefix()));
        println!("n_keys: {}", block.index.len());
        println!("n_versions: {}", block.n_versions);
        println!("values_len: {}", block.values_len);
        println!("  == keys ======");
        for i in 0..block.index.len() {
            let key = block.index.get_key(i);
            let versions_offset = block.index.get_value(i);
            println!("    [{}] {}", hexlify(&key), versions_offset);
        }
        let versions = block.versions();
        println!("  == versions ======");
        for i in 0..block.n_versions {
            let value_str = match versions.value_offsets(i) {
                Some((value_start, value_end)) => format!("({}, {})", value_start, value_end),
                None => "<TOMBSTONE>".into(),
            };
            println!("    {} {} {}", i, versions.ts(i), value_str);
        }
        Ok(())
    }
}

fn hexlify(b: &[u8]) -> String {
    b.iter().map(|b| format!("{:02x}", b)).collect()
}
