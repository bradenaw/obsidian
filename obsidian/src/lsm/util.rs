use std::cmp;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Deref;

use byteorder::ByteOrder;
use byteorder::LittleEndian;

use crate::types::RevisionValue;
use crate::types::Timestamp;
use crate::util::binary_search_by_idx;
use crate::util::byte_width;
use crate::util::hexlify;
use crate::util::longest_shared_prefix;

// Distinct from crate::types::Revision because the internals of the LSM aren't aware of keyspace
// IDs. Here the keys are just Vec<u8>.
#[derive(Clone, Eq, PartialEq)]
pub(super) struct LsmRevision {
    pub key: Vec<u8>,
    pub ts: Timestamp,
    pub value: RevisionValue,
}

impl PartialOrd for LsmRevision {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LsmRevision {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.cmp(&other.key) {
            Ordering::Equal => {}
            ord => return ord,
        }
        self.ts.cmp(&other.ts).reverse()
    }
}

impl Debug for LsmRevision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rev:[{}]@{}:{:?}",
            hexlify(&self.key),
            self.ts,
            self.value
        )
    }
}

#[derive(Clone)]
pub(crate) struct PrefixCompressedKV<B, V> {
    v: PhantomData<V>,
    offset_width: usize,
    prefix_len: usize,
    n: usize,
    suffixes_len: usize,
    data: B,
}

const PREFIX_COMPRESSED_KV_HEADER_SIZE: usize = 9;

impl<B, V: FixedSizeSerializable> PrefixCompressedKV<B, V> {
    pub(super) fn write(out: &mut Vec<u8>, m: &BTreeMap<Vec<u8>, V>) {
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

        out.reserve(header.len() + prefix.len() + offset_and_values.len() + suffixes.len());

        out.extend_from_slice(&header[..]);
        out.extend_from_slice(&prefix[..]);
        out.extend_from_slice(&offset_and_values[..]);
        out.extend_from_slice(&suffixes[..]);
    }
}

impl<B: Deref<Target = [u8]>, V: FixedSizeSerializable> PrefixCompressedKV<B, V> {
    pub(super) fn open(data: B) -> Self {
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

    pub(super) fn len(&self) -> usize {
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

    pub(super) fn search(&self, k: &[u8]) -> Result<usize, usize> {
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

    pub(super) fn get_key(&self, idx: usize) -> Vec<u8> {
        let prefix = self.prefix();
        let suffix = self.get_suffix(idx);
        let mut k = Vec::with_capacity(prefix.len() + suffix.len());
        k.extend_from_slice(prefix);
        k.extend_from_slice(suffix);
        k
    }

    pub(super) fn get_value(&self, idx: usize) -> V {
        let width = self.offset_width + V::size();
        let offset_start = idx * width + self.offset_width;
        let offset_end = offset_start + V::size();
        V::read(&self.offset_and_values()[offset_start..offset_end])
    }
}

pub(super) trait FixedSizeSerializable {
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

/// PackedVec2 encodes a sequence of `u64` such that each element is a fixed width, but each
/// uses only the minumum number of bytes needed to store the largest value.
pub(super) struct PackedVec<B> {
    encoded: B,
    width: usize,
}

impl<B: Deref<Target = [u8]>> PackedVec<B> {
    fn open(encoded: B) -> Self {
        let width = encoded[encoded.len() - 1] as usize;
        Self {
            encoded: encoded,
            width,
        }
    }

    fn write(out: &mut Vec<u8>, v: &Vec<u64>) {
        let mut width = 1;
        for item in v {
            width = cmp::max(width, byte_width(*item));
        }

        out.reserve(v.len() * width + 1);

        for item in v {
            let mut buf = [0u8; 8];
            LittleEndian::write_u64(&mut buf[..], *item);
            out.extend_from_slice(&buf[..width]);
        }

        out.push(width as u8);
    }

    fn get(&self, i: usize) -> u64 {
        let offset = i * self.width;
        let mut b = [0u8; 8];
        b[..self.width].copy_from_slice(&self.encoded[offset..offset + self.width]);
        LittleEndian::read_u64(&b[..])
    }

    fn len(&self) -> usize {
        (self.encoded.len() - 1) / self.width
    }
}

/// PackedVec2 encodes a sequence of `(u64, u64)` such that each element is a fixed width, but each
/// uses only the minumum number of bytes needed to store the largest value.
pub(super) struct PackedVec2<B> {
    encoded: B,
    width_a: usize,
    width_b: usize,
}

impl<B> PackedVec2<B> {
    pub(super) fn write(out: &mut Vec<u8>, v: &Vec<(u64, u64)>) {
        let mut width_a = 1;
        let mut width_b = 1;
        for (a, b) in v {
            width_a = cmp::max(width_a, byte_width(*a));
            width_b = cmp::max(width_b, byte_width(*b));
        }

        out.reserve(v.len() * (width_a + width_b) + 2);

        for (a, b) in v {
            let mut buf = [0u8; 8];
            LittleEndian::write_u64(&mut buf[..], *a);
            out.extend_from_slice(&buf[..width_a]);

            let mut buf = [0u8; 8];
            LittleEndian::write_u64(&mut buf[..], *b);
            out.extend_from_slice(&buf[..width_b]);
        }

        out.push(width_a as u8);
        out.push(width_b as u8);

        println!(
            "len = {}, width_a = {}, width_b = {}",
            v.len(),
            width_a,
            width_b
        );
    }
}

impl<B: Deref<Target = [u8]>> PackedVec2<B> {
    pub(super) fn open(encoded: B) -> Self {
        let width_a = encoded[encoded.len() - 2] as usize;
        let width_b = encoded[encoded.len() - 1] as usize;
        Self {
            encoded: encoded,
            width_a,
            width_b,
        }
    }

    pub(super) fn get(&self, i: usize) -> (u64, u64) {
        let offset_a = i * self.width();
        let offset_b = i * self.width() + self.width_a;

        let mut buf = [0u8; 8];
        buf[..self.width_a].copy_from_slice(&self.encoded[offset_a..offset_a + self.width_a]);
        let a = LittleEndian::read_u64(&buf[..]);

        let mut buf = [0u8; 8];
        buf[..self.width_b].copy_from_slice(&self.encoded[offset_b..offset_b + self.width_b]);
        let b = LittleEndian::read_u64(&buf[..]);

        (a, b)
    }

    pub(super) fn len(&self) -> usize {
        (self.encoded.len() - 2) / (self.width_a + self.width_b)
    }

    pub(super) fn slice<'a>(&'a self, start_idx: usize, end_idx: usize) -> PackedVec2<&'a [u8]> {
        PackedVec2 {
            encoded: &self.encoded[start_idx * self.width()..end_idx * self.width() + 2],
            width_a: self.width_a,
            width_b: self.width_b,
        }
    }

    pub(super) fn borrow<'a>(&'a self) -> PackedVec2<&'a [u8]> {
        PackedVec2 {
            encoded: self.encoded.deref(),
            width_a: self.width_a,
            width_b: self.width_b,
        }
    }

    fn width(&self) -> usize {
        self.width_a + self.width_b
    }
}
