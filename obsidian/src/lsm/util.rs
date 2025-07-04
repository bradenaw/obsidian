use std::cmp;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::Deref;

use anyhow::anyhow;
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

/// `PrefixCompressedKV` is like a `BTreeMap<Vec<u8>, u64>`, but packed much more tightly. Any
/// prefix shared by all of the keys is only encoded once, and then we're left with a map from
/// suffixes to values. Each value also only takes up as much space as the byte width of the
/// largest value, so even though the values are constrained to u64 here, if all of the values fit
/// into a u16 that's all that'll be used.
#[derive(Clone)]
pub(crate) struct PrefixCompressedKV<B> {
    n: usize,
    prefix_len: usize,
    suffixes_len: usize,
    data: B,
}

const PREFIX_COMPRESSED_KV_HEADER_SIZE: usize = 8;

impl<B> PrefixCompressedKV<B> {
    pub(super) fn write(out: &mut Vec<u8>, m: &BTreeMap<Vec<u8>, u64>) {
        let prefix: Vec<u8> = match (m.first_key_value(), m.last_key_value()) {
            (Some((first_key, _)), Some((last_key, _))) => {
                longest_shared_prefix(&first_key[..], &last_key[..])
            }
            _ => vec![],
        };
        let suffixes_len: usize = m.keys().map(|k| k.len() - prefix.len()).sum();

        let mut suffixes = Vec::with_capacity(suffixes_len);

        let mut suffix_offsets_and_values = Vec::with_capacity(m.len());

        for (k, v) in m {
            let offset = suffixes.len();

            suffixes.extend_from_slice(&k[prefix.len()..]);

            suffix_offsets_and_values.push((offset as u64, *v));
        }

        let mut header = [0u8; PREFIX_COMPRESSED_KV_HEADER_SIZE];
        LittleEndian::write_u16(&mut header[0..2], m.len() as u16);
        LittleEndian::write_u16(&mut header[2..4], prefix.len() as u16);
        LittleEndian::write_u32(&mut header[4..8], suffixes.len() as u32);

        out.reserve(header.len() + prefix.len() + suffixes.len());
        out.extend_from_slice(&header[..]);
        out.extend_from_slice(&prefix[..]);
        out.extend_from_slice(&suffixes[..]);

        PackedVec2::<()>::write(out, &suffix_offsets_and_values[..]);
    }
}

impl<B: Deref<Target = [u8]>> PrefixCompressedKV<B> {
    pub(super) fn open(data: B) -> anyhow::Result<Self> {
        let header = &data[0..PREFIX_COMPRESSED_KV_HEADER_SIZE];
        let n = LittleEndian::read_u16(&header[0..2]) as usize;
        let prefix_len = LittleEndian::read_u16(&header[2..4]) as usize;
        let suffixes_len = LittleEndian::read_u32(&header[4..8]) as usize;

        // So we can just unwrap() it later.
        _ = PackedVec2::open(
            &data[PREFIX_COMPRESSED_KV_HEADER_SIZE + prefix_len + suffixes_len..],
        )?;

        Ok(Self {
            n,
            prefix_len,
            suffixes_len,
            data,
        })
    }

    pub(super) fn len(&self) -> usize {
        self.n
    }

    fn prefix(&self) -> &[u8] {
        &self.data
            [PREFIX_COMPRESSED_KV_HEADER_SIZE..PREFIX_COMPRESSED_KV_HEADER_SIZE + self.prefix_len]
    }

    fn suffixes(&self) -> &[u8] {
        let start = PREFIX_COMPRESSED_KV_HEADER_SIZE + self.prefix_len;
        &self.data[start..start + self.suffixes_len]
    }

    fn suffix_offset_and_values(&self) -> PackedVec2<&'_ [u8]> {
        let start = PREFIX_COMPRESSED_KV_HEADER_SIZE + self.prefix_len + self.suffixes_len;
        PackedVec2::open(&self.data[start..]).unwrap()
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
        self.suffix_offset_and_values().get(idx).0 as usize
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

    pub(super) fn get_value(&self, idx: usize) -> u64 {
        self.suffix_offset_and_values().get(idx).1
    }
}

/// PackedVec2 encodes a sequence of `u64` such that each element is a fixed width, but each
/// uses only the minumum number of bytes needed to store the largest value.
pub(super) struct PackedVec<B> {
    encoded: B,
    width: usize,
}

impl<B: Deref<Target = [u8]>> PackedVec<B> {
    fn open(encoded: B) -> anyhow::Result<Self> {
        if encoded.len() < 1 {
            return Err(anyhow!("PackedVec too short: {} < {}", encoded.len(), 1));
        }
        let width = encoded[encoded.len() - 1] as usize;
        Ok(Self {
            encoded: encoded,
            width,
        })
    }

    fn write(out: &mut Vec<u8>, v: &[u64]) {
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
    pub(super) fn write(out: &mut Vec<u8>, v: &[(u64, u64)]) {
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
    }
}

impl<B: Deref<Target = [u8]>> PackedVec2<B> {
    pub(super) fn open(encoded: B) -> anyhow::Result<Self> {
        if encoded.len() < 2 {
            return Err(anyhow!("PackedVec2 too short: {} < {}", encoded.len(), 2));
        }
        let width_a = encoded[encoded.len() - 2] as usize;
        let width_b = encoded[encoded.len() - 1] as usize;
        Ok(Self {
            encoded: encoded,
            width_a,
            width_b,
        })
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
