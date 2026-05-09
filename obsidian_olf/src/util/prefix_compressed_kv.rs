use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Deref;

use byteorder::ByteOrder;
use byteorder::LittleEndian;
use obsidian_util::binary_search_by_idx;
use obsidian_util::longest_shared_prefix;

use crate::util::PackedVec2;

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
    pub fn write(out: &mut Vec<u8>, m: &BTreeMap<Vec<u8>, u64>) {
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
    pub fn open(data: B) -> anyhow::Result<Self> {
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

    pub fn len(&self) -> usize {
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

    pub fn search(&self, k: &[u8]) -> Result<usize, usize> {
        let prefix = self.prefix();
        if !k.starts_with(prefix) {
            match k.cmp(prefix) {
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

    pub fn get_key(&self, idx: usize) -> Vec<u8> {
        let prefix = self.prefix();
        let suffix = self.get_suffix(idx);
        let mut k = Vec::with_capacity(prefix.len() + suffix.len());
        k.extend_from_slice(prefix);
        k.extend_from_slice(suffix);
        k
    }

    pub fn get_value(&self, idx: usize) -> u64 {
        self.suffix_offset_and_values().get(idx).1
    }
}
