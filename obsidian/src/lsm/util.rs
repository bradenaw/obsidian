use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::marker::PhantomData;

use byteorder::ByteOrder;
use byteorder::LittleEndian;

use crate::util::binary_search_by_idx;
use crate::util::byte_width;
use crate::util::longest_shared_prefix;

#[derive(Clone)]
pub(crate) struct PrefixCompressedKV<V> {
    v: PhantomData<V>,
    offset_width: usize,
    prefix_len: usize,
    n: usize,
    suffixes_len: usize,
    data: Vec<u8>,
}

const PREFIX_COMPRESSED_KV_HEADER_SIZE: usize = 9;

impl<V: FixedSizeSerializable> PrefixCompressedKV<V> {
    pub(super) fn encode(m: &BTreeMap<Vec<u8>, V>) -> Vec<u8> {
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

    pub(super) fn decode(data: Vec<u8>) -> Self {
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
