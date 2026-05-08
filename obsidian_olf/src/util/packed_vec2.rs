use std::cmp;
use std::ops::Deref;

use anyhow::anyhow;
use byteorder::ByteOrder;
use byteorder::LittleEndian;
use obsidian_util::byte_width;

/// PackedVec2 encodes a sequence of `(u64, u64)` such that each element is a fixed width, but each
/// uses only the minumum number of bytes needed to store the largest value.
pub(crate) struct PackedVec2<B> {
    encoded: B,
    width_a: usize,
    width_b: usize,
}

impl<B> PackedVec2<B> {
    pub fn write(out: &mut Vec<u8>, v: &[(u64, u64)]) {
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
    pub fn open(encoded: B) -> anyhow::Result<Self> {
        if encoded.len() < 2 {
            return Err(anyhow!("PackedVec2 too short: {} < {}", encoded.len(), 2));
        }
        let width_a = encoded[encoded.len() - 2] as usize;
        let width_b = encoded[encoded.len() - 1] as usize;
        Ok(Self {
            encoded,
            width_a,
            width_b,
        })
    }

    pub fn get(&self, i: usize) -> (u64, u64) {
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

    pub fn len(&self) -> usize {
        (self.encoded.len() - 2) / (self.width_a + self.width_b)
    }

    pub fn slice(&self, start_idx: usize, end_idx: usize) -> PackedVec2<&[u8]> {
        PackedVec2 {
            encoded: &self.encoded[start_idx * self.width()..end_idx * self.width() + 2],
            width_a: self.width_a,
            width_b: self.width_b,
        }
    }

    pub fn borrow(&self) -> PackedVec2<&[u8]> {
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
