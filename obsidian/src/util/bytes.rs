use std::cmp;
use std::io::Read;
use std::io::Write;

use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

pub(crate) trait Encode {
    fn encoded_size_estimate(&self) -> usize;
    fn encode(&self, w: &mut Vec<u8>);
}

pub(crate) trait Decode: Sized {
    fn decode(b: &[u8]) -> anyhow::Result<Self>;
}

pub(crate) fn encode<E: Encode>(e: &E) -> Vec<u8> {
    let mut v = Vec::with_capacity(e.encoded_size_estimate());
    e.encode(&mut v);
    v
}

pub(crate) fn hexlify(b: &[u8]) -> String {
    b.iter().map(|b| format!("{:02x}", b)).collect()
}

pub(crate) fn longest_shared_prefix(a: &[u8], b: &[u8]) -> Vec<u8> {
    std::iter::zip(a.iter(), b.iter())
        .take_while(|(a, b)| *a == *b)
        .map(|(a, _)| *a)
        .collect()
}

pub(crate) fn longest_shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    std::iter::zip(a.iter(), b.iter())
        .take_while(|(a, b)| *a == *b)
        .map(|(a, _)| *a)
        .count()
}

/// Returns one of the shortest byte strings that lies between a and b in sorted order.
///
/// Example:
///   shortest_between(aaaaa, cccccc) -> c
///   shortest_between(aaaaa, aacccc) -> aac
pub(crate) fn shortest_between(a: &[u8], b: &[u8]) -> Vec<u8> {
    if a == b {
        a.to_vec()
    } else if a < b {
        // +1 is safe here because the only way the shared prefix could be at b.len() is if we
        // should already be in one of the other branches: either a==b or b is a prefix of a, but
        // then b<a.
        b[..longest_shared_prefix_len(a, b) + 1].to_vec()
    } else {
        a[..longest_shared_prefix_len(a, b) + 1].to_vec()
    }
}

// Returns the number of bytes needed to represent x.
pub(crate) fn byte_width(x: u64) -> usize {
    let bits_needed = 64 - x.leading_zeros();
    cmp::max(bits_needed.div_ceil(8) as usize, 1)
}

pub(crate) fn write_varint(b: &mut [u8], mut x: u64) -> usize {
    for i in 0..10 {
        b[i] = (x & 0x7F) as u8;
        x >>= 7;
        if x != 0 {
            b[i] |= 0x80;
        } else {
            return i;
        }
    }
    10
}

pub(crate) fn write_varint_to(mut w: impl Write, mut x: u64) -> std::io::Result<usize> {
    for i in 0..10 {
        let mut b = (x & 0x7F) as u8;
        x >>= 7;
        if x != 0 {
            b |= 0x80;
        }

        w.write_u8(b)?;

        if x == 0 {
            return Ok(i);
        }
    }
    Ok(10)
}

pub(crate) fn read_varint(b: &[u8]) -> anyhow::Result<(u64, usize)> {
    let mut x = 0u64;
    for i in 0..cmp::min(10, b.len()) {
        x <<= 7;
        x |= (b[i] & 0x7F) as u64;
        if b[i] & 0x80 == 0 {
            return Ok((x, i));
        }
    }
    anyhow::bail!("invalid varint");
}

pub(crate) fn read_varint_from(mut r: impl Read) -> anyhow::Result<(u64, usize)> {
    let mut x = 0u64;
    for i in 0..10 {
        let b = r.read_u8()?;
        x <<= 7;
        x |= (b & 0x7F) as u64;
        if b & 0x80 == 0 {
            return Ok((x, i));
        }
    }
    anyhow::bail!("invalid varint");
}
