use std::cmp;
use std::fmt::Display;

pub trait Encode {
    fn encoded_size_estimate(&self) -> usize;
    fn encode(&self, w: &mut Vec<u8>);
}

pub trait Decode: Sized {
    fn decode(b: &[u8]) -> anyhow::Result<Self>;
}

pub fn encode<E: Encode>(e: &E) -> Vec<u8> {
    let mut v = Vec::with_capacity(e.encoded_size_estimate());
    e.encode(&mut v);
    v
}

pub fn hexlify(b: &[u8]) -> Hex<'_> {
    Hex { b }
}

pub struct Hex<'a> {
    b: &'a [u8],
}

impl<'a> Display for Hex<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.b {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

pub fn longest_shared_prefix(a: &[u8], b: &[u8]) -> Vec<u8> {
    std::iter::zip(a.iter(), b.iter())
        .take_while(|(a, b)| *a == *b)
        .map(|(a, _)| *a)
        .collect()
}

pub fn longest_shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
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
pub fn shortest_between(a: &[u8], b: &[u8]) -> Vec<u8> {
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
pub fn byte_width(x: u64) -> usize {
    let bits_needed = 64 - x.leading_zeros();
    cmp::max(bits_needed.div_ceil(8) as usize, 1)
}
