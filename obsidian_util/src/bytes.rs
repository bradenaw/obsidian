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
        .count()
}

/// Returns one of the shortest byte strings that lies between a and b in sorted order, if there is
/// one.
///
/// Example:
///   shortest_between(aaaaa, cccccc) -> c
///   shortest_between(aaaaa, aacccc) -> aac
pub fn shortest_between(a: &[u8], b: &[u8]) -> Option<Vec<u8>> {
    if a == b {
        return None;
    }
    if b < a {
        return shortest_between(b, a);
    }

    let n = longest_shared_prefix_len(a, b);

    // Unwrap is fine here because b cannot be a prefix of a.
    let b_next = b.get(n).copied().unwrap();

    let out = if let Some(a_next) = a.get(n).copied() {
        if a_next + 1 == b_next {
            let mut out = a.to_vec();
            out.push(0x7F);
            out
        } else {
            let mut out = a[..n].to_vec();
            out.push((b_next - a_next) / 2 + a_next);
            out
        }
    } else {
        let mut out = a.to_vec();
        if b_next == 0 {
            return None;
        }
        out.push(b_next / 2);
        out
    };

    if !(a < &out[..] && &out[..] < b) {
        panic!("shortest_between failed on {:?} {:?} -> {:?}", a, b, out);
    }
    Some(out)
}

// Returns the number of bytes needed to represent x.
pub fn byte_width(x: u64) -> usize {
    let bits_needed = 64 - x.leading_zeros();
    cmp::max(bits_needed.div_ceil(8) as usize, 1)
}

#[cfg(test)]
mod tests {
    use super::shortest_between;

    #[test]
    fn test_shortest_between() {
        assert_eq!(shortest_between(b"aaaaaa", b"cccccc"), Some(b"b".to_vec()));
        assert_eq!(
            shortest_between(b"aaaaaa", b"aacccc"),
            Some(b"aab".to_vec())
        );
        assert_eq!(shortest_between(&[], &[0xFF]), Some(vec![0x7F]));
        assert_eq!(shortest_between(&[], &[0x01]), Some(vec![0x00]));
        assert_eq!(shortest_between(&[], &[]), None);
        assert_eq!(shortest_between(&[], &[0x00]), None);
        assert_eq!(shortest_between(&[0x00], &[0xFF]), Some(vec![0x7F]));
        assert_eq!(
            shortest_between(&[0x15, 0x52], &[0x15, 0x53]),
            Some(vec![0x15, 0x52, 0x7F])
        );
        assert_eq!(
            shortest_between(&[0x05, 0x80, 0xFF], &[0x05, 0xFF]),
            Some(vec![0x05, 0xBF])
        );
    }
}
