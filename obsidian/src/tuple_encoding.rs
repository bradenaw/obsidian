use std::cmp;

use anyhow::anyhow;
use byteorder::BigEndian;
use byteorder::ByteOrder;
use byteorder::WriteBytesExt;

trait Element: Sized {
    fn encoded_size(&self) -> usize;
    fn encode(&self, w: &mut [u8]);
    fn decode(b: &[u8]) -> anyhow::Result<(Self, usize)>;
}

impl Element for u64 {
    fn encoded_size(&self) -> usize {
        let n_bits = 64 - self.leading_zeros();
        if n_bits == 0 {
            return 1;
        } else if n_bits > 56 {
            return 9;
        }
        ((n_bits as usize) + 6) / 7
    }

    fn encode(&self, w: &mut [u8]) {
        let n_bits = 64 - self.leading_zeros();
        match n_bits {
            0..=7 => {
                w[0] = *self as u8;
            }
            8..=14 => {
                w[0] = 0b10_000000 | ((*self >> 8) as u8);
                w[1] = *self as u8;
            }
            15..=21 => {
                w[0] = 0b110_00000 | ((*self >> 16) as u8);
                w[1] = (*self >> 8) as u8;
                w[2] = *self as u8;
            }
            22..=28 => {
                w[0] = 0b1110_0000 | ((*self >> 24) as u8);
                w[1] = (*self >> 16) as u8;
                w[2] = (*self >> 8) as u8;
                w[3] = *self as u8;
            }
            29..=35 => {
                w[0] = 0b11110_000 | ((*self >> 32) as u8);
                w[1] = (*self >> 24) as u8;
                w[2] = (*self >> 16) as u8;
                w[3] = (*self >> 8) as u8;
                w[4] = *self as u8;
            }
            36..=42 => {
                w[0] = 0b111110_00 | ((*self >> 40) as u8);
                w[1] = (*self >> 32) as u8;
                w[2] = (*self >> 24) as u8;
                w[3] = (*self >> 16) as u8;
                w[4] = (*self >> 8) as u8;
                w[5] = *self as u8;
            }
            43..=49 => {
                w[0] = 0b1111110_0 | ((*self >> 48) as u8);
                w[1] = (*self >> 40) as u8;
                w[2] = (*self >> 32) as u8;
                w[3] = (*self >> 24) as u8;
                w[4] = (*self >> 16) as u8;
                w[5] = (*self >> 8) as u8;
                w[6] = *self as u8;
            }
            50..=56 => {
                w[0] = 0b11111110;
                w[1] = (*self >> 48) as u8;
                w[2] = (*self >> 40) as u8;
                w[3] = (*self >> 32) as u8;
                w[4] = (*self >> 24) as u8;
                w[5] = (*self >> 16) as u8;
                w[6] = (*self >> 8) as u8;
                w[7] = *self as u8;
            }
            _ => {
                w[0] = 0b11111111;
                w[1] = (*self >> 56) as u8;
                w[2] = (*self >> 48) as u8;
                w[3] = (*self >> 40) as u8;
                w[4] = (*self >> 32) as u8;
                w[5] = (*self >> 24) as u8;
                w[6] = (*self >> 16) as u8;
                w[7] = (*self >> 8) as u8;
                w[8] = *self as u8;
            }
        }
    }

    fn decode(b: &[u8]) -> anyhow::Result<(Self, usize)> {
        if b[0] & 0b10000000 == 0 {
            return Ok((b[0] as u64, 1));
        }
        match b[0].leading_ones() {
            1 => {
                return Ok(((((b[0] & 0b00_111111) as u64) << 8) | (b[1] as u64), 2));
            }
            2 => {
                return Ok((
                    (((b[0] & 0b000_11111) as u64) << 16) | ((b[1] as u64) << 8) | (b[2] as u64),
                    3,
                ));
            }
            3 => {
                return Ok((
                    (((b[0] & 0b0000_1111) as u64) << 24)
                        | ((b[1] as u64) << 16)
                        | ((b[2] as u64) << 8)
                        | (b[3] as u64),
                    4,
                ));
            }
            4 => {
                return Ok((
                    (((b[0] & 0b00000_111) as u64) << 32)
                        | ((b[1] as u64) << 24)
                        | ((b[2] as u64) << 16)
                        | ((b[3] as u64) << 8)
                        | (b[4] as u64),
                    5,
                ));
            }
            5 => {
                return Ok((
                    (((b[0] & 0b000000_11) as u64) << 40)
                        | ((b[1] as u64) << 32)
                        | ((b[2] as u64) << 24)
                        | ((b[3] as u64) << 16)
                        | ((b[4] as u64) << 8)
                        | (b[5] as u64),
                    6,
                ));
            }
            6 => {
                return Ok((
                    (((b[0] & 0b0000000_1) as u64) << 48)
                        | ((b[1] as u64) << 40)
                        | ((b[2] as u64) << 32)
                        | ((b[3] as u64) << 24)
                        | ((b[4] as u64) << 16)
                        | ((b[5] as u64) << 8)
                        | (b[6] as u64),
                    7,
                ));
            }
            7 => {
                return Ok((
                    ((b[1] as u64) << 48)
                        | ((b[2] as u64) << 40)
                        | ((b[3] as u64) << 32)
                        | ((b[4] as u64) << 24)
                        | ((b[5] as u64) << 16)
                        | ((b[6] as u64) << 8)
                        | (b[7] as u64),
                    8,
                ));
            }
            8 => {
                return Ok((
                    ((b[1] as u64) << 56)
                        | ((b[2] as u64) << 48)
                        | ((b[3] as u64) << 40)
                        | ((b[4] as u64) << 32)
                        | ((b[5] as u64) << 24)
                        | ((b[6] as u64) << 16)
                        | ((b[7] as u64) << 8)
                        | (b[8] as u64),
                    9,
                ));
            }
            _ => {
                return Err(anyhow!("invalid first byte for u64 0x{:02x}", b[0]));
            }
        }
    }
}

impl Element for Vec<u8> {
    fn encoded_size(&self) -> usize {
        cmp::max((self.len() * 8 + 6) / 7, 1)
    }

    fn encode(&self, w: &mut [u8]) {
        let mut reg = 0u32;
        let mut reg_bits = 0;
        let mut out_i = 0;
        for b in self {
            reg = (reg << 8) | (*b as u32);
            reg_bits += 8;

            while reg_bits >= 7 {
                let front7 = reg >> (reg_bits - 7);
                w[out_i] = ((front7 << 1) as u8) | 1;
                reg_bits -= 7;
                out_i += 1;
            }
        }
        if reg_bits > 0 {
            w[out_i] = (reg << (8 - reg_bits)) as u8;
        }
        w[w.len() - 1] &= 0b11111110
    }

    fn decode(encoded: &[u8]) -> anyhow::Result<(Self, usize)> {
        let mut reg = 0u32;
        let mut reg_bits = 0;
        let mut out = Vec::with_capacity((encoded.len() * 7 + 7) / 8);
        let mut found_end = false;
        let mut i = 0;
        for b in encoded.iter() {
            i += 1;
            reg = reg << 7 | ((*b >> 1) as u32);
            reg_bits += 7;

            while reg_bits >= 8 {
                out.push((reg >> (reg_bits - 8)) as u8);
                reg_bits -= 8;
            }

            if b & 1 == 0 {
                found_end = true;
                break;
            }
        }
        if !found_end {
            return Err(anyhow!("didn't terminate"));
        }
        Ok((out, i))
    }
}

impl Element for uuid::Uuid {
    fn encoded_size(&self) -> usize {
        16
    }

    fn encode(&self, mut w: &mut [u8]) {
        // Only fails if not enough space, encoded_size() already guarantees.
        w.write_u128::<BigEndian>(self.as_u128()).unwrap();
    }

    fn decode(b: &[u8]) -> anyhow::Result<(Self, usize)> {
        if b.len() < 16 {
            return Err(anyhow!("uuid too short: {} < 16", b.len()));
        }
        Ok((Self::from_u128(BigEndian::read_u128(b)), 16))
    }
}

pub(crate) trait Tuple: Sized {
    fn encode(&self) -> Vec<u8>;
    fn decode(b: &[u8]) -> anyhow::Result<Self>;
    fn decode_prefix(b: &[u8]) -> anyhow::Result<Self>;
}

impl<E: Element> Tuple for (E,) {
    fn encode(&self) -> Vec<u8> {
        let mut out = vec![0u8; self.0.encoded_size()];
        self.0.encode(&mut out);
        out
    }

    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        let (e, i) = E::decode(b)?;
        if i != b.len() {
            return Err(anyhow!("didn't consume full input"));
        }
        Ok((e,))
    }

    fn decode_prefix(b: &[u8]) -> anyhow::Result<Self> {
        let (e, _) = E::decode(b)?;
        Ok((e,))
    }
}

impl<E0: Element, E1: Element> Tuple for (E0, E1) {
    fn encode(&self) -> Vec<u8> {
        let size_0 = self.0.encoded_size();
        let size_1 = self.1.encoded_size();
        let mut out = vec![0u8; size_0 + size_1];
        let mut i = 0;
        self.0.encode(&mut out[i..i + size_0]);
        i += size_0;
        self.1.encode(&mut out[i..i + size_1]);
        out
    }

    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        let mut i = 0;
        let (e0, e0_size) = E0::decode(&b[i..])?;
        i += e0_size;
        let (e1, e1_size) = E1::decode(&b[i..])?;
        i += e1_size;
        if i != b.len() {
            return Err(anyhow!("didn't consume full input"));
        }
        Ok((e0, e1))
    }

    fn decode_prefix(b: &[u8]) -> anyhow::Result<Self> {
        let mut i = 0;
        let (e0, e0_size) = E0::decode(&b[i..])?;
        i += e0_size;
        let (e1, _) = E1::decode(&b[i..])?;
        Ok((e0, e1))
    }
}

impl<E0: Element, E1: Element, E2: Element> Tuple for (E0, E1, E2) {
    fn encode(&self) -> Vec<u8> {
        let size_0 = self.0.encoded_size();
        let size_1 = self.1.encoded_size();
        let size_2 = self.2.encoded_size();
        let mut out = vec![0u8; size_0 + size_1 + size_2];
        let mut i = 0;
        self.0.encode(&mut out[i..i + size_0]);
        i += size_0;
        self.1.encode(&mut out[i..i + size_1]);
        i += size_0;
        self.2.encode(&mut out[i..i + size_2]);
        out
    }

    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        let mut i = 0;
        let (e0, e0_size) = E0::decode(&b[i..])?;
        i += e0_size;
        let (e1, e1_size) = E1::decode(&b[i..])?;
        i += e1_size;
        let (e2, e2_size) = E2::decode(&b[i..])?;
        i += e2_size;
        if i != b.len() {
            return Err(anyhow!("didn't consume full input"));
        }
        Ok((e0, e1, e2))
    }

    fn decode_prefix(b: &[u8]) -> anyhow::Result<Self> {
        let mut i = 0;
        let (e0, e0_size) = E0::decode(&b[i..])?;
        i += e0_size;
        let (e1, e1_size) = E1::decode(&b[i..])?;
        i += e1_size;
        let (e2, _) = E2::decode(&b[i..])?;
        Ok((e0, e1, e2))
    }
}

pub(crate) fn tuple_encode<T: Tuple>(t: &T) -> Vec<u8> {
    Tuple::encode(t)
}

pub(crate) fn tuple_decode<T: Tuple>(b: &[u8]) -> anyhow::Result<T> {
    Tuple::decode(b)
}

pub(crate) fn tuple_decode_prefix<T: Tuple>(b: &[u8]) -> anyhow::Result<T> {
    Tuple::decode_prefix(b)
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use super::tuple_decode;
    use super::tuple_encode;
    use super::Element;
    use super::Tuple;

    fn assert_tuple_roundtrip<T: Tuple + Debug + Eq>(t: T) {
        let encoded = tuple_encode(&t);
        let decoded: T = tuple_decode(&encoded).expect("tuple could not be decoded");

        assert_eq!(t, decoded);
    }

    fn assert_element_roundtrip<E: Element + Debug + Eq>(e: E) {
        let mut v = vec![0u8; e.encoded_size()];
        println!("{:?} encoded size is {:?}", e, v.len());
        e.encode(&mut v);
        let (act, n) = E::decode(&v).unwrap();
        assert_eq!(
            e, act,
            "{:?} didn't roundtrip, encoded as {:?}, decoded as {:?}",
            e, v, act
        );
        assert_eq!(n, v.len());
    }

    #[test]
    fn test_u64_roundtrip() {
        assert_element_roundtrip(0u64);

        for i in 1..=(64 / 7) {
            assert_element_roundtrip(((1u64 << (7 * i)) - 1) as u64);
            assert_element_roundtrip(((1u64 << (7 * i)) + 0) as u64);
            assert_element_roundtrip(((1u64 << (7 * i)) + 1) as u64);
        }

        assert_element_roundtrip(u64::MAX);

        assert_element_roundtrip(0x0000000000000004);
        assert_element_roundtrip(0x0000000000000009);
        assert_element_roundtrip(0x000000000000003e);
        assert_element_roundtrip(0x0000000000000097);
        assert_element_roundtrip(0x0000000000000687);
        assert_element_roundtrip(0x0000000000000925);
        assert_element_roundtrip(0x00000000000014c1);
        assert_element_roundtrip(0x000000000000bd79);
        assert_element_roundtrip(0x000000000001c8e5);
        assert_element_roundtrip(0x00000000000f799f);
        assert_element_roundtrip(0x0000000000130500);
        assert_element_roundtrip(0x0000000000f0fb60);
        assert_element_roundtrip(0x00000000035e0677);
        assert_element_roundtrip(0x000000000d5d9fae);
        assert_element_roundtrip(0x0000000032c2c857);
        assert_element_roundtrip(0x00000000821a7ccb);
        assert_element_roundtrip(0x00000003237e19e9);
        assert_element_roundtrip(0x0000000a2fd589a3);
        assert_element_roundtrip(0x00000018e232cc65);
        assert_element_roundtrip(0x000000a98e0ef68b);
        assert_element_roundtrip(0x00000ab8e0ee67e7);
        assert_element_roundtrip(0x00000c94afbdec6e);
        assert_element_roundtrip(0x0000a37a399bd847);
        assert_element_roundtrip(0x0000b9922d8f1633);
        assert_element_roundtrip(0x00030d1f70987ec0);
        assert_element_roundtrip(0x00088cb732ebec6a);
        assert_element_roundtrip(0x00428c42f303177e);
        assert_element_roundtrip(0x00c859f44a6acdf5);
        assert_element_roundtrip(0x041f9ce1c07e2db1);
        assert_element_roundtrip(0x0baaa4174a339d30);
        assert_element_roundtrip(0xc7a641024a25429b);
        assert_element_roundtrip(0xcf10a638efe7f801);
    }

    #[test]
    fn test_bytes_roundtrip() {
        assert_element_roundtrip(vec![]);
        assert_element_roundtrip(vec![0]);
        assert_element_roundtrip(vec![0xb5]);
        assert_element_roundtrip(vec![0x46, 0x9f]);
        assert_element_roundtrip(vec![0x2f, 0xf9, 0xfe]);
        assert_element_roundtrip(vec![0x0d, 0x2a, 0x6c, 0x3a]);
        assert_element_roundtrip(vec![0xe5, 0x90, 0x7d, 0xdd, 0x1e]);
        assert_element_roundtrip(vec![0x42, 0x39, 0x9c, 0xb5, 0x4b, 0xf1]);
        assert_element_roundtrip(vec![0xee, 0x27, 0x1b, 0xe8, 0x68, 0xf2, 0x77]);
        assert_element_roundtrip(vec![0xf0, 0x3c, 0x92, 0x6f, 0xbc, 0x2c, 0x3d, 0xdc]);
        assert_element_roundtrip(vec![0xde, 0x16, 0xff, 0xb9, 0xdb, 0x7f, 0x53, 0x31, 0x07]);
        assert_element_roundtrip(vec![
            0x03, 0x4d, 0xe0, 0x70, 0xad, 0x2a, 0xea, 0x1b, 0x4f, 0x6f,
        ]);
        assert_element_roundtrip(vec![
            0x32, 0xa3, 0x3b, 0xfe, 0x39, 0x85, 0xeb, 0x33, 0xb5, 0x62, 0x2e,
        ]);
        assert_element_roundtrip(vec![
            0xfe, 0xf7, 0x26, 0x15, 0x51, 0x0e, 0x51, 0xf4, 0x1c, 0xce, 0xd2, 0xb1,
        ]);
        assert_element_roundtrip(vec![
            0x99, 0xd7, 0x06, 0x5b, 0x01, 0x5b, 0x80, 0xf1, 0x4e, 0xba, 0x59, 0xb3, 0x8c,
        ]);
        assert_element_roundtrip(vec![
            0x0b, 0x60, 0xfd, 0xec, 0xf9, 0x12, 0x76, 0xad, 0xc3, 0x45, 0xa5, 0x3a, 0xec, 0xf3,
        ]);
        assert_element_roundtrip(vec![
            0xa1, 0x21, 0x3f, 0x0d, 0xa8, 0xe9, 0xe1, 0x22, 0x18, 0xcb, 0x09, 0x2c, 0x65, 0x54,
            0xa0,
        ]);
        assert_element_roundtrip(vec![
            0x51, 0xf9, 0x7e, 0x0a, 0x2b, 0x0c, 0xb2, 0xe3, 0x2f, 0x41, 0x15, 0x3b, 0xac, 0xe3,
            0x9c, 0x20,
        ]);
        assert_element_roundtrip(vec![
            0x9d, 0xbf, 0xf3, 0xa1, 0x68, 0x68, 0x59, 0x67, 0x01, 0x09, 0xaf, 0x1e, 0xe2, 0xdb,
            0xdf, 0x92, 0x58,
        ]);
        assert_element_roundtrip(vec![
            0x92, 0x93, 0x78, 0x61, 0xd3, 0xff, 0x75, 0x9f, 0x33, 0x44, 0xa6, 0x48, 0x25, 0xe2,
            0x6e, 0x9c, 0xba, 0x11,
        ]);
        assert_element_roundtrip(vec![
            0xd4, 0x5d, 0x1b, 0x38, 0x2a, 0x3f, 0x8c, 0x00, 0x5b, 0x84, 0x1f, 0x2e, 0xe1, 0x4e,
            0x81, 0x4b, 0xa1, 0x7d, 0xed,
        ]);
        assert_element_roundtrip(vec![
            0x84, 0x7b, 0x50, 0xb9, 0xf8, 0x7e, 0xa1, 0x89, 0x09, 0x65, 0xd3, 0xfd, 0x11, 0xa0,
            0x3f, 0xe4, 0x21, 0x80, 0x42, 0xa6,
        ]);
        assert_element_roundtrip(vec![
            0xdd, 0x34, 0xed, 0x6d, 0x6c, 0xce, 0x35, 0x50, 0xdf, 0x41, 0x88, 0xaa, 0x07, 0x8e,
            0x38, 0x01, 0xfd, 0xa2, 0xe1, 0x95, 0x44,
        ]);
        assert_element_roundtrip(vec![
            0xa3, 0x29, 0xf4, 0x97, 0x0e, 0xbc, 0xc7, 0x98, 0xc5, 0x7b, 0xec, 0x42, 0x42, 0xc4,
            0x80, 0xc3, 0x88, 0x47, 0xe1, 0xb1, 0xaa, 0x2f,
        ]);
        assert_element_roundtrip(vec![
            0x92, 0xd0, 0xe0, 0xb5, 0xd8, 0x35, 0x85, 0x56, 0xd9, 0x75, 0xb5, 0x0f, 0xd0, 0x3b,
            0xc8, 0xa3, 0xef, 0x70, 0xe8, 0xc4, 0xbc, 0x80, 0x70,
        ]);
        assert_element_roundtrip(vec![
            0x01, 0xf9, 0x9b, 0xc8, 0xdf, 0x2b, 0xc0, 0xd2, 0x51, 0xbf, 0xc0, 0x4d, 0xc7, 0x76,
            0x27, 0xdc, 0xdc, 0xb3, 0x4a, 0x14, 0x3c, 0x84, 0x0a, 0x1d,
        ]);
        assert_element_roundtrip(vec![
            0x1e, 0xfd, 0x94, 0xb3, 0xed, 0xe9, 0x10, 0xf0, 0x84, 0xe9, 0x17, 0xc2, 0xac, 0x65,
            0x89, 0xf5, 0xad, 0x8f, 0xda, 0x53, 0xb3, 0xdb, 0xc6, 0xf7, 0x37,
        ]);
        assert_element_roundtrip(vec![
            0xa5, 0x48, 0xa3, 0x82, 0xdb, 0xe8, 0x32, 0xe7, 0xfa, 0x4b, 0x4d, 0x15, 0x60, 0x39,
            0x70, 0x8f, 0xe0, 0xfe, 0x3b, 0x0a, 0x29, 0x5b, 0x26, 0x86, 0xb0, 0x27,
        ]);
        assert_element_roundtrip(vec![
            0x1f, 0xad, 0xa4, 0xb3, 0x96, 0xf6, 0x43, 0x8f, 0x00, 0xe6, 0xb4, 0x10, 0x65, 0xaf,
            0x26, 0x44, 0xcc, 0xf3, 0x0e, 0xcb, 0x9b, 0x43, 0x39, 0xb3, 0x3e, 0xbe, 0x21,
        ]);
        assert_element_roundtrip(vec![
            0xa6, 0x84, 0xb6, 0x0e, 0x49, 0xff, 0x67, 0x06, 0x3e, 0xaf, 0xda, 0x29, 0xe9, 0x65,
            0x12, 0xd3, 0xe1, 0xfc, 0x64, 0xaf, 0xc4, 0xb1, 0xba, 0x30, 0xd5, 0x6a, 0xa5, 0x6f,
        ]);
        assert_element_roundtrip(vec![
            0x31, 0x12, 0xd0, 0xb6, 0xd0, 0xe1, 0x0a, 0xbe, 0x98, 0x25, 0x07, 0x3c, 0x82, 0x6a,
            0x64, 0x26, 0xe8, 0xe8, 0x98, 0x72, 0x87, 0x84, 0x63, 0xd1, 0xb3, 0xe0, 0x7c, 0xb6,
            0x47,
        ]);
        assert_element_roundtrip(vec![
            0xd2, 0x05, 0x5c, 0x10, 0x0b, 0x1b, 0x00, 0x7f, 0x7c, 0xa0, 0x8d, 0xfa, 0x3e, 0x03,
            0x85, 0x52, 0xcf, 0x7f, 0xbe, 0x09, 0xe2, 0xaa, 0xd5, 0x6e, 0xeb, 0x56, 0x97, 0xae,
            0xb7, 0x03,
        ]);
    }

    #[test]
    fn test_tuple_roundtrip() {
        assert_tuple_roundtrip((2u64,));

        assert_tuple_roundtrip((2u64, 1u64));
    }
}
