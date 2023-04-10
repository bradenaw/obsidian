use anyhow::anyhow;

trait Element: Sized {
    fn encoded_size(&self) -> usize;
    fn encode(&self, w: &mut [u8]);
    fn decode(b: &[u8]) -> anyhow::Result<(Self, usize)>;
}

impl Element for u64 {
    fn encoded_size(&self) -> usize {
        let n_bits = 64 - self.leading_zeros();
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
            51..=56 => {
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
        (self.len() * 8 + 6) / 7
    }

    fn encode(&self, w: &mut [u8]) {
        let mut reg = 0u32;
        let mut reg_bits = 0;
        let mut out_i = 0;
        for b in self {
            reg = (reg << 8) | (*b as u32);
            reg_bits += 8;

            while reg_bits >= 7 {
                w[out_i] = ((reg >> (reg_bits - 6)) | 1) as u8;
                reg = reg >> 7;
                reg_bits -= 7;
                out_i += 1;
            }
        }
        if reg_bits > 0 {
            w[out_i] = (reg << (8 - reg_bits)) as u8;
        }
        w[out_i] &= 0b1111110
    }

    fn decode(encoded: &[u8]) -> anyhow::Result<(Self, usize)> {
        let mut reg = 0u32;
        let mut reg_bits = 0;
        let mut out = Vec::with_capacity((encoded.len() * 7 + 7) / 8);
        for (i, b) in encoded.iter().enumerate() {
            reg = reg << 7 | ((*b >> 1) as u32);
            reg_bits += 7;

            while reg_bits >= 8 {
                out.push((reg >> (reg_bits - 8)) as u8);
            }

            if b & 1 == 0 {
                return Ok((out, i));
            }
        }
        Err(anyhow!("didn't terminate"))
    }
}

trait Tuple: Sized {
    fn encode(&self) -> Vec<u8>;
    fn decode(b: &[u8]) -> anyhow::Result<Self>;
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
}

impl<E1: Element, E2: Element> Tuple for (E1, E2) {
    fn encode(&self) -> Vec<u8> {
        let size_0 = self.0.encoded_size();
        let size_1 = self.1.encoded_size();
        let mut out = vec![0u8; size_0 + size_1];
        let mut i = 0;
        self.0.encode(&mut out[i..]);
        i += size_0;
        self.1.encode(&mut out[i..]);
        out
    }

    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        let mut i = 0;
        let (e0, e0_size) = E1::decode(&b[i..])?;
        i += e0_size;
        let (e1, e1_size) = E2::decode(&b[i..])?;
        i += e1_size;
        if i != b.len() {
            return Err(anyhow!("didn't consume full input"));
        }
        Ok((e0, e1))
    }
}
