#![allow(dead_code)]
use std::collections::VecDeque;

use byteorder::{ByteOrder, NativeEndian};

struct Trie {
    root: Node,
}

struct Node {
    children: [Option<Box<Node>>; 256],
    terminal: bool,
}

impl Node {
    fn empty() -> Self {
        Self {
            children: [
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None,
            ],
            terminal: false,
        }
    }
}

impl Trie {
    fn new() -> Self {
        Self {
            root: Node::empty(),
        }
    }

    fn put(&mut self, k: Vec<u8>) {
        let mut curr = &mut self.root;
        for b in k {
            curr = curr.children[b as usize].get_or_insert_with(|| Box::new(Node::empty()));
        }
        curr.terminal = true;
    }

    fn to_louds(&self) -> Louds {
        let mut labels = vec![];
        let mut louds = BitStream::new();
        let mut terminal = BitStream::new();
        let mut queue = VecDeque::new();
        queue.push_back(&self.root);

        while let Some(node) = queue.pop_front() {
            let mut n_children = 0;
            for (label, maybe_child) in node.children.iter().enumerate() {
                if let Some(child) = maybe_child {
                    labels.push(label as u8);
                    n_children += 1;
                    queue.push_back(child.as_ref());
                }
            }
            for _ in 0..n_children {
                louds.push(true);
            }
            louds.push(false);
            terminal.push(node.terminal);
        }

        Louds {
            labels,
            louds: louds.to_vec(),
            terminal: terminal.to_vec(),
        }
    }
}

struct BitStream {
    buf: Vec<u8>,
    n: usize,
}
impl BitStream {
    fn new() -> Self {
        Self { buf: vec![], n: 0 }
    }

    fn push(&mut self, b: bool) {
        if self.n == self.buf.len() * 8 {
            self.buf.push(0);
        }
        if b {
            let buf_len = self.buf.len();
            self.buf[buf_len - 1] |= 1 << (7 - (self.n % 8));
        }
        self.n += 1;
    }

    fn to_vec(self) -> Vec<u8> {
        self.buf
    }
}

struct Louds {
    // bad
    // ban
    // bank
    // bar
    // bog
    // book
    // boot
    // 24 bytes in total, plus lengths
    //
    //                                            □0                                              //
    //                                            │                                               //
    //                                           b│                                               //
    //                                            │                                               //
    //                                            □1                                              //
    //                                     ┌──────┴──────┐                                        //
    //                                    a│            o│                                        //
    //                                     │             │                                        //
    //                                     □2            □3                                       //
    //                                 ┌───┼───┐      ┌──┴──┐                                     //
    //                                d│  n│  r│     g│    o│                                     //
    //                                 │   │   │      │     │                                     //
    //                                 ■4  ■5  ■6     ■7    □8                                    //
    //                                     │             ┌──┴──┐                                  //
    //                                    k│            k│    t│                                  //
    //                                     │             │     │                                  //
    //                                     ■9            ■10   ■12                                //
    //
    // node number: 0    1    2    3    4    5    6    7    8    9    10   11
    // n children:  1    2    3    2    0    1    0    0    2    0    0    0
    // louds:       10   110  1110 110  0    10   0    0    110  0    0    0           (23 bits)
    // node_idx:    0    2    5    9    12   13   15   16   17   20   21   22
    // labels:      b    ao   dnr  go        k              kt                         (11 bytes = 88 bits)
    // terminal:    0    0    0    0    1    1    1    1    0    1    1    1           (12 bits)
    //                                                                                 ~= 16 bytes
    louds: Vec<u8>,
    labels: Vec<u8>,
    terminal: Vec<u8>,
}

impl Louds {
    // Two important things to keep straight:
    // node_idx refers to the index of the first bit in LOUDS that is a part of this node.
    // node_num refers to level-ordered numbering as in the diagram above.
    //
    // With the unary encoding, each node has exactly one zero, so we can use that fact to
    // translate between the two.

    fn node_idx_to_num(&self, node_idx: usize) -> usize {
        rank_0(&self.louds[..], node_idx)
    }

    fn node_num_to_idx(&self, node_num: usize) -> Option<usize> {
        if node_num == 0 {
            return Some(0);
        }
        Some(select_0(&self.louds[..], node_num)? + 1)
    }

    fn get(&self, k: Vec<u8>) -> Option<()> {
        let mut node_idx = 0;
        for b in k {
            node_idx = self.child(node_idx, b)?;
        }
        if bit(&self.terminal[..], self.node_idx_to_num(node_idx)) == 1 {
            return Some(());
        }
        None
    }

    fn n_children(&self, node_idx: usize) -> Option<usize> {
        Some(select_0(&self.louds[..], rank_0(&self.louds[..], node_idx) + 1)? - node_idx)
    }

    fn kth_child(&self, node_idx: usize, k: usize) -> Option<usize> {
        let child_node_num = rank_1(&self.louds[..], node_idx) + k + 1;
        Some(self.node_num_to_idx(child_node_num)?)
    }

    fn child(&self, node_idx: usize, label: u8) -> Option<usize> {
        let n = self.n_children(node_idx)?;
        let label_start = rank_1(&self.louds[..], node_idx);
        for i in 0..n {
            if self.labels[label_start + i] == label {
                return self.kth_child(node_idx, i);
            }
        }
        None
    }

    fn parent(&self, node_idx: usize) -> Option<usize> {
        Some(select_1(
            &self.louds[..],
            rank_0(&self.louds[..], node_idx),
        )?)
    }
}

// Returns the number of zero bits to the left of n.
fn rank_0(b: &[u8], idx: usize) -> usize {
    idx - rank_1(b, idx)
}

// Returns the number of one bits to the left of n.
pub fn rank_1(b: &[u8], idx: usize) -> usize {
    if idx == 0 {
        return 0;
    }
    let mut count = 0;
    let iter = b[0..idx / 8].chunks_exact(8);
    for chunk in iter {
        count += NativeEndian::read_u64(chunk).count_ones();
    }
    if idx % 64 == 0 {
        // No partial chunk to speak of, idx landed right on a boundary.
        return count as usize;
    }
    let partial_chunk_start = idx / 8 / 8 * 8;
    let partial_chunk_end = std::cmp::min(b.len(), partial_chunk_start + 8);
    let partial_chunk = &b[partial_chunk_start..partial_chunk_end];
    let mask = u64::MAX << (64 - (idx % 64));
    let mut rem_u64 = 0u64;
    for (i, b) in partial_chunk.iter().enumerate() {
        rem_u64 |= (*b as u64) << (64 - 8 - i * 8);
    }
    count += (rem_u64 & mask).count_ones();
    count as usize
}

// Returns the index of the nth zero bit.
fn select_0(b: &[u8], n: usize) -> Option<usize> {
    // Make use of the fact that count_zeros() on large chunks is pretty quick, so first figure out
    // which 8-byte chunk the answer is in, then the 8-bit byte, then which bit within that byte.
    //
    // Note: using u128 may be faster, worth a benchmark, and messing with
    // _mm{256,512}_popcnt_epi64 may be a little faster especially on repeated calls when stuff is
    // already in cache.

    fn find_chunk(b: &[u8], n: usize) -> Option<(usize, usize)> {
        let mut count = 0;
        let iter = b.chunks_exact(8);
        let rem = iter.remainder();
        for (i, chunk) in iter.enumerate() {
            let chunk_count = NativeEndian::read_u64(chunk).count_zeros() as usize;
            if count + chunk_count >= n {
                return Some((i * 8, count));
            }
            count += chunk_count;
        }
        if rem.len() > 0 {
            // Not sure if the last chunk actually does have n in it, but we can blindly return
            // Some here and let find_byte figure it out.
            return Some((b.len() - rem.len(), count));
        }
        None
    }

    fn find_byte(b: &[u8], n: usize) -> Option<(usize, usize)> {
        let mut count = 0;
        for (i, byte) in b.iter().enumerate() {
            let byte_count = byte.count_zeros() as usize;
            if count + byte_count >= n {
                return Some((i, count));
            }
            count += byte_count;
        }
        None
    }

    let (chunk_idx, count_at_chunk_idx) = find_chunk(b, n)?;
    let (byte_idx, count_at_byte_idx) = {
        let (byte_idx_in_chunk, count_at_byte_in_chunk) =
            find_byte(&b[chunk_idx..], n - count_at_chunk_idx)?;
        (
            chunk_idx + byte_idx_in_chunk,
            count_at_chunk_idx + count_at_byte_in_chunk,
        )
    };
    let byte = b[byte_idx];
    let mut count = count_at_byte_idx;
    for i in 0..8 {
        count += 1 - ((byte >> (7 - i)) & 1) as usize;
        if count == n {
            return Some(byte_idx * 8 + i);
        }
    }
    None
}

// Returns the index of the nth one bit.
fn select_1(b: &[u8], n: usize) -> Option<usize> {
    fn find_chunk(b: &[u8], n: usize) -> Option<(usize, usize)> {
        let mut count = 0;
        let iter = b.chunks_exact(8);
        let rem = iter.remainder();
        for (i, chunk) in iter.enumerate() {
            let chunk_count = NativeEndian::read_u64(chunk).count_ones() as usize;
            if count + chunk_count >= n {
                return Some((i * 8, count));
            }
            count += chunk_count;
        }
        if rem.len() > 0 {
            // Not sure if the last chunk actually does have n in it, but we can blindly return
            // Some here and let find_byte figure it out.
            return Some((b.len() - rem.len(), count));
        }
        None
    }

    fn find_byte(b: &[u8], n: usize) -> Option<(usize, usize)> {
        let mut count = 0;
        for (i, byte) in b.iter().enumerate() {
            let byte_count = byte.count_ones() as usize;
            if count + byte_count >= n {
                return Some((i, count));
            }
            count += byte_count;
        }
        None
    }

    let (chunk_idx, count_at_chunk_idx) = find_chunk(b, n)?;
    let (byte_idx, count_at_byte_idx) = {
        let (byte_idx_in_chunk, count_at_byte_in_chunk) =
            find_byte(&b[chunk_idx..], n - count_at_chunk_idx)?;
        (
            chunk_idx + byte_idx_in_chunk,
            count_at_chunk_idx + count_at_byte_in_chunk,
        )
    };
    let byte = b[byte_idx];
    let mut count = count_at_byte_idx;
    for i in 0..8 {
        count += ((byte >> (7 - i)) & 1) as usize;
        if count == n {
            return Some(byte_idx * 8 + i);
        }
    }
    None
}

fn bit(b: &[u8], idx: usize) -> usize {
    ((b[idx / 8] >> (7 - (idx % 8))) & 1) as usize
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;

    use crate::{bit, rank_0, rank_1, select_0, select_1, BitStream, Trie};

    #[test]
    fn test_bit_stream() {
        let mut bs = BitStream::new();
        bs.push(true);
        bs.push(true);
        bs.push(false);
        bs.push(true);

        bs.push(false);
        bs.push(true);
        bs.push(false);
        bs.push(false);

        bs.push(true);
        bs.push(false);
        bs.push(true);

        assert_eq!(bs.to_vec(), vec![0b11010100, 0b10100000]);
    }

    #[test]
    fn test_trie_to_louds() {
        // Using the hand-computed trie from the comment in Louds.

        let words = vec!["bad", "ban", "bank", "bar", "bog", "book", "boot"];

        let louds = {
            let mut trie = Trie::new();
            for word in &words {
                trie.put(word.clone().into());
            }
            trie.to_louds()
        };

        assert_eq!(louds.louds, vec![0b10110111, 0b01100100, 0b01100000]);
        assert_eq!(louds.labels, Into::<Vec<u8>>::into("baodnrgokkt"));
        assert_eq!(louds.terminal, vec![0b00001111, 0b01110000]);

        assert_eq!(louds.node_num_to_idx(0), Some(0));
        assert_eq!(louds.node_num_to_idx(1), Some(2));
        assert_eq!(louds.node_num_to_idx(2), Some(5));
        assert_eq!(louds.node_num_to_idx(3), Some(9));
        assert_eq!(louds.node_idx_to_num(0), 0);
        assert_eq!(louds.node_idx_to_num(2), 1);
        assert_eq!(louds.node_idx_to_num(5), 2);
        assert_eq!(louds.node_idx_to_num(9), 3);

        for word in &words {
            assert_eq!(louds.get(word.clone().into()), Some(()), "word: {}", word);
        }

        let not_words = vec!["a", "b", "baa", "bang", "bark", "boo", "booking", "boots"];
        for word in &not_words {
            assert_eq!(louds.get(word.clone().into()), None, "not word: {}", word);
        }
    }

    #[test]
    fn test_bit() {
        let v = vec![0b11011000, 0b01101101];
        //             01234567    89012345
        //                           1

        assert_eq!(bit(&v[..], 0), 1);
        assert_eq!(bit(&v[..], 1), 1);
        assert_eq!(bit(&v[..], 2), 0);
        assert_eq!(bit(&v[..], 3), 1);
        assert_eq!(bit(&v[..], 4), 1);
        assert_eq!(bit(&v[..], 5), 0);
        assert_eq!(bit(&v[..], 6), 0);
        assert_eq!(bit(&v[..], 7), 0);
        assert_eq!(bit(&v[..], 8), 0);
        assert_eq!(bit(&v[..], 9), 1);
        assert_eq!(bit(&v[..], 10), 1);
        assert_eq!(bit(&v[..], 11), 0);
        assert_eq!(bit(&v[..], 12), 1);
        assert_eq!(bit(&v[..], 13), 1);
        assert_eq!(bit(&v[..], 14), 0);
        assert_eq!(bit(&v[..], 15), 1);
    }

    #[test]
    fn test_rank() {
        let v = vec![0b11011000, 0b01101101];
        //             01234567    89012345
        //                           1

        assert_eq!(rank_0(&v[..], 0), 0);
        assert_eq!(rank_0(&v[..], 1), 0);
        assert_eq!(rank_0(&v[..], 2), 0);
        assert_eq!(rank_0(&v[..], 3), 1);
        assert_eq!(rank_0(&v[..], 4), 1);
        assert_eq!(rank_0(&v[..], 5), 1);
        assert_eq!(rank_0(&v[..], 6), 2);
        assert_eq!(rank_0(&v[..], 7), 3);
        assert_eq!(rank_0(&v[..], 8), 4);
        assert_eq!(rank_0(&v[..], 9), 5);
        assert_eq!(rank_0(&v[..], 10), 5);
        assert_eq!(rank_0(&v[..], 11), 5);
        assert_eq!(rank_0(&v[..], 12), 6);
        assert_eq!(rank_0(&v[..], 13), 6);
        assert_eq!(rank_0(&v[..], 14), 6);
        assert_eq!(rank_0(&v[..], 15), 7);

        assert_eq!(rank_1(&v[..], 0), 0);
        assert_eq!(rank_1(&v[..], 1), 1);
        assert_eq!(rank_1(&v[..], 2), 2);
        assert_eq!(rank_1(&v[..], 3), 2);
        assert_eq!(rank_1(&v[..], 4), 3);
        assert_eq!(rank_1(&v[..], 5), 4);
        assert_eq!(rank_1(&v[..], 6), 4);
        assert_eq!(rank_1(&v[..], 7), 4);
        assert_eq!(rank_1(&v[..], 8), 4);
        assert_eq!(rank_1(&v[..], 9), 4);
        assert_eq!(rank_1(&v[..], 10), 5);
        assert_eq!(rank_1(&v[..], 11), 6);
        assert_eq!(rank_1(&v[..], 12), 6);
        assert_eq!(rank_1(&v[..], 13), 7);
        assert_eq!(rank_1(&v[..], 14), 8);
        assert_eq!(rank_1(&v[..], 15), 8);

        let v = vec![0, 0, 0, 0, 0, 0, 0, 0, 0b01000000, 0b00010000];
        //                                     ^64         ^72
        assert_eq!(rank_1(&v[..], 64), 0);
        assert_eq!(rank_1(&v[..], 65), 0);
        assert_eq!(rank_1(&v[..], 66), 1);
        assert_eq!(rank_1(&v[..], 74), 1);
        assert_eq!(rank_1(&v[..], 75), 1);
        assert_eq!(rank_1(&v[..], 76), 2);
    }

    #[test]
    fn test_select() {
        let v = vec![0b11011000, 0b01101101];
        //             01234567    89012345
        //                           1

        assert_eq!(select_0(&v[..], 1), Some(2));
        assert_eq!(select_0(&v[..], 2), Some(5));
        assert_eq!(select_0(&v[..], 3), Some(6));
        assert_eq!(select_0(&v[..], 4), Some(7));
        assert_eq!(select_0(&v[..], 5), Some(8));
        assert_eq!(select_0(&v[..], 6), Some(11));
        assert_eq!(select_0(&v[..], 7), Some(14));
        assert_eq!(select_0(&v[..], 9), None);

        assert_eq!(select_1(&v[..], 1), Some(0));
        assert_eq!(select_1(&v[..], 2), Some(1));
        assert_eq!(select_1(&v[..], 3), Some(3));
        assert_eq!(select_1(&v[..], 4), Some(4));
        assert_eq!(select_1(&v[..], 5), Some(9));
        assert_eq!(select_1(&v[..], 6), Some(10));
        assert_eq!(select_1(&v[..], 7), Some(12));
        assert_eq!(select_1(&v[..], 8), Some(13));
        assert_eq!(select_1(&v[..], 9), Some(15));
        assert_eq!(select_1(&v[..], 10), None);
    }

    proptest! {
        #[test]
        fn proptest_select_0(b in any::<Vec<u8>>()) {
            fn select_0_basic(b: &[u8], n: usize) -> Option<usize> {
                let mut count = 0;
                for i in 0..b.len() * 8 {
                    count += 1 - bit(b, i);
                    if count == n {
                        return Some(i);
                    }
                }
                None
            }

            for n in 0..b.len()*8 {
                assert_eq!(select_0(&b[..], n), select_0_basic(&b[..], n), "n: {}", n);
            }
        }

        #[test]
        fn proptest_select_1(b in any::<Vec<u8>>()) {
            fn select_1_basic(b: &[u8], n: usize) -> Option<usize> {
                let mut count = 0;
                for i in 0..b.len() * 8 {
                    count += bit(b, i);
                    if count == n {
                        return Some(i);
                    }
                }
                None
            }

            for n in 0..b.len()*8 {
                assert_eq!(select_1(&b[..], n), select_1_basic(&b[..], n), "n: {}", n);
            }
        }

        #[test]
        fn proptest_rank_1(b in any::<Vec<u8>>()) {
            fn rank_1_basic(b: &[u8], idx: usize) -> usize {
                let mut count = 0;
                for i in 0..idx {
                    count += bit(b, i);
                }
                count
            }

            for n in 0..=b.len()*8 {
                assert_eq!(rank_1(&b[..], n), rank_1_basic(&b[..], n), "n: {}", n);
            }
        }
    }
}
