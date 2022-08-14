#![allow(dead_code)]
use std::collections::VecDeque;

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
    // 27 bytes in total, plus lengths
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
        Some(first_zero(&self.louds[..], node_idx)? - node_idx)
    }

    fn kth_child(&self, node_idx: usize, k: usize) -> Option<usize> {
        let child_node_num = rank_1(&self.louds[..], node_idx) + k;
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

// Returns the index of the first zero bit in b after idx.
fn first_zero(b: &[u8], idx: usize) -> Option<usize> {
    for i in idx..b.len() * 8 {
        if bit(b, i) == 0 {
            return Some(i);
        }
    }
    None
}

// Returns the number of zero bits to the left of n.
fn rank_0(b: &[u8], idx: usize) -> usize {
    idx - rank_1(b, idx)
}

// Returns the number of one bits to the left of n.
fn rank_1(b: &[u8], idx: usize) -> usize {
    // TODO: This can be accelerated with some std::arch and lookup table shenanigans.
    let mut count = 0;
    for i in 0..idx {
        count += bit(b, i);
    }
    count
}

// Returns the index of the nth zero bit.
fn select_0(b: &[u8], n: usize) -> Option<usize> {
    // TODO: This can be accelerated with some std::arch and lookup table shenanigans.
    let mut count = 0;
    for i in 0..b.len() * 8 {
        count += 1 - bit(b, i);
        if count == n {
            return Some(n);
        }
    }
    None
}

// Returns the index of the nth one bit.
fn select_1(b: &[u8], n: usize) -> Option<usize> {
    // TODO: This can be accelerated with some std::arch and lookup table shenanigans.
    let mut count = 0;
    for i in 0..b.len() * 8 {
        count += bit(b, i);
        if count == n {
            return Some(n);
        }
    }
    None
}

fn bit(b: &[u8], idx: usize) -> usize {
    ((b[idx / 8] >> (7 - (idx % 8))) & 1) as usize
}

#[cfg(test)]
mod test {
    use crate::{BitStream, Trie};

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

        for word in &words {
            assert_eq!(louds.get(word.clone().into()), Some(()), "word: {}", word);
        }
    }
}
