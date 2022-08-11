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
    // hl ─   jk │   jl ┌   hj ┐   kl └   hk ┘   jkl ├   hjk ┤  hjl ┬   hkl ┴   hjkl ┼
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                          •                                                //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    //                                                                                           //
    louds: Vec<u8>,
    labels: Vec<u8>,
    terminal: Vec<u8>,
}

impl Louds {
    fn get(&self, k: Vec<u8>) -> Option<()> {
        unimplemented!();
    }

    fn n_children(&self, node_idx: usize) -> Option<usize> {
        Some(first_zero(&self.louds[..], node_idx)? - node_idx)
    }

    fn child(&self, node_idx: usize, label: u8) -> Option<usize> {
        unimplemented!()
    }

    fn parent(&self, node_idx: usize) -> Option<usize> {
        unimplemented!()
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

// Returns the number of one bits to the left of n.
fn rank(b: &[u8], idx: usize) -> usize {
    // TODO: This can be accelerated with some std::arch and lookup table shenanigans.
    let mut count = 0;
    for i in 0..idx {
        count += bit(b, i);
    }
    count
}

// Returns the index of the nth one bit.
fn select(b: &[u8], n: usize) -> Option<usize> {
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
