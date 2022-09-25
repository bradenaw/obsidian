use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::sync::RwLock;

use async_stream::try_stream;
use futures::stream::Stream;
use futures::stream::StreamExt;

pub(crate) fn merge_sorted<'a, T: Ord + 'a>(
    mut iters: Vec<impl Iterator<Item = T> + 'a>,
) -> impl Iterator<Item = T> + 'a {
    let mut h: BinaryHeap<(std::cmp::Reverse<T>, usize)> = BinaryHeap::new();
    h.reserve_exact(iters.len());
    for i in 0..iters.len() {
        if let Some(t) = iters[i].next() {
            h.push((std::cmp::Reverse(t), i));
        }
    }
    std::iter::from_fn(move || {
        let (t, i) = h.pop()?;
        if let Some(t) = iters[i].next() {
            h.push((std::cmp::Reverse(t), i));
        }
        Some(t.0)
    })
}

pub(crate) fn merge_sorted_streams<T: Ord + Send>(
    mut streams: Vec<impl Stream<Item = anyhow::Result<T>> + Unpin + Send>,
) -> impl Stream<Item = anyhow::Result<T>> + Send {
    try_stream! {
        let mut h: BinaryHeap<(std::cmp::Reverse<T>, usize)> = BinaryHeap::new();
        h.reserve_exact(streams.len());
        let n = streams.len();
        for i in 0..n {
            if let Some(t) = streams[i].next().await.transpose()? {
                h.push((std::cmp::Reverse(t), i));
            }
        }
        while let Some((t, i)) = h.pop() {
            if let Some(t) = streams[i].next().await.transpose()? {
                h.push((std::cmp::Reverse(t), i));
            }
            yield t.0;
        }
    }
}

pub(crate) struct OrdEqByFirst<A, B>(pub A, pub B);

impl<A: Eq, B> Eq for OrdEqByFirst<A, B> {}
impl<A: Eq, B> PartialEq for OrdEqByFirst<A, B> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl<A: Ord, B> Ord for OrdEqByFirst<A, B> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}
impl<A: Ord, B> PartialOrd for OrdEqByFirst<A, B> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) fn hexlify(b: &[u8]) -> String {
    b.iter().map(|b| format!("{:02x}", b)).collect()
}

pub(crate) fn binary_search_by_idx<K: Ord, F: Fn(usize) -> K>(
    n: usize,
    k: K,
    f: F,
) -> Result<usize, usize> {
    let mut lower = 0;
    let mut upper = n;
    while lower < upper {
        let mid = (lower + upper) / 2;
        let at_mid = f(mid);
        match k.cmp(&at_mid) {
            Ordering::Equal => return Ok(mid),
            Ordering::Less => upper = mid,
            Ordering::Greater => lower = mid + 1,
        }
    }
    Err(lower)
}

pub(crate) fn longest_shared_prefix(a: &[u8], b: &[u8]) -> Vec<u8> {
    std::iter::zip(a.iter(), b.iter())
        .take_while(|(a, b)| *a == *b)
        .map(|(a, _)| *a)
        .collect()
}

// Returns the number of bytes needed to represent x.
pub(crate) fn byte_width(x: u64) -> usize {
    let bits_needed = 64 - x.leading_zeros();
    ((bits_needed + 7) / 8) as usize
}

pub(crate) struct AtomicArc<T> {
    // TODO: figure out how to do this with actual atomic instructions
    lock: RwLock<Arc<T>>,
}

impl<T> AtomicArc<T> {
    pub fn new(t: Arc<T>) -> Self {
        Self {
            lock: RwLock::new(t),
        }
    }

    pub fn load(&self) -> Arc<T> {
        self.lock.read().unwrap().clone()
    }

    pub fn store(&self, t: Arc<T>) {
        let mut guard = self.lock.write().unwrap();
        *guard = t;
    }

    pub fn compare_and_swap(&self, prev: &Arc<T>, next: Arc<T>) -> bool {
        let mut guard = self.lock.write().unwrap();
        if Arc::ptr_eq(prev, &*guard) {
            *guard = next;
            return true;
        }
        false
    }
}
