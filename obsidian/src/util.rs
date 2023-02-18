use std::cmp;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::future::Future;
use std::io::Read;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use async_stream::try_stream;
use async_trait::async_trait;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use futures::stream::Stream;
use futures::stream::StreamExt;
use rand::Rng;

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

pub(crate) fn longest_shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    std::iter::zip(a.iter(), b.iter())
        .take_while(|(a, b)| *a == *b)
        .map(|(a, _)| *a)
        .count()
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

pub(crate) async fn bounded_unordered_map<T, F: Fn(T) -> Fut, Fut: futures::Future<Output = ()>>(
    receiver: tokio::sync::mpsc::Receiver<T>,
    max_concurrent: usize,
    process: F,
) {
    let mut waits = futures::stream::FuturesUnordered::new();

    futures::pin_mut!(receiver);
    let mut done = false;
    loop {
        tokio::select! {
            next = receiver.recv(), if !done && waits.len() < max_concurrent => {
                match next {
                    Some(t) => {
                        waits.push(process(t));
                    },
                    None => {
                        done = true;
                    }
                }
            }
            Some(_) = waits.next() => {
                if done && waits.len() == 0 {
                    break;
                }
            }
        }
    }
}

#[async_trait]
pub(crate) trait AsyncReadExactAt {
    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<()>;
    async fn len(&self) -> anyhow::Result<u64>;
}

#[async_trait]
impl AsyncReadExactAt for Vec<u8> {
    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<()> {
        Ok(buf.copy_from_slice(&self[(offset as usize)..(offset as usize) + buf.len()]))
    }
    async fn len(&self) -> anyhow::Result<u64> {
        Ok(self.len() as u64)
    }
}

#[async_trait]
impl AsyncReadExactAt for Arc<Vec<u8>> {
    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<()> {
        Ok(buf.copy_from_slice(&self[(offset as usize)..(offset as usize) + buf.len()]))
    }
    async fn len(&self) -> anyhow::Result<u64> {
        Ok(Vec::len(self) as u64)
    }
}

#[async_trait]
impl AsyncReadExactAt for File {
    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<()> {
        // TODO: This requires an extra allocation because spawn_blocking can't hold onto a mut ref
        // to buf because compiler isn't smart enough to know that we immediately await it and that
        // awaiting it implies that the function is done running.
        //
        // Static-sized reads are not the common case here it seems, so it might be worth just
        // changing this function to take a length and always do the allocation internally, or
        // figure out how tokio implements AsyncRead::read_exact() when poll_read() requires a
        // spawn_blocking.
        let mut inner_buf = vec![0u8; buf.len()];
        // We can safely clone this because the file descriptor's state is not affected by
        // read_exact_at.
        let other = self.try_clone()?;
        let mut inner_buf = tokio::task::spawn_blocking(move || {
            FileExt::read_exact_at(&other, &mut inner_buf, offset)?;
            Ok::<Vec<u8>, anyhow::Error>(inner_buf)
        })
        .await??;
        buf.copy_from_slice(&mut inner_buf);
        Ok(())
    }
    async fn len(&self) -> anyhow::Result<u64> {
        todo!()
    }
}

pub(crate) fn delay_for_retry(i: usize, min_delay: Duration, max_delay: Duration) -> Duration {
    let avg_delay = std::cmp::min(
        min_delay.saturating_mul(2u32.saturating_pow(i as u32)),
        max_delay,
    );
    rand::thread_rng().gen_range(avg_delay / 2..avg_delay * 3 / 2)
}
pub(crate) async fn sleep_for_retry(i: usize, min_delay: Duration, max_delay: Duration) {
    tokio::time::sleep(delay_for_retry(i, min_delay, max_delay)).await;
}

pub(crate) struct Retry {
    min_delay: Duration,
    max_delay: Duration,
    timeout: Duration,
    n_attempts: usize,
}

impl Retry {
    pub fn new() -> Self {
        Self {
            min_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(5000),
            timeout: Duration::MAX,
            n_attempts: 5,
        }
    }

    pub fn min_delay(mut self, x: Duration) -> Self {
        self.min_delay = x;
        self
    }

    pub fn max_delay(mut self, x: Duration) -> Self {
        self.max_delay = x;
        self
    }

    pub fn timeout(mut self, x: Duration) -> Self {
        self.timeout = x;
        self
    }

    pub fn n_attempts(mut self, x: usize) -> Self {
        self.n_attempts = x;
        self
    }

    pub async fn indefinitely<
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        T,
        E: std::error::Error + Send + Sync + 'static,
    >(
        self,
        f: F,
    ) -> T {
        let mut i = 0;
        loop {
            if let Ok(t) = f().await {
                return t;
            }
            sleep_for_retry(i, self.min_delay, self.max_delay).await;
            i = i.saturating_add(1);
        }
    }

    pub async fn with_retry<
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        T,
        E: std::error::Error + Send + Sync + 'static,
    >(
        self,
        f: F,
    ) -> anyhow::Result<T> {
        let start = std::time::Instant::now();
        let mut last_err = None;
        for i in 0..self.n_attempts {
            match tokio::time::timeout(self.timeout - start.elapsed(), f()).await {
                Ok(Ok(t)) => return Ok(t),
                Ok(Err(e)) => {
                    last_err = Some(e);
                }
                Err(_) => {
                    // Timeout. Bubble out the last actual error if possible.
                    if let Some(e) = last_err {
                        return Err(e.into());
                    }
                    anyhow::bail!("timed out")
                }
            }
            let delay = delay_for_retry(i, self.min_delay, self.max_delay);
            if delay > self.timeout - start.elapsed() {
                // last_err can't be None here, if it were we would have already returned.
                return Err(last_err.unwrap().into());
            }
            tokio::time::sleep(delay).await;
        }
        if let Some(e) = last_err {
            return Err(e.into());
        }
        anyhow::bail!("no attempts")
    }
}
