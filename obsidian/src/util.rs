use std::cmp;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::io::Read;
use std::io::Write;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use anyhow::anyhow;
use async_stream::try_stream;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use futures::stream::Stream;
use futures::stream::StreamExt;
use rand::Rng;

use crate::pb;
use crate::types::ColoGroupId;
use crate::types::Key;
use crate::types::KeyspaceId;
use crate::types::Timestamp;

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

pub(crate) enum IteratorEither<A, B> {
    Left(A),
    Right(B),
}

impl<T, A: Iterator<Item = T>, B: Iterator<Item = T>> Iterator for IteratorEither<A, B> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            IteratorEither::Left(inner) => inner.next(),
            IteratorEither::Right(inner) => inner.next(),
        }
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

pub(crate) enum RetryResult<T, E> {
    Ok(T),
    Retry(E),
    Err(E),
}

impl<T, E> From<Result<T, E>> for RetryResult<T, E> {
    fn from(value: Result<T, E>) -> Self {
        match value {
            Ok(v) => RetryResult::Ok(v),
            Err(e) => RetryResult::Err(e),
        }
    }
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
        E: Deref<Target = dyn std::error::Error + Send + Sync + 'static>,
    >(
        self,
        f: &F,
    ) -> T {
        let mut delays = RetryDelay::new(self.min_delay, self.max_delay);
        loop {
            match f().await {
                Ok(t) => {
                    return t;
                }
                Err(e) => {
                    let delay = delays.next();
                    log::warn!("error, retrying in {:?}: {:?}", delay, e.deref());
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    pub async fn with_retry<
        F: Fn() -> Fut,
        Fut: Future<Output = RetryResult<T, E>>,
        T,
        E: std::error::Error + Send + Sync + 'static,
    >(
        self,
        f: F,
    ) -> anyhow::Result<T> {
        let start = std::time::Instant::now();
        let mut last_err = None;

        let mut delays = RetryDelay::new(self.min_delay, self.max_delay);

        for _ in 0..self.n_attempts {
            match tokio::time::timeout(self.timeout - start.elapsed(), f()).await {
                Ok(RetryResult::Ok(t)) => return Ok(t),
                Ok(RetryResult::Retry(e)) => {
                    last_err = Some(e);
                }
                Ok(RetryResult::Err(e)) => {
                    return Err(e.into());
                }
                Err(_) => {
                    // Timeout. Bubble out the last actual error if possible.
                    if let Some(e) = last_err {
                        return Err(e.into());
                    }
                    anyhow::bail!("timed out")
                }
            }

            let delay = delays.next();
            if delay > self.timeout - start.elapsed() {
                // last_err can't be None here, if it were we would have already returned.
                return Err(last_err.unwrap().into());
            }
            log::warn!("error, retrying in {:?}: {:?}", delay, last_err);
            tokio::time::sleep(delay).await;
        }
        if let Some(e) = last_err {
            return Err(e.into());
        }
        anyhow::bail!("no attempts")
    }
}

struct RetryDelay {
    attempts: VecDeque<std::time::Instant>,
    size: usize,
    max_age: Duration,
    min_delay: Duration,
    max_delay: Duration,
}

impl RetryDelay {
    fn new(min_delay: Duration, max_delay: Duration) -> Self {
        // We only need this many for delay_for_retry to return the max value, so any more are
        // pointless.
        let size =
            (max_delay.as_secs_f64().log2() - max_delay.as_secs_f64().log2()).ceil() as usize;
        // If we were consistently picking the high end of the jitter on max_delay and failing
        // immediately (the worst case), we'd need to keep around attempts for this long to keep
        // delay_for_retry returning max_delay.
        //
        // We forget anything older because they're probably less relevant.
        let max_age = Duration::from_secs_f64(max_delay.as_secs_f64() * (size as f64) * 1.5);

        Self {
            attempts: VecDeque::new(),
            size,
            max_age,
            min_delay,
            max_delay,
        }
    }

    fn next(&mut self) -> Duration {
        self.attempts.push_back(std::time::Instant::now());
        while let Some(attempt) = self.attempts.front() {
            if self.attempts.len() < self.size && attempt.elapsed() < self.max_age {
                break;
            }
            self.attempts.pop_front();
        }

        delay_for_retry(self.attempts.len(), self.min_delay, self.max_delay)
    }
}

/// Background is a set of owned tasks which are aborted on drop.
pub(crate) struct Background {
    tasks: std::sync::Arc<std::sync::Mutex<(u64, HashMap<u64, tokio::task::JoinHandle<()>>)>>,
}

impl Background {
    pub(crate) fn new() -> Self {
        Self {
            tasks: std::sync::Arc::new(std::sync::Mutex::new((0, HashMap::new()))),
        }
    }

    pub(crate) fn spawn<F: Future<Output = ()> + Send + 'static>(&self, f: F) {
        let mut guard = self.tasks.lock().unwrap();
        let id = guard.0;
        let tasks_arc = self.tasks.clone();
        let handle = tokio::task::spawn(async move {
            f.await;
            let mut guard = tasks_arc.lock().unwrap();
            guard.1.remove(&id);
        });
        guard.0 += 1;
        guard.1.insert(id, handle);
    }
}

impl Drop for Background {
    fn drop(&mut self) {
        for (_, handle) in self.tasks.lock().unwrap().1.drain() {
            handle.abort();
        }
    }
}

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

pub(crate) struct WaitableTimestamp {
    inner: RwLock<WaitableTimestampInner>,
}

struct WaitableTimestampInner {
    ts: Timestamp,
    waiters: BinaryHeap<OrdEqByFirst<Timestamp, tokio::sync::oneshot::Sender<()>>>,
}

impl WaitableTimestamp {
    pub(crate) fn new() -> Self {
        Self {
            inner: RwLock::new(WaitableTimestampInner {
                ts: Timestamp::ZERO,
                waiters: BinaryHeap::new(),
            }),
        }
    }

    pub(crate) fn set(&self, ts: Timestamp) {
        let mut inner = self.inner.write().unwrap();

        if inner.ts >= ts {
            return;
        }
        inner.ts = ts;

        while let Some(OrdEqByFirst(wait_ts, _)) = inner.waiters.peek() {
            if *wait_ts > inner.ts {
                break;
            }
            let OrdEqByFirst(_, sender) = inner.waiters.pop().unwrap();
            let _ = sender.send(());
        }
    }

    pub(crate) fn get(&self) -> Timestamp {
        self.inner.read().unwrap().ts
    }

    pub(crate) async fn wait(&self, ts: Timestamp) -> anyhow::Result<()> {
        {
            let inner = self.inner.read().unwrap();
            if inner.ts >= ts {
                return Ok(());
            }
        }
        let receiver = {
            let mut inner = self.inner.write().unwrap();
            if inner.ts >= ts {
                return Ok(());
            }
            let (sender, receiver) = tokio::sync::oneshot::channel();
            inner.waiters.push(OrdEqByFirst(ts, sender));
            receiver
        };
        receiver.await?;
        Ok(())
    }
}

impl From<BTreeSet<Key>> for pb::internal::CompressedKeySet {
    fn from(set: BTreeSet<Key>) -> Self {
        let mut keyspace_id_counts = HashMap::new();
        let mut key_to_keyspace_ids = BTreeMap::new();
        for (keyspace_id, key) in set {
            *(keyspace_id_counts.entry(keyspace_id).or_insert(0)) += 1;
            key_to_keyspace_ids
                .entry(key)
                .or_insert_with(Vec::new)
                .push(keyspace_id);
        }
        let mut keyspace_ids_by_pop = keyspace_id_counts.keys().copied().collect::<Vec<_>>();
        keyspace_ids_by_pop.sort_by_key(|keyspace_id| keyspace_id_counts.get(keyspace_id));
        let keyspace_id_to_idx = keyspace_ids_by_pop
            .iter()
            .enumerate()
            .map(|(i, keyspace_id)| (*keyspace_id, i))
            .collect::<HashMap<_, _>>();

        let mut key_fragments = vec![];
        let mut key_shared_prefixes = vec![];
        let mut maybe_prev_key = None;
        for key in key_to_keyspace_ids.keys() {
            let n_shared = match maybe_prev_key {
                Some(prev_key) => longest_shared_prefix_len(key, prev_key),
                None => 0,
            };

            key_fragments.push(key[n_shared..].to_vec());
            key_shared_prefixes.push(n_shared as u32);

            maybe_prev_key = Some(key);
        }

        let mut key_keyspaces_counts = vec![];
        let mut key_keyspaces_refs = vec![];
        if keyspace_id_to_idx.len() > 1 {
            for keyspace_ids in key_to_keyspace_ids.values() {
                let mut count = 0;
                for keyspace_id in keyspace_ids {
                    let idx = *(keyspace_id_to_idx.get(keyspace_id).unwrap());
                    count += 1;
                    key_keyspaces_refs.push(idx as u32);
                }
                key_keyspaces_counts.push(count);
            }
        }

        pb::internal::CompressedKeySet {
            keyspace_ids: keyspace_ids_by_pop
                .iter()
                .map(|keyspace_id| pb::KeyspaceId {
                    colo_group_id: keyspace_id.0 .0,
                    id: keyspace_id.1,
                })
                .collect(),
            key_fragments,
            key_shared_prefixes,
            key_keyspaces_counts,
            key_keyspaces_refs,
        }
    }
}

impl TryFrom<pb::internal::CompressedKeySet> for BTreeSet<Key> {
    type Error = anyhow::Error;

    fn try_from(value: pb::internal::CompressedKeySet) -> Result<Self, Self::Error> {
        let keyspace_ids = value
            .keyspace_ids
            .iter()
            .map(|keyspace_id_pb| {
                KeyspaceId(ColoGroupId(keyspace_id_pb.colo_group_id), keyspace_id_pb.id)
            })
            .collect::<Vec<_>>();

        if value.key_fragments.len() != value.key_shared_prefixes.len() {
            return Err(anyhow!(""));
        }

        let mut prev_key = vec![];
        let mut j = 0;
        let mut out = BTreeSet::new();
        for (i, key_fragment) in value.key_fragments.iter().enumerate() {
            let n_shared = value.key_shared_prefixes[i] as usize;
            let n_more = key_fragment.len();

            if n_shared > prev_key.len() {
                return Err(anyhow!(""));
            }

            let mut key = vec![0u8; n_shared + n_more];
            (key[..n_shared]).copy_from_slice(&prev_key[..n_shared]);
            (key[n_shared..]).copy_from_slice(&key_fragment);

            if keyspace_ids.len() == 1 {
                out.insert((keyspace_ids[0], key.clone()));
            } else {
                for _ in 0..value.key_keyspaces_counts[i] {
                    if j >= value.key_keyspaces_refs.len() {
                        return Err(anyhow!(""));
                    }

                    let idx = value.key_keyspaces_refs[j] as usize;
                    if idx >= keyspace_ids.len() {
                        return Err(anyhow!(""));
                    }

                    let keyspace_id = keyspace_ids[idx];
                    out.insert((keyspace_id, key.clone()));
                    j += 1;
                }
            }

            prev_key = key;
        }

        Ok(out)
    }
}
