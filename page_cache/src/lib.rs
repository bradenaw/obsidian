#![feature(ptr_as_ref_unchecked)]
#![feature(sync_unsafe_cell)]
#![feature(unsafe_cell_access)]

use std::cell::SyncUnsafeCell;
use std::cmp::min;
use std::collections::HashMap;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Deref;
use std::ptr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;
use std::thread;

use crossbeam::queue::ArrayQueue;
use crossbeam::sync::ShardedLock;

const PAGE_SIZE: usize = 4096;
type Page = [u8; PAGE_SIZE];

// Means the largest segment is ~256MiB.
const MAX_SEGMENT_SIZE: usize = 65536;

pub struct PageCache<K> {
    slab: Arc<Slab>,
    cache: Cache<K, Handle>,
}

impl<K: Eq + Hash + Clone> PageCache<K> {
    pub fn new(capacity: usize) -> Self {
        Self {
            slab: Arc::new(Slab::new()),
            cache: Cache::new(capacity, 16),
        }
    }

    pub fn insert(&self, k: K, page: Page) {
        self.cache.insert(k, self.slab.put(page));
    }

    pub fn get(&self, k: &K) -> Option<Handle> {
        self.cache.get(k)
    }

    pub fn remove(&self, k: &K) {
        self.cache.remove(k);
    }
}

struct CacheStats {
    in_use: usize,
    capacity: usize,
}

struct Cache<K, V> {
    shards: Vec<RwLock<CacheShard<K, V>>>,
}

impl<K: Eq + Hash + Clone, V: Clone> Cache<K, V> {
    fn new(capacity: usize, n_shards: usize) -> Self {
        let mut shards = Vec::with_capacity(n_shards);
        for _ in 0..n_shards {
            shards.push(RwLock::new(CacheShard::new(capacity / n_shards)));
        }
        Self { shards }
    }

    fn insert(&self, k: K, v: V) {
        self.shard_for(&k).write().unwrap().insert(k, v)
    }

    fn get(&self, k: &K) -> Option<V> {
        self.shard_for(k).read().unwrap().get(k)
    }

    fn remove(&self, k: &K) {
        self.shard_for(k).write().unwrap().remove(k)
    }

    fn shard_for(&self, k: &K) -> &RwLock<CacheShard<K, V>> {
        &self.shards[(hash(k) % (self.shards.len() as u64)) as usize]
    }
}

struct CacheShard<K, V> {
    m: HashMap<K, usize>,
    entries: Vec<CacheEntry<K, V>>,
    hand: usize,
    capacity: usize,
}

struct CacheEntry<K, V> {
    kv: Option<(K, V)>,
    touched: AtomicBool,
}

impl<K: Eq + Hash + Clone, V: Clone> CacheShard<K, V> {
    fn new(capacity: usize) -> Self {
        Self {
            m: HashMap::new(),
            entries: Vec::new(),
            hand: 0,
            capacity: capacity,
        }
    }

    fn insert(&mut self, k: K, v: V) {
        if let Some(idx) = self.m.get(&k) {
            let entry = &mut self.entries[*idx];
            entry.kv = Some((k, v));
            entry.touched.store(true, Ordering::SeqCst);
            return;
        }

        if self.m.len() < self.capacity {
            self.entries.push(CacheEntry {
                kv: Some((k.clone(), v)),
                touched: AtomicBool::new(true),
            });
            self.m.insert(k, self.entries.len() - 1);
            return;
        }

        loop {
            let idx = self.hand;
            let entry = &mut self.entries[idx];
            self.hand = (self.hand + 1) % self.m.len();
            if !entry.touched.swap(false, Ordering::SeqCst) {
                if let Some((k, _)) = &entry.kv {
                    self.m.remove(&k);
                }

                self.m.insert(k.clone(), idx);
                entry.kv = Some((k, v));
                entry.touched.store(true, Ordering::SeqCst);

                return;
            }
        }
    }

    fn get(&self, k: &K) -> Option<V> {
        let idx = self.m.get(k)?;
        let entry = &self.entries[*idx];
        entry.touched.store(true, Ordering::SeqCst);
        Some(entry.kv.as_ref()?.1.clone())
    }

    fn remove(&mut self, k: &K) {
        if let Some(idx) = self.m.remove(k) {
            self.entries[idx].kv = None;
            self.entries[idx].touched.store(false, Ordering::SeqCst);
        }
    }
}

struct Slab {
    segments: ShardedLock<Vec<Arc<Segment>>>,
}

impl Slab {
    fn new() -> Self {
        Self {
            segments: ShardedLock::new(Vec::from([Arc::new(Segment::new(4096))])),
        }
    }

    fn put(self: &Arc<Self>, val: Page) -> Handle {
        {
            let segments = self.segments.read().unwrap();
            for segment in segments.deref().iter().rev() {
                if let Some(handle) = segment.try_put(val) {
                    return handle;
                }
            }
        }
        {
            let mut segments = self.segments.write().unwrap();
            loop {
                // Somebody could've beaten us to it.
                if let Some(handle) = segments.last().unwrap().try_put(val) {
                    return handle;
                }

                let size = min(segments.last().unwrap().items.len() * 2, MAX_SEGMENT_SIZE);
                segments.push(Arc::new(Segment::new(size)));
            }
        }
    }

    fn stats(&self) -> SlabStats {
        let segments = self.segments.read().unwrap();
        let mut in_use = 0;
        let mut capacity = 0;
        let mut segments_stats = Vec::new();
        for segment in segments.deref() {
            let segment_stats = segment.stats();

            in_use += segment_stats.in_use;
            capacity += segment_stats.capacity;
            segments_stats.push(segment_stats);
        }

        SlabStats {
            in_use,
            capacity,
            segments: segments_stats,
        }
    }
}

struct SlabStats {
    in_use: usize,
    capacity: usize,

    segments: Vec<SegmentStats>,
}

struct Segment {
    items: Vec<SyncUnsafeCell<Page>>,
    ref_counts: Vec<AtomicUsize>,
    free: ArrayQueue<usize>,
}

impl Segment {
    fn new(size: usize) -> Self {
        let mut items = Vec::with_capacity(size);
        let mut ref_counts = Vec::with_capacity(size);
        let free = ArrayQueue::new(size);
        for i in 0..size {
            free.force_push(i);
            items.push(SyncUnsafeCell::new([0; PAGE_SIZE]));
            ref_counts.push(AtomicUsize::new(0));
        }

        Self {
            items,
            ref_counts,
            free,
        }
    }

    fn try_put(self: &Arc<Self>, val: Page) -> Option<Handle> {
        let idx = self.free.pop()?;
        unsafe {
            self.items[idx].replace(val);
        }
        assert_eq!(self.ref_counts[idx].fetch_add(1, Ordering::SeqCst), 0);
        Some(Handle {
            parent: self.clone(),
            idx: idx,
        })
    }

    fn stats(&self) -> SegmentStats {
        SegmentStats {
            in_use: self.items.len() - self.free.len(),
            capacity: self.items.len(),
        }
    }
}

struct SegmentStats {
    in_use: usize,
    capacity: usize,
}

#[derive(Clone)]
pub struct Handle {
    parent: Arc<Segment>,
    idx: usize,
}

impl Handle {
    pub fn get(&self) -> &Page {
        unsafe { self.parent.items[self.idx].as_ref_unchecked() }
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        let prev_remaining = self.parent.ref_counts[self.idx].fetch_sub(1, Ordering::SeqCst);
        if prev_remaining == 1 {
            self.parent.free.push(self.idx).unwrap();
        }
    }
}

struct ShardedCounter {
    counters: [AtomicUsize; 8],
}

impl ShardedCounter {
    fn add(&self, x: usize) {
        self.counters[(hash(thread::current().id()) % 8) as usize].fetch_add(x, Ordering::SeqCst);
    }

    fn load(&self) -> usize {
        self.counters.iter().map(|c| c.load(Ordering::SeqCst)).sum()
    }
}

fn hash<T: Hash>(t: T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

trait SyncUnsafeCellExt<T> {
    unsafe fn replace(&self, value: T) -> T;
    unsafe fn as_ref_unchecked(&self) -> &T;
}

impl<T> SyncUnsafeCellExt<T> for SyncUnsafeCell<T> {
    #[inline]
    unsafe fn replace(&self, value: T) -> T {
        // SAFETY: pointer comes from `&self` so naturally satisfies invariants.
        unsafe { ptr::replace(self.get(), value) }
    }

    #[inline]
    unsafe fn as_ref_unchecked(&self) -> &T {
        // SAFETY: pointer comes from `&self` so naturally satisfies ptr-to-ref invariants.
        unsafe { self.get().as_ref_unchecked() }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::thread::sleep;
    use std::time::Duration;
    use std::time::Instant;

    use crossbeam::sync::WaitGroup;
    use sha2::Digest;
    use sha2::Sha256;

    use super::*;

    #[test]
    fn test_stress() {
        let cache = Arc::new(PageCache::new(1_000_000));

        let wg = WaitGroup::new();
        let start = Instant::now();

        const WRITERS: usize = 8;
        const READERS: usize = 1;
        const DURATION: Duration = Duration::from_secs(3);

        let counters = Arc::new({
            let mut counters = Vec::with_capacity(WRITERS);
            for _ in 0..WRITERS {
                counters.push(AtomicUsize::new(0));
            }
            counters
        });

        for i in 0..WRITERS {
            let cache = cache.clone();
            let wg = wg.clone();
            let counters = counters.clone();

            thread::spawn(move || {
                let mut j = 0;
                loop {
                    for _ in 0..128 {
                        cache.insert((i, j), make_page());
                        counters[i].store(j, Ordering::SeqCst);
                        j += 1
                    }

                    if start.elapsed() > DURATION {
                        break;
                    }
                }
                drop(wg);
            });
        }

        for _ in 0..READERS {
            let cache = cache.clone();
            let wg = wg.clone();
            let counters = counters.clone();

            thread::spawn(move || {
                loop {
                    for _ in 0..128 {
                        let i = rand::random_range(0..WRITERS);
                        let max_j = counters[i].load(Ordering::SeqCst);
                        if max_j == 0 {
                            sleep(Duration::from_millis(10));
                            continue;
                        }
                        let j = rand::random_range(0..max_j);
                        if let Some(handle) = cache.get(&(i, j)) {
                            check_page(handle.get());
                        }
                    }

                    if start.elapsed() > DURATION {
                        break;
                    }
                }
                drop(wg);
            });
        }

        wg.wait();
    }

    fn make_page() -> Page {
        let mut page = [0u8; PAGE_SIZE];
        //rand::fill(&mut page);

        //let mut h = Sha256::new();
        //h.update(&page[0..PAGE_SIZE - 32]);
        //let hash = h.finalize();
        //page[PAGE_SIZE - 32..].copy_from_slice(&hash);

        let x = rand::random_range(0..256);
        page.fill(x as u8);

        page
    }

    fn check_page(page: &Page) {
        let x = page[0];
        assert_eq!(page, &[x; PAGE_SIZE]);
        //let mut h = Sha256::new();
        //h.update(&page[0..PAGE_SIZE - 32]);
        //let hash = h.finalize();

        //assert_eq!(&page[PAGE_SIZE - 32..], hash.as_slice());
    }
}
