use std::cmp;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::RandomState;
use std::mem::MaybeUninit;
use std::pin::pin;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use tokio::io::AsyncRead;
use tokio::io::ReadBuf;

use crate::storage::FileReader;
use crate::storage::Storage;

/// CachedStorage wraps another implementation of `Storage`, holding pages in an
/// approximately-LRU in-memory cache.
pub(crate) struct CachedStorage<S: Storage + Sync> {
    inner: S,
    page_size: usize,

    // Pages are cached by the name of their file and the offset of the start of the page.
    //
    // Values are at most `page_size`, but might be shorter at the end of a file.
    cache: Arc<Cache<(Arc<String>, u64), Arc<Vec<u8>>>>,
}

impl<S: Storage + Sync> CachedStorage<S> {
    /// `page_size` is the size of reads issued to `inner` and the size of the cached pages.
    ///
    /// The cache can hold up to `stripe_size_pages * n_stripes` pages.  Note that there is some
    /// overhead per-page, so the cache takes more memory than `page_size * stripe_size_pages *
    /// n_stripes`.
    ///
    /// `n_stripes` determines how many stripes to split the cache into. Higher `n_stripes` makes
    /// the cache more efficient at concurrent access but slightly less accurate in its evicition.
    pub(crate) fn new(
        inner: S,
        page_size: usize,
        stripe_size_pages: usize,
        n_stripes: usize,
    ) -> Self {
        Self {
            inner,
            page_size,
            cache: Arc::new(Cache::new(stripe_size_pages, n_stripes)),
        }
    }
}

#[async_trait]
impl<S: Storage + Sync> Storage for CachedStorage<S> {
    type R = GetCacher<S::R>;

    // Assuming that:
    // 1. Objects are immutable, i.e. put() with an object that exists will fail.
    // 2. Names are globally unique and never reused.
    // 3. inner.get() errors if an object has been deleted.
    //
    // Then it's perfectly safe to not actually make any changes to the cache from put/delete.

    async fn put<C: AsyncRead + Send>(&self, name: &str, content: C) -> anyhow::Result<()> {
        let mut backing: Box<[MaybeUninit<u8>]> = Box::new_uninit_slice(self.page_size);
        let buf = ReadBuf::uninit(&mut backing);

        let mut wrapper = PutCacher {
            inner: Box::pin(content),
            cache: self.cache.clone(),
            name: Arc::new(name.to_string()),
            buf: buf,
            buf_offset: 0,
            cursor: 0,
            inner_done: false,
        };

        self.inner.put(name, &mut wrapper).await
    }

    async fn get(&self, name: &str) -> anyhow::Result<Self::R> {
        let f = self.inner.get(name).await?;
        let len = f.len().await?;
        Ok(GetCacher {
            inner: f,
            len: len,
            page_size: self.page_size,
            name: Arc::new(name.to_string()),
            cache: self.cache.clone(),
        })
    }

    async fn delete(&self, name: &str) -> anyhow::Result<()> {
        self.inner.delete(name).await
    }
}

// Caches pages while a file is being put.
//
// put() takes in a stream of bytes, so we have to wrap the reader of that stream.
struct PutCacher<'a, C: AsyncRead + Send> {
    inner: C,
    // The name of the file being read.
    name: Arc<String>,
    // A reference to the cache from the CachedStorage this PutCacher was made from.
    cache: Arc<Cache<(Arc<String>, u64), Arc<Vec<u8>>>>,
    // Must have `buf.capacity() == page_size`.
    buf: ReadBuf<'a>,
    // The offset of the bytes contained in `buf` from the start of the file.
    buf_offset: u64,
    // The reader's cursor, in `[0..buf.capacity()]`.
    cursor: usize,
    // True if inner doesn't have any more bytes to give, so the read is over once the reader
    // consumes what's left in `buf`.
    inner_done: bool,
}

impl<'a, C: AsyncRead + Send + Unpin> PutCacher<'a, C> {
    fn flush_page(&mut self) {
        if self.buf.filled().len() == 0 {
            return;
        }

        let page = Arc::new(self.buf.filled().to_vec());
        println!(
            "putting a page ({}, {}) with size {}",
            self.name,
            self.buf_offset,
            page.len()
        );
        self.cache
            .insert((self.name.clone(), self.buf_offset), page);
        self.buf_offset += self.buf.capacity() as u64;
        self.buf.set_filled(0);
        self.cursor = 0;
    }

    fn poll_read_inner(
        &mut self,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        loop {
            // See if we have any bytes to give to the caller.
            let n = cmp::min(self.buf.filled().len() - self.cursor, buf.remaining());
            if n > 0 {
                buf.put_slice(&self.buf.filled()[self.cursor..self.cursor + n]);
                self.cursor += n;

                // If that means the reader just finished a whole page, place it into cache.
                if self.cursor == self.buf.capacity() {
                    self.flush_page();
                }

                return std::task::Poll::Ready(Ok(()));
            }

            // If we're here, it implies that the cursor is at the end of our buf, we need to get
            // some more bytes from inner.

            let start_len = self.buf.filled().len();
            let inner = pin!(&mut self.inner);
            let inner_poll = inner.poll_read(cx, &mut self.buf);
            if let std::task::Poll::Ready(Ok(())) = inner_poll {
                if start_len == self.buf.filled().len() {
                    // No more bytes from inner, so we're done reading.
                    self.flush_page();
                    return std::task::Poll::Ready(Ok(()));
                }

                // We got some bytes back, so loop back around to see if we can give them to the
                // caller.
                continue;
            }

            return inner_poll;
        }
    }
}

impl<'a, C: AsyncRead + Send + Unpin> AsyncRead for PutCacher<'a, C> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let self_ = Pin::get_mut(self);
        self_.poll_read_inner(cx, buf)
    }
}

// Wraps read_exact_at in a cache.
#[derive(Clone)]
pub(crate) struct GetCacher<R: FileReader + Sync + Clone> {
    inner: R,
    page_size: usize,
    len: u64,
    name: Arc<String>,
    // A reference to the cache from the CachedStorage this GetCacher was made from.
    cache: Arc<Cache<(Arc<String>, u64), Arc<Vec<u8>>>>,
}

#[async_trait]
impl<R: FileReader + Sync + Clone> FileReader for GetCacher<R> {
    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<()> {
        let end_offset = offset + (buf.len() as u64);

        // This might be unaligned with the cache lines. Here's an example:
        //
        //   pages | page_0        | page_4096     | page_8192     | page_12288    |
        //   read          |                             |
        //
        // read 7680 bytes at offset 2816
        // buf[0..1280] = page_0[2816..4096]
        // buf[1280..5376] = page_1[..]
        // buf[5376..7680] = page_2[..2034]

        let mut current_offset = offset;
        while current_offset < end_offset {
            // Figure out what page the next bytes we need are in.
            let page_offset = current_offset - (current_offset % (self.page_size as u64));

            // Attempt to fetch from cache, fall through and populate on a miss.
            let page = match self.cache.get(&(self.name.clone(), page_offset)) {
                Some(b) => b,
                None => {
                    let mut page =
                        vec![0u8; cmp::min(self.page_size as u64, self.len - page_offset) as usize];
                    self.inner.read_exact_at(&mut page, page_offset).await?;
                    let page_arc = Arc::new(page);
                    self.cache
                        .insert((self.name.clone(), page_offset), page_arc.clone());
                    page_arc
                }
            };

            // How many bytes are we taking from this page? In other words, how many bytes are
            // there between current_offset and either the end of the page or the end of the read,
            // whichever comes first.
            let n = (cmp::min(page_offset + (self.page_size as u64), end_offset) - current_offset)
                as usize;

            let offset_in_buf = (current_offset - offset) as usize;
            let offset_in_page = (current_offset - page_offset) as usize;

            if offset_in_page + n > Vec::len(&page) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "unexpected eof",
                )
                .into());
            }

            buf[offset_in_buf..offset_in_buf + n]
                .copy_from_slice(&page[offset_in_page..offset_in_page + n]);

            current_offset += n as u64;
        }

        Ok(())
    }

    async fn len(&self) -> anyhow::Result<u64> {
        self.inner.len().await
    }
}

// Cache is a basic striped-clock.
//
// Items are placed into stripes by the hash of their key. Operations contend only with operations
// in the same stripe, so more stripes are better. However, eviction is also per-stripe, which
// means with many stripes relative to the number of keys, clock won't approximate LRU very well.
struct Cache<K, V> {
    random_state: RandomState,
    stripes: Vec<RwLock<CacheStripe<K, V>>>,
}

impl<K: Eq + Hash + Clone, V: Clone> Cache<K, V> {
    fn new(capacity: usize, n_stripes: usize) -> Self {
        // We don't want to create more actual capacity than `capacity` and we don't want any empty
        // stripes, so min here to make sure every stripe gets at least one.
        let n_stripes = cmp::min(n_stripes, capacity);
        let mut stripes = Vec::with_capacity(n_stripes);

        // The last stripe might be smaller if `capacity % n_stripes != 0`.
        let max_size_per_stripe = capacity.div_ceil(n_stripes);
        let mut capacity_remaining = capacity;

        for _ in 0..n_stripes {
            let stripe_size = cmp::min(max_size_per_stripe, capacity_remaining);
            stripes.push(RwLock::new(CacheStripe::new(stripe_size)));
            capacity_remaining -= stripe_size;
        }

        Self {
            random_state: RandomState::new(),
            stripes,
        }
    }

    fn insert(&self, k: K, v: V) {
        self.stripe_for(&k).write().unwrap().insert(k, v)
    }

    fn get(&self, k: &K) -> Option<V> {
        self.stripe_for(k).read().unwrap().get(k)
    }

    fn remove(&self, k: &K) {
        self.stripe_for(k).write().unwrap().remove(k)
    }

    fn evictions(&self) -> usize {
        let mut n = 0;
        for stripe in &self.stripes {
            n += stripe.read().unwrap().evictions;
        }
        n
    }

    fn stripe_for(&self, k: &K) -> &RwLock<CacheStripe<K, V>> {
        &self.stripes[(self.random_state.hash_one(k) % (self.stripes.len() as u64)) as usize]
    }
}

struct CacheStripe<K, V> {
    capacity: usize,

    // Map from key to index in entries. Always the same size as entries and contains the same
    // keys.
    m: HashTrie<K, usize>,
    // None for vacant entries left behind by `remove()`.
    entries: TreeList<Option<CacheEntry<K, V>>>,
    // Index of the clock hand, in 0..entries.len().
    hand: usize,
    evictions: usize,
}

struct CacheEntry<K, V> {
    k: K,
    v: V,
    touched: AtomicBool,
}

impl<K: Eq + Hash + Clone, V: Clone> CacheStripe<K, V> {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity,
            m: HashTrie::new(),
            entries: TreeList::new(),
            hand: 0,
            evictions: 0,
        }
    }

    fn insert(&mut self, k: K, v: V) {
        // k is already in the cache, so replace the entry that's already here.
        if let Some(idx) = self.m.get(&k) {
            let entry = &mut self.entries[*idx];
            entry.replace(CacheEntry {
                k,
                v,
                touched: AtomicBool::new(true),
            });
            return;
        }

        // There's still room, so just add an entry.
        if self.m.len() < self.capacity {
            self.entries.push(Some(CacheEntry {
                k: k.clone(),
                v,
                touched: AtomicBool::new(true),
            }));
            self.m.insert(k, self.entries.len() - 1);
            return;
        }

        // Else we need to find someone to evict to make room.
        loop {
            let idx = self.hand;
            let maybe_entry = &mut self.entries[idx];
            // Advance the hand unconditionally. We want it to be past the value we're putting now,
            // since this is now the newest thing in the cache.
            self.hand = (self.hand + 1) % self.m.len();
            if let Some(entry) = maybe_entry {
                if entry.touched.swap(false, Ordering::SeqCst) {
                    // That entry was touched recently, so move on to the next one.
                    continue;
                }
            }
            // We're either at a vacant entry or one that hasn't been touched recently, the perfect
            // place to put our new item.
            if let Some(prev_entry) = maybe_entry.replace(CacheEntry {
                k: k.clone(),
                v: v,
                touched: AtomicBool::new(true),
            }) {
                self.evictions += 1;
                self.m.remove(&prev_entry.k);
            }
            self.m.insert(k, idx);
            return;
        }
    }

    fn get(&self, k: &K) -> Option<V> {
        let idx = self.m.get(k)?;
        match &self.entries[*idx] {
            Some(entry) => {
                entry.touched.store(true, Ordering::SeqCst);
                Some(entry.v.clone())
            }
            None => return None,
        }
    }

    fn remove(&mut self, k: &K) {
        if let Some(idx) = self.m.remove(k) {
            self.entries[idx] = None;
        }
    }
}

// HashTrie behaves mostly like a HashMap, but does not have the amortized growth problem
// (occasional very slow inserts in a large map) at the cost of being a little slower.
struct HashTrie<K, V> {
    random_state: RandomState,
    root: HashTrieNode<K, V>,
    len: usize,
}

// The number of bits of the hash we use per level of the trie.
const HASH_TRIE_BITS_PER_LEVEL: usize = 4;
// How many children does each node need to have in order to use up HASH_TRIE_BITS_PER_LEVEL?
const HASH_TRIE_BRANCH_FACTOR: usize = 1 << HASH_TRIE_BITS_PER_LEVEL;
// Leaves can't be any deeper than this because there aren't any more hash bits to differentiate.
// Won't happen unless the hash is really weak.
const HASH_TRIE_MAX_DEPTH: usize = 64 / HASH_TRIE_BITS_PER_LEVEL;
// How many K,V pairs can a leaf hold before we split it into HASH_TRIE_BRANCH_FACTOR nodes?
//
// Since we have to make HASH_TRIE_BRANCH_FACTOR new hashmaps and re-hash this many contents in
// order to do the split, this determines approximately how expensive the most expensive write is,
// but setting it larger will mean slightly cheaper reads because the tree depth is lower.
const HASH_TRIE_LEAF_MAX: usize = 16384;

enum HashTrieNode<K, V> {
    Internal([Option<Box<HashTrieNode<K, V>>>; HASH_TRIE_BRANCH_FACTOR]),
    Leaf(HashMap<K, V>),
}

impl<K: Eq + Hash, V> HashTrie<K, V> {
    fn new() -> Self {
        Self {
            random_state: RandomState::new(),
            root: HashTrieNode::Leaf(HashMap::new()),
            len: 0,
        }
    }

    fn insert(&mut self, k: K, v: V) {
        let h = self.random_state.hash_one(&k);

        fn insert_inner<K: Eq + Hash, V>(
            random_state: &RandomState,
            node: &mut HashTrieNode<K, V>,
            depth: usize,
            h: u64,
            k: K,
            v: V,
        ) -> bool {
            match node {
                HashTrieNode::Internal(children) => {
                    let child_idx = HashTrie::<K, V>::child_idx(h, depth);
                    let child = children[child_idx]
                        .get_or_insert_with(|| Box::new(HashTrieNode::Leaf(HashMap::new())));

                    return insert_inner(random_state, child, depth + 1, h, k, v);
                }
                HashTrieNode::Leaf(m) => {
                    // If we're this deep then we're out of hash bits, so we don't have any choice
                    // but to just let the leaf get bigger. This isn't realistically a problem
                    // unless the hash function is very poor.
                    if depth == HASH_TRIE_MAX_DEPTH
                        || m.len() < HASH_TRIE_LEAF_MAX
                        || m.contains_key(&k)
                    {
                        if let None = m.insert(k, v) {
                            return true;
                        }
                        return false;
                    }

                    // We didn't insert because we need to split the node.

                    let mut new_leaves: [Option<HashMap<K, V>>; HASH_TRIE_BRANCH_FACTOR] =
                        [const { None }; HASH_TRIE_BRANCH_FACTOR];
                    for (other_k, other_v) in m.drain() {
                        let other_h = random_state.hash_one(&other_k);

                        let child_idx = HashTrie::<K, V>::child_idx(other_h, depth);

                        new_leaves[child_idx as usize]
                            .get_or_insert_with(|| HashMap::new())
                            .insert(other_k, other_v);
                    }

                    let children =
                        new_leaves.map(|maybe_m| maybe_m.map(|m| Box::new(HashTrieNode::Leaf(m))));
                    *node = HashTrieNode::Internal(children);

                    // After split, revisit the same node to actually insert k,v.
                    return insert_inner(random_state, node, depth, h, k, v);
                }
            }
        }

        let inserted = insert_inner(&self.random_state, &mut self.root, 0, h, k, v);
        if inserted {
            self.len += 1;
        }
    }

    fn get(&self, k: &K) -> Option<&V> {
        let mut curr = &self.root;
        let mut depth = 0;
        let h = self.random_state.hash_one(&k);
        loop {
            match curr {
                HashTrieNode::Internal(children) => {
                    let child_idx = Self::child_idx(h, depth);
                    match &children[child_idx] {
                        Some(node) => {
                            curr = node;
                            depth += 1;
                        }
                        None => return None,
                    }
                }
                HashTrieNode::Leaf(m) => {
                    return m.get(k);
                }
            }
        }
    }

    fn remove(&mut self, k: &K) -> Option<V> {
        let h = self.random_state.hash_one(&k);

        fn remove_inner<K: Hash + Eq, V>(
            node: &mut HashTrieNode<K, V>,
            depth: usize,
            h: u64,
            k: &K,
        ) -> Option<V> {
            match node {
                HashTrieNode::Internal(children) => {
                    let child_idx = HashTrie::<K, V>::child_idx(h, depth);
                    let child = (&mut children[child_idx]).as_mut()?;
                    let result = remove_inner(child, depth + 1, h, k);

                    if result.is_some() {
                        // If we did actually remove something, then see if we should merge nodes.
                        //
                        // That is, if:
                        // 1. All of our children are leaves or vacant, and
                        // 2. The sum of their sizes is less than HASH_TRIE_LEAF_MAX/2 (smaller
                        //    than HASH_TRIE_LEAF_MAX to keep from thrashing on repeated
                        //    insert/remove)
                        // then merge them all together and replace this node with a leaf.
                        let mut all_leaves_or_vacant = true;
                        let mut n_grandchildren = 0;
                        for (leaf_or_vacant, n) in children.iter().map(|maybe_child| {
                            maybe_child
                                .as_ref()
                                .map(|child| match &**child {
                                    HashTrieNode::Leaf(m) => (true, m.len()),
                                    HashTrieNode::Internal(_) => (false, 0),
                                })
                                .unwrap_or((true, 0))
                        }) {
                            if !leaf_or_vacant {
                                all_leaves_or_vacant = false;
                                break;
                            }
                            n_grandchildren += n;
                        }

                        if all_leaves_or_vacant && n_grandchildren < HASH_TRIE_LEAF_MAX / 2 {
                            let all_grandchildren = children
                                .into_iter()
                                .filter_map(|maybe_child| {
                                    maybe_child.take().map(|child| match *child {
                                        HashTrieNode::Leaf(m) => m.into_iter(),
                                        // Checked by all_leaves_or_vacant above.
                                        HashTrieNode::Internal(_) => unreachable!(),
                                    })
                                })
                                .flatten();

                            let mut m = HashMap::with_capacity(n_grandchildren);
                            for (k, v) in all_grandchildren {
                                m.insert(k, v);
                            }
                            *node = HashTrieNode::Leaf(m);
                        }
                    }

                    result
                }
                HashTrieNode::Leaf(m) => {
                    let result = m.remove(k);

                    result
                }
            }
        }

        let result = remove_inner(&mut self.root, 0 /*depth*/, h, k);
        if result.is_some() {
            self.len -= 1;
        }
        result
    }

    fn len(&self) -> usize {
        self.len
    }

    fn child_idx(h: u64, depth: usize) -> usize {
        const MASK: u64 = (HASH_TRIE_BRANCH_FACTOR as u64) - 1;
        let shift = 64 - (depth + 1) * HASH_TRIE_BITS_PER_LEVEL;

        ((h >> shift) & MASK) as usize
    }
}

const TREE_LIST_NODE_SIZE: usize = 16384;

// TreeList mostly behaves like a Vec<T> but it has non-amortized inserts at the cost of log(n)
// accesses.
struct TreeList<T> {
    // Map from page index to page. BTreeMap just because it grows gracefully in log(n) time on
    // inserts.
    m: BTreeMap<usize, Vec<T>>,
    len: usize,
}

impl<T> TreeList<T> {
    fn new() -> Self {
        Self {
            m: BTreeMap::new(),
            len: 0,
        }
    }

    fn push(&mut self, item: T) {
        self.m
            .entry(self.len / TREE_LIST_NODE_SIZE)
            .or_insert_with(|| Vec::new())
            .push(item);
        self.len += 1;
    }

    fn len(&self) -> usize {
        self.len
    }
}

impl<T> std::ops::Index<usize> for TreeList<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        if index >= self.len {
            panic!("out of bounds");
        }
        &self.m[&(index / TREE_LIST_NODE_SIZE)][index % TREE_LIST_NODE_SIZE]
    }
}

impl<T> std::ops::IndexMut<usize> for TreeList<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        if index >= self.len {
            panic!("out of bounds");
        }
        &mut self.m.get_mut(&(index / TREE_LIST_NODE_SIZE)).unwrap()[index % TREE_LIST_NODE_SIZE]
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::hash::BuildHasher;
    use std::hash::Hash;
    use std::hash::RandomState;

    use super::Cache;
    use super::CachedStorage;
    use super::FileReader;
    use super::HashTrie;
    use super::HashTrieNode;
    use super::Storage;
    use super::TreeList;
    use super::HASH_TRIE_LEAF_MAX;
    use super::TREE_LIST_NODE_SIZE;
    use crate::storage::MemStorage;

    #[tokio::test]
    async fn test_cached_storage() -> anyhow::Result<()> {
        const PAGE_SIZE: usize = 16;
        const STRIPE_SIZE_PAGES: usize = 8;
        const N_STRIPES: usize = 2;
        const CAPACITY_PAGES: usize = STRIPE_SIZE_PAGES * N_STRIPES;

        let storage = CachedStorage::new(
            MemStorage::new(),
            PAGE_SIZE,         // page_size
            STRIPE_SIZE_PAGES, // stripe_size_pages
            N_STRIPES,         // n_stripes
        );

        fn make_test_file(len: usize) -> Vec<u8> {
            let mut content = vec![0u8; len];
            for i in 0..len {
                content[i] = (i % 7) as u8;
            }
            content
        }

        let test_files = BTreeMap::from([
            ("foo", make_test_file(55)),
            ("bar", make_test_file(127)),
            ("baz", make_test_file(74)),
        ]);

        for (name, content) in &test_files {
            storage.put(name, &content[..]).await?;
        }

        let n_pages = test_files
            .values()
            .map(|v| v.len().div_ceil(PAGE_SIZE))
            .sum::<usize>();

        // We might end up with _more_ evictions than this because of striping.
        assert!(storage.cache.evictions() > n_pages - CAPACITY_PAGES);

        let check = async |name, offset, size| -> anyhow::Result<()> {
            println!("reading {} [{}..{}]", name, offset, offset + size);
            let expected = &test_files[name][offset..offset + size];
            let mut actual = vec![0u8; size];
            storage
                .get(name)
                .await?
                .read_exact_at(&mut actual, offset as u64)
                .await?;

            assert_eq!(&actual[..], expected);

            Ok(())
        };

        check("foo", 0, 16).await?;
        check("bar", 16, 5).await?;
        check("bar", 125, 2).await?;
        check("bar", 0, 16).await?;
        check("bar", 5, 2).await?;
        check("foo", 54, 1).await?;
        check("foo", 0, 32).await?;
        check("foo", 4, 15).await?;
        check("bar", 100, 11).await?;
        check("bar", 59, 55).await?;

        Ok(())
    }

    #[test]
    fn test_cache_insert_remove() {
        let c: Cache<&str, usize> = Cache::new(10 /*capacity*/, 1 /*n_stripes*/);

        c.insert("hello", 5);
        assert_eq!(c.get(&"hello"), Some(5));
        c.remove(&"hello");
        assert_eq!(c.get(&"hello"), None);
    }

    #[test]
    fn test_cache_put_put() {
        let c: Cache<&str, usize> = Cache::new(10 /*capacity*/, 1 /*n_stripes*/);

        c.insert("hello", 5);
        assert_eq!(c.get(&"hello"), Some(5));
        c.insert("hello", 6);
        assert_eq!(c.get(&"hello"), Some(6));
    }

    #[test]
    fn test_cache_evict() {
        let c: Cache<&str, usize> = Cache::new(4 /*capacity*/, 1 /*n_stripes*/);

        c.insert("foo", 1);
        c.insert("bar", 2);
        c.insert("baz", 3);
        c.insert("qux", 4);

        c.insert("quux", 5);
        // "foo" got evicted because it's the oldest.
        assert_eq!(c.get(&"foo"), None);
        assert_eq!(c.get(&"bar"), Some(2));
        // Deliberately leaving this one out so that it stays touched=false.
        // assert_eq!(c.get(&"baz"), Some(3));
        assert_eq!(c.get(&"qux"), Some(4));
        assert_eq!(c.get(&"quux"), Some(5));

        c.insert("garply", 6);
        // baz was the only one we didn't get() above.
        assert_eq!(c.get(&"baz"), None);
        assert_eq!(c.get(&"garply"), Some(6));
    }

    #[test]
    fn test_stripes() {
        let c: Cache<usize, usize> = Cache::new(8 /*capacity*/, 2 /*n_stripes*/);

        for i in 0..24 {
            c.insert(i, i);
        }

        let mut n_present = 0;
        for i in 0..24 {
            let result = c.get(&i);
            if let Some(v) = result {
                assert_eq!(i, v);
                n_present += 1;
            }
        }

        assert_eq!(n_present, 8);
    }

    #[test]
    fn test_tree_list() {
        let mut list = TreeList::new();

        let n = TREE_LIST_NODE_SIZE * 2;

        for i in 0..n {
            list.push(i);
        }

        for i in 0..n {
            assert_eq!(list[i], i);
        }

        for i in 0..n {
            list[i] = i * 5;
        }

        for i in 0..n {
            assert_eq!(list[i], i * 5);
        }
    }

    #[test]
    fn test_hash_trie_small() {
        let mut m = HashTrie::new();

        for i in 0..HASH_TRIE_LEAF_MAX {
            m.insert(i, i * 2);
        }

        assert_eq!(m.len(), HASH_TRIE_LEAF_MAX);

        for i in 0..HASH_TRIE_LEAF_MAX {
            assert_eq!(m.get(&i), Some(&(i * 2)));
        }
    }

    #[test]
    fn test_hash_trie_split_small() {
        let mut m = HashTrie::new();

        let n = HASH_TRIE_LEAF_MAX + 1;

        for i in 0..n {
            m.insert(i, i * 2);
        }

        assert_eq!(m.len(), n);

        for i in 0..n {
            assert_eq!(m.get(&i), Some(&(i * 2)));
        }
    }

    #[test]
    fn test_hash_trie_split_large() {
        let mut m = HashTrie::new();

        let n = HASH_TRIE_LEAF_MAX * 12;

        for i in 0..n {
            m.insert(i, i * 2);
        }

        assert_eq!(m.len(), n);

        for i in 0..n {
            assert_eq!(m.get(&i), Some(&(i * 2)));
        }
    }

    #[test]
    fn test_hash_trie_merge_large() {
        let mut m = HashTrie::new();

        let n = HASH_TRIE_LEAF_MAX * 12;

        for i in 0..n {
            m.insert(i, i * 2);
        }

        assert_eq!(m.len(), n);

        for i in 12..n {
            m.remove(&i);
        }

        assert_eq!(m.len(), 12);

        for i in 0..12 {
            assert_eq!(m.get(&i), Some(&(i * 2)));
        }

        assert_eq!(m.get(&13), None);

        match m.root {
            HashTrieNode::Leaf(_) => {}
            _ => {
                panic!("HashTrie didn't shrink as expected")
            }
        }
    }

    #[test]
    fn test_hash_trie_child_idx() {
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x123456789abcdef5, 0),
            0x1
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x123456789abcdef5, 1),
            0x2
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x123456789abcdef5, 2),
            0x3
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x123456789abcdef5, 3),
            0x4
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x123456789abcdef5, 4),
            0x5
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x123456789abcdef5, 5),
            0x6
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x123456789abcdef5, 6),
            0x7
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x123456789abcdef5, 7),
            0x8
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x123456789abcdef5, 8),
            0x9
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x123456789abcdef5, 9),
            0xa
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x123456789abcdef5, 10),
            0xb
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x123456789abcdef5, 11),
            0xc
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x123456789abcdef5, 12),
            0xd
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x123456789abcdef5, 13),
            0xe
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x123456789abcdef5, 14),
            0xf
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x123456789abcdef5, 15),
            0x5
        );

        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0xf000000000000000, 0),
            0xf
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x0f00000000000000, 1),
            0xf
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x00f0000000000000, 2),
            0xf
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x000f000000000000, 3),
            0xf
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x0000f00000000000, 4),
            0xf
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x00000f0000000000, 5),
            0xf
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x000000f000000000, 6),
            0xf
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x0000000f00000000, 7),
            0xf
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x00000000f0000000, 8),
            0xf
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x000000000f000000, 9),
            0xf
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x0000000000f00000, 10),
            0xf
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x00000000000f0000, 11),
            0xf
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x000000000000f000, 12),
            0xf
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x0000000000000f00, 13),
            0xf
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x00000000000000f0, 14),
            0xf
        );
        assert_eq!(
            HashTrie::<bool, bool>::child_idx(0x000000000000000f, 15),
            0xf
        );
    }

    fn dump_hash_trie<K: Hash + std::fmt::Debug, V: std::fmt::Debug>(m: &HashTrie<K, V>) {
        fn dump_hash_trie_inner<K: Hash + std::fmt::Debug, V: std::fmt::Debug>(
            random_state: &RandomState,
            node: &HashTrieNode<K, V>,
            depth: usize,
            h: u64,
        ) {
            match node {
                HashTrieNode::Internal(children) => {
                    println!("{}[internal {:x}]", " ".repeat(depth * 2), h);
                    for (i, maybe_child) in children.iter().enumerate() {
                        match maybe_child {
                            Some(child) => {
                                dump_hash_trie_inner(
                                    random_state,
                                    child,
                                    depth + 1,
                                    h << 4 | (i as u64),
                                );
                            }
                            None => {
                                println!("{}[vacant]", " ".repeat(depth * 2 + 2));
                            }
                        }
                    }
                }
                HashTrieNode::Leaf(m) => {
                    println!("{}[leaf {:x}]", " ".repeat(depth * 2), h);
                    for (k, v) in m {
                        println!(
                            "{}{:?} ({:x}): {:?}",
                            " ".repeat(depth * 2 + 2),
                            k,
                            random_state.hash_one(&k),
                            v
                        );
                    }
                }
            }
        }
        dump_hash_trie_inner(&m.random_state, &m.root, 0, 0);
    }
}
