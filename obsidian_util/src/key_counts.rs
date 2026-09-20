use std::collections::HashMap;
use std::hash::Hash;

pub struct KeyCounts<K> {
    counts: HashMap<K, usize>,
}

impl<K> KeyCounts<K>
where
    K: Eq + Hash,
{
    pub fn new() -> Self {
        Self {
            counts: HashMap::new(),
        }
    }

    pub fn incr(&mut self, key: K) {
        *self.counts.entry(key).or_default() += 1;
    }

    pub fn decr(&mut self, key: &K) {
        let remove = if let Some(count) = self.counts.get_mut(key) {
            *count -= 1;
            *count == 0
        } else {
            false
        };
        if remove {
            self.counts.remove(key);
        }
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.counts.contains_key(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, usize)> {
        self.counts.iter().map(|(k, count)| (k, *count))
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.counts.keys()
    }
}
