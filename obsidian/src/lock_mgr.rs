use std::cmp::Reverse;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

pub(crate) struct LockMgr {
    buckets: Vec<tokio::sync::RwLock<()>>,
    random_state: RandomState,
}

impl LockMgr {
    pub fn new(n_buckets: usize) -> Self {
        Self {
            buckets: (0..n_buckets)
                .map(|_| tokio::sync::RwLock::new(()))
                .collect(),
            random_state: RandomState::new(),
        }
    }

    pub async fn lock<'a, 'b, 'c>(
        &'a self,
        read: impl Iterator<Item = &'b [u8]>,
        write: impl Iterator<Item = &'c [u8]>,
    ) -> Guard<'a> {
        let (n_reads, _) = read.size_hint();
        let (n_writes, _) = write.size_hint();

        let mut hashes = Vec::with_capacity(n_reads + n_writes);
        for key in read {
            hashes.push((self.hash(key), Reverse(false)));
        }
        for key in write {
            hashes.push((self.hash(key), Reverse(true)));
        }
        hashes.sort();
        hashes.dedup_by_key(|(hash, _)| *hash);

        // Acquire locks in sorted order to avoid deadlock.
        let mut read_guards = Vec::with_capacity(n_reads);
        let mut write_guards = Vec::with_capacity(n_writes);
        for (hash, Reverse(need_write)) in hashes {
            match need_write {
                true => {
                    write_guards.push(self.buckets[hash].write().await);
                }
                false => {
                    read_guards.push(self.buckets[hash].read().await);
                }
            }
        }

        Guard {
            read_guards,
            write_guards,
        }
    }

    fn hash(&self, key: &[u8]) -> usize {
        (self.random_state.hash_one(key) % (self.buckets.len() as u64)) as usize
    }
}

pub(crate) struct Guard<'a> {
    read_guards: Vec<tokio::sync::RwLockReadGuard<'a, ()>>,
    write_guards: Vec<tokio::sync::RwLockWriteGuard<'a, ()>>,
}
