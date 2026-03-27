use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use crate::Timestamp;

pub(crate) struct AtomicTimestamp {
    inner: AtomicU64,
}

impl AtomicTimestamp {
    pub fn new(ts: Timestamp) -> Self {
        Self {
            inner: AtomicU64::new(ts.as_micros()),
        }
    }

    pub fn load(&self) -> Timestamp {
        Timestamp::from_micros(self.inner.load(Ordering::SeqCst))
    }

    pub fn store(&self, ts: Timestamp) {
        self.inner.store(ts.as_micros(), Ordering::SeqCst);
    }
}
