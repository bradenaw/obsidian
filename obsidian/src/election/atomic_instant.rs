use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::time::Instant;

pub(super) struct AtomicInstant {
    epoch: Instant,
    elapsed: AtomicI64,
}

impl AtomicInstant {
    pub fn new(value: Instant) -> Self {
        Self {
            epoch: value,
            elapsed: AtomicI64::new(0),
        }
    }

    pub fn load(&self) -> Instant {
        let x = self.elapsed.load(Ordering::SeqCst);
        if x >= 0 {
            self.epoch
                .checked_add(Duration::from_nanos(x as u64))
                .unwrap()
        } else {
            self.epoch
                .checked_sub(Duration::from_nanos(-x as u64))
                .unwrap()
        }
    }

    pub fn store(&self, x: Instant) {
        if let Some(elapsed) = x.checked_duration_since(self.epoch) {
            self.elapsed
                .store(elapsed.as_nanos() as i64, Ordering::SeqCst);
        } else {
            let elapsed = self.epoch.duration_since(x);
            self.elapsed
                .store(elapsed.as_nanos() as i64, Ordering::SeqCst);
        }
    }
}
