use std::collections::BTreeSet;
use std::collections::BinaryHeap;
use std::sync::RwLock;
use std::time::Duration;
use std::time::SystemTime;

use crate::OrdEqByFirst;

pub struct Sequencer {
    inner: RwLock<SequencerInner>,
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("now before UNIX_EPOCH?")
        .as_nanos() as u64
}

impl Sequencer {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(SequencerInner {
                last_ts: 0,
                safe_read_ts: 0,
                pending: BTreeSet::new(),
                waiters: BinaryHeap::new(),
            }),
        }
    }

    pub fn start_write(&self) -> u64 {
        let now = now_nanos();

        let mut inner = self.inner.write().unwrap();
        let ts = std::cmp::max(now, inner.last_ts + 1);
        inner.last_ts = ts;
        if inner.pending.is_empty() {
            inner.safe_read_ts = ts - 1;
            inner.wake_waiters();
        }
        inner.pending.insert(ts);
        ts
    }

    pub fn finish_write(&self, ts: u64) {
        let mut inner = self.inner.write().unwrap();
        assert!(inner.pending.remove(&ts));
        if let Some(lowest_pending_ts) = inner.pending.first() {
            inner.safe_read_ts = lowest_pending_ts - 1;
        } else {
            inner.safe_read_ts = inner.last_ts;
        }
        inner.wake_waiters();
    }

    pub fn safe_read_ts(&self) -> u64 {
        self.inner.read().unwrap().safe_read_ts
    }

    pub async fn wait_for_safe_read(&self, ts: u64) -> anyhow::Result<()> {
        // Straight reject - it's going to be a while before this timestamp is safe to read and we
        // may as well let the client wait instead of us.
        if ts.saturating_sub(now_nanos()) > 100_000_000 {
            anyhow::bail!("timestamp in the future");
        }
        loop {
            if ts <= self.safe_read_ts() {
                return Ok(());
            }
            let mut inner = self.inner.write().unwrap();
            if let Some(highest_pending_ts) = inner.pending.last() {
                if *highest_pending_ts >= ts {
                    let (sender, receiver) = tokio::sync::oneshot::channel();
                    inner.waiters.push(OrdEqByFirst(ts, sender));
                    // Important: don't hold the lock while waiting, otherwise nobody will be able
                    // to wake us up.
                    drop(inner);
                    receiver.await.unwrap();
                    return Ok(());
                }
            } else {
                if ts <= inner.safe_read_ts {
                    return Ok(());
                }
                // pending.is_empty() which implies last_ts=safe_read_ts already.
                let now = now_nanos();
                if ts.saturating_sub(now) < 10_000_000 {
                    inner.safe_read_ts = ts;
                    inner.last_ts = ts;
                    return Ok(());
                }
            }
            drop(inner);
            tokio::time::sleep(Duration::from_nanos(now_nanos() - ts)).await;
        }
    }
}

struct SequencerInner {
    // Last assigned timestamp. All new timestamps must be higher than this.
    last_ts: u64,
    // All writes that could have this timestamp or lower assigned are already completed and
    // visible.
    //
    // Invariant: !pending.is_empty() || safe_read_ts==last_ts.
    safe_read_ts: u64,
    pending: BTreeSet<u64>,
    waiters: BinaryHeap<OrdEqByFirst<u64, tokio::sync::oneshot::Sender<()>>>,
}

impl SequencerInner {
    fn wake_waiters(&mut self) {
        while let Some(OrdEqByFirst(wait_ts, _)) = self.waiters.peek() {
            if *wait_ts >= self.safe_read_ts {
                break;
            }

            let OrdEqByFirst(_, sender) = self.waiters.pop().unwrap();
            let _ = sender.send(());
        }
    }
}
