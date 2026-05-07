use std::collections::BTreeSet;
use std::collections::BinaryHeap;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use obsidian_util::spawn_owned;
use obsidian_util::OrdEqByFirst;
use obsidian_util::OwnedJoinHandle;

use crate::Timestamp;

const ADVANCE_TICK: Duration = Duration::from_millis(25);

pub struct Sequencer {
    inner: Arc<RwLock<SequencerInner>>,
    advance: OwnedJoinHandle<()>,
}

impl Sequencer {
    pub fn new() -> Self {
        let inner = Arc::new(RwLock::new(SequencerInner {
            last_ts: Timestamp::ZERO,
            safe_read_ts: Timestamp::ZERO,
            pending: BTreeSet::new(),
            waiters: BinaryHeap::new(),
        }));

        let advance = spawn_owned({
            let inner = Arc::clone(&inner);
            async move {
                loop {
                    {
                        let mut inner = inner.write().unwrap();
                        let ts = inner.start_write();
                        inner.finish_write(ts);
                    }
                    tokio::time::sleep(ADVANCE_TICK).await;
                }
            }
        });
        Self { inner, advance }
    }

    pub fn start_write(&self) -> WriteTsGuard<'_> {
        let mut inner = self.inner.write().unwrap();
        let ts = inner.start_write();
        WriteTsGuard { parent: self, ts }
    }

    fn finish_write(&self, ts: Timestamp) {
        let mut inner = self.inner.write().unwrap();
        inner.finish_write(ts)
    }

    pub fn safe_read_ts(&self) -> Timestamp {
        self.inner.read().unwrap().safe_read_ts
    }

    pub async fn wait_for_safe_read(&self, ts: Timestamp) -> anyhow::Result<()> {
        // Straight reject - it's going to be a while before this timestamp is safe to read and we
        // may as well let the client wait instead of us.
        if ts.saturating_duration_since(Timestamp::now()) > Duration::from_millis(100) {
            anyhow::bail!("timestamp too far in the future");
        }
        loop {
            if ts <= self.safe_read_ts() {
                return Ok(());
            }
            let maybe_receiver = {
                let mut inner = self.inner.write().unwrap();
                if let Some(highest_pending_ts) = inner.pending.last() {
                    if *highest_pending_ts >= ts {
                        let (sender, receiver) = tokio::sync::oneshot::channel();
                        inner.waiters.push(OrdEqByFirst(ts, sender));
                        // Important: don't hold the lock while waiting, otherwise nobody will be able
                        // to wake us up.
                        Some(receiver)
                    } else {
                        None
                    }
                } else {
                    if ts <= inner.safe_read_ts {
                        return Ok(());
                    }
                    // pending.is_empty() which implies last_ts=safe_read_ts already.
                    if ts.saturating_duration_since(Timestamp::now()) < Duration::from_millis(10) {
                        inner.safe_read_ts = ts;
                        inner.last_ts = ts;
                        return Ok(());
                    }
                    None
                }
            };
            if let Some(receiver) = maybe_receiver {
                receiver.await.unwrap();
                return Ok(());
            }
            tokio::time::sleep(ts.saturating_duration_since(Timestamp::now())).await;
        }
    }
}

pub struct WriteTsGuard<'a> {
    parent: &'a Sequencer,
    ts: Timestamp,
}

impl<'a> Deref for WriteTsGuard<'a> {
    type Target = Timestamp;
    fn deref(&self) -> &Timestamp {
        &self.ts
    }
}

impl<'a> Drop for WriteTsGuard<'a> {
    fn drop(&mut self) {
        self.parent.finish_write(self.ts);
    }
}

struct SequencerInner {
    // Last assigned timestamp. All new timestamps must be higher than this.
    last_ts: Timestamp,
    // All writes that could have this timestamp or lower assigned are already completed and
    // visible.
    //
    // Invariant: !pending.is_empty() || safe_read_ts==last_ts.
    safe_read_ts: Timestamp,
    pending: BTreeSet<Timestamp>,
    waiters: BinaryHeap<OrdEqByFirst<Timestamp, tokio::sync::oneshot::Sender<()>>>,
}

impl SequencerInner {
    fn start_write(&mut self) -> Timestamp {
        let ts = Timestamp::now_after(self.last_ts);
        self.last_ts = ts;
        if self.pending.is_empty() {
            self.safe_read_ts = ts.minus_one();
            self.wake_waiters();
        }
        self.pending.insert(ts);
        ts
    }

    fn finish_write(&mut self, ts: Timestamp) {
        assert!(self.pending.remove(&ts));
        if let Some(lowest_pending_ts) = self.pending.first() {
            self.safe_read_ts = lowest_pending_ts.minus_one();
        } else {
            self.safe_read_ts = self.last_ts;
        }
        self.wake_waiters();
    }
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
