use std::collections::BTreeSet;
use std::collections::BinaryHeap;
use std::future::Future;
use std::sync::RwLock;
use std::time::SystemTime;

use crate::OrdEqByFirst;

pub struct Sequencer {
    inner: RwLock<SequencerInner>,
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

    fn start_write(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("now before UNIX_EPOCH?")
            .as_nanos() as u64;

        let mut inner = self.inner.write().unwrap();
        let ts = std::cmp::max(now, inner.last_ts + 1);
        inner.last_ts = ts;
        inner.pending.insert(ts);
        ts
    }

    fn finish_write(&self, ts: u64) {
        let mut inner = self.inner.write().unwrap();
        assert!(inner.pending.remove(&ts));
        if let Some(lowest_pending_ts) = inner.pending.first() {
            inner.safe_read_ts = lowest_pending_ts - 1;
            while let Some(OrdEqByFirst(wait_ts, _)) = inner.waiters.peek() {
                if *wait_ts >= inner.safe_read_ts {
                    break;
                }

                let OrdEqByFirst(_, sender) = inner.waiters.pop().unwrap();
                let _ = sender.send(());
            }
        }
    }

    pub async fn write<F, T, Fu>(&self, f: F) -> T
    where
        F: FnOnce(u64) -> Fu,
        Fu: Future<Output = T>,
    {
        let ts = self.start_write();
        let out = f(ts).await;
        self.finish_write(ts);
        out
    }

    pub fn safe_read_ts(&self) -> u64 {
        self.inner.read().unwrap().safe_read_ts
    }

    pub async fn wait_for_safe_read(&self, ts: u64) {
        if ts <= self.safe_read_ts() {
            return;
        }
        let receiver = {
            let mut inner = self.inner.write().unwrap();
            let (sender, receiver) = tokio::sync::oneshot::channel();
            inner.waiters.push(OrdEqByFirst(ts, sender));
            receiver
        };
        receiver.await.unwrap()
    }
}

struct SequencerInner {
    last_ts: u64,
    safe_read_ts: u64,
    pending: BTreeSet<u64>,
    waiters: BinaryHeap<crate::OrdEqByFirst<u64, tokio::sync::oneshot::Sender<()>>>,
}
