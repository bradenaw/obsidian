use std::collections::BinaryHeap;
use std::sync::RwLock;

use crate::types::Timestamp;
use crate::util::OrdEqByFirst;

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
