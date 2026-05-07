use std::collections::BinaryHeap;
use std::sync::RwLock;

use crate::OrdEqByFirst;

pub struct WaitableOrd<T> {
    inner: RwLock<WaitableOrdInner<T>>,
}

struct WaitableOrdInner<T> {
    current: T,
    waiters: BinaryHeap<OrdEqByFirst<T, tokio::sync::oneshot::Sender<()>>>,
}

impl<T> WaitableOrd<T>
where
    T: Ord + Copy,
{
    pub fn new(initial: T) -> Self {
        Self {
            inner: RwLock::new(WaitableOrdInner {
                current: initial,
                waiters: BinaryHeap::new(),
            }),
        }
    }

    pub fn set(&self, v: T) {
        let mut inner = self.inner.write().unwrap();

        if inner.current >= v {
            return;
        }

        inner.current = v;

        while let Some(OrdEqByFirst(wait_v, _)) = inner.waiters.peek() {
            if *wait_v > inner.current {
                break;
            }
            let OrdEqByFirst(_, sender) = inner.waiters.pop().unwrap();
            let _ = sender.send(());
        }
    }

    pub fn get(&self) -> T {
        self.inner.read().unwrap().current
    }

    pub async fn wait(&self, until: T) {
        {
            let inner = self.inner.read().unwrap();
            if inner.current >= until {
                return;
            }
        }
        let receiver = {
            let mut inner = self.inner.write().unwrap();
            if inner.current >= until {
                return;
            }
            let (sender, receiver) = tokio::sync::oneshot::channel();
            inner.waiters.push(OrdEqByFirst(until, sender));
            receiver
        };
        let _ = receiver.await;
    }
}
