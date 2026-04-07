use std::future::Future;
use std::sync::Arc;
use std::sync::RwLock;

pub(crate) struct Watchable<T> {
    value: RwLock<T>,
    changed: Arc<tokio::sync::Notify>,
}

impl<T> Watchable<T>
where
    T: Clone,
{
    pub fn new(initial: T) -> Self {
        Self {
            value: RwLock::new(initial),
            changed: Arc::new(tokio::sync::Notify::new()),
        }
    }

    pub fn set(&self, value: T) {
        let mut value_locked = self.value.write().unwrap();
        *value_locked = value;
        self.changed.notify_waiters();
    }

    pub fn get(&self) -> (T, impl Future<Output = ()>) {
        let notified = Arc::clone(&self.changed).notified_owned();
        (self.value.read().unwrap().clone(), async move {
            notified.await;
        })
    }
}
