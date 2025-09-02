use std::collections::HashMap;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;

/// Background is a set of owned tasks which are aborted on drop.
pub(crate) struct Background {
    tasks: std::sync::Arc<std::sync::Mutex<(u64, HashMap<u64, OwnedJoinHandle<()>>)>>,
}

impl Background {
    pub(crate) fn new() -> Self {
        Self {
            tasks: std::sync::Arc::new(std::sync::Mutex::new((0, HashMap::new()))),
        }
    }

    pub(crate) fn spawn<F: Future<Output = ()> + Send + 'static>(&self, f: F) {
        let mut guard = self.tasks.lock().unwrap();
        let id = guard.0;
        let tasks_arc = self.tasks.clone();
        let handle = spawn_owned(async move {
            f.await;
            let mut guard = tasks_arc.lock().unwrap();
            guard.1.remove(&id);
        });
        guard.0 += 1;
        guard.1.insert(id, handle);
    }
}

pub(crate) struct WithBackground<T> {
    inner: Arc<T>,
    bg: Background,
}

impl<T> WithBackground<T>
where
    T: Send + Sync + 'static,
{
    pub(crate) fn new(t: Arc<T>) -> Self {
        Self {
            inner: t,
            bg: Background::new(),
        }
    }

    pub(crate) fn spawn<F, Fut>(&self, f: F)
    where
        F: FnOnce(Arc<T>) -> Fut + Sync + Send + 'static,
        Fut: Future<Output = ()> + Send,
    {
        self.bg.spawn({
            let inner = self.inner.clone();
            async move {
                f(inner).await;
            }
        });
    }
}

impl<T> Deref for WithBackground<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

/// spawn_owned is just like tokio::spawn, but if the returned handle is dropped the task is
/// aborted.
///
/// Panics in the task may also panic the owner.
pub(crate) fn spawn_owned<F, T>(f: F) -> OwnedJoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    OwnedJoinHandle(Box::pin(tokio::spawn(f)))
}

/// Wraps a JoinHandle, calling abort() on it when dropped.
///
/// tokio::spawn naturally just allows the task to keep running in the background indefinitely, but
/// this is used when a function is supposed to 'own' the tasks it spawns.
pub(crate) struct OwnedJoinHandle<T>(Pin<Box<tokio::task::JoinHandle<T>>>);

impl<T> Future for OwnedJoinHandle<T> {
    type Output = T;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // unwrap() here is safe because JoinErrors are only produced for two reasons:
        // 1. The task is aborted. This can't happen because we only abort on drop, which means we
        //    can't be polling.
        // 2. The task itself panics. We're allowed to panic the calling task by the API contract.
        Pin::as_mut(&mut self.0)
            .poll(cx)
            .map(|result| result.unwrap())
    }
}

impl<T> Drop for OwnedJoinHandle<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
