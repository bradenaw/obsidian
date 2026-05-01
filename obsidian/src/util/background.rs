use std::collections::HashMap;
use std::future::Future;
use std::mem;
use std::ops::Deref;
use std::ops::DerefMut;
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

    pub(crate) async fn stop(self) {
        let tasks_map = {
            let mut guard = self.tasks.lock().unwrap();
            mem::replace(guard.deref_mut(), (0, HashMap::new()))
        };
        for (_, join_handle) in tasks_map.1.into_iter() {
            join_handle.cancel().await;
        }
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

pub(crate) struct OwnedWithBackground<T> {
    inner: Arc<T>,
    bg: Background,
}

impl<T> OwnedWithBackground<T>
where
    T: Send + Sync + 'static,
{
    pub fn new(t: T) -> Self {
        Self {
            inner: Arc::new(t),
            bg: Background::new(),
        }
    }

    pub(crate) fn spawn<F>(&self, f: F)
    where
        F: AsyncFnOnce(&T) + Send + 'static,
        for<'a> <F as AsyncFnOnce<(&'a T,)>>::CallOnceFuture: Send,
    {
        self.bg.spawn({
            let inner = Arc::clone(&self.inner);
            async move {
                let inner2 = Arc::clone(&inner);
                f(inner2.deref()).await;
            }
        });
    }

    pub async fn take(self) -> T {
        self.bg.stop().await;
        // This unwrap is safe because the only way we end up with clones of self.inner is in
        // spawn, and bg.stop() already made sure all of those are gone.
        Arc::into_inner(self.inner).unwrap()
    }
}

impl<T> Deref for OwnedWithBackground<T> {
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
    OwnedJoinHandle(Some(Box::pin(tokio::spawn(f))))
}

/// Wraps a JoinHandle, calling abort() on it when dropped.
///
/// tokio::spawn naturally just allows the task to keep running in the background indefinitely, but
/// this is used when a function is supposed to 'own' the tasks it spawns.
pub(crate) struct OwnedJoinHandle<T>(Option<Pin<Box<tokio::task::JoinHandle<T>>>>);

impl<T> OwnedJoinHandle<T> {
    /// Aborts the task and blocks until it stops running.
    pub async fn cancel(mut self) {
        let inner = self.0.take().unwrap();
        inner.abort();
        let _ = inner.await;
    }
}

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
        Pin::as_mut(&mut self.0.as_mut().unwrap())
            .poll(cx)
            .map(|result| result.unwrap())
    }
}

impl<T> Drop for OwnedJoinHandle<T> {
    fn drop(&mut self) {
        if let Some(inner) = self.0.take() {
            inner.abort();
        }
    }
}
