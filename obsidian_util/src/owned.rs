use std::ops::Deref;
use std::sync::Arc;
use std::sync::Weak;

use anyhow::anyhow;
use tokio::select;
use tokio::sync::SetOnce;

/// Dropping an [`Owned<T>`] will interrupt the calls in progress on the associated
/// [`WeakView<T>`]s, causing them to return an error. This does not drop `T` synchronously, it just
/// bounds how long the callers can keep `T` alive via the `WeakView<T>`.
///
/// This exists because of the prevalence of `Arc<dyn U>`. We want to be able to hand these out
/// without worrying that the caller keeps the underlying thing alive indefinitely, for example by
/// calling a function that blocks.
///
/// The intended use of this is to `impl U for WeakView<T>`, and then make `Arc<dyn U>` via
/// [`Owned::weak()`].
pub struct Owned<T> {
    inner: Arc<T>,
    closed: Arc<SetOnce<()>>,
    weak: Arc<WeakView<T>>,
}

impl<T> Owned<T> {
    pub fn new(inner: T) -> Self {
        let arc = Arc::new(inner);
        let closed = Arc::new(SetOnce::new());
        let weak = WeakView {
            inner: Arc::downgrade(&arc),
            closed: Arc::clone(&closed),
        };
        Self {
            inner: arc,
            closed,
            weak: Arc::new(weak),
        }
    }

    pub fn weak(this: &Self) -> Arc<WeakView<T>> {
        Arc::clone(&this.weak)
    }
}

impl<T> Deref for Owned<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl<T> Drop for Owned<T> {
    fn drop(&mut self) {
        _ = self.closed.set(());
    }
}

/// A weak reference to an [`Owned<T>`].
pub struct WeakView<T> {
    inner: Weak<T>,
    closed: Arc<SetOnce<()>>,
}

impl<T> WeakView<T> {
    /// `or_closed` calls `f` with a reference to the inner `T`.
    ///
    /// If the associated [`Owned<T>`] is dropped, `f`'s future is also dropped and `or_closed`
    /// returns an error.
    pub async fn or_closed<F, U, E>(&self, f: F) -> Result<U, E>
    where
        F: AsyncFnOnce(&T) -> Result<U, E>,
        E: From<anyhow::Error>,
    {
        let inner = self.inner.upgrade().ok_or_else(|| anyhow!("closed"))?;
        select! {
            biased;

            _ = self.closed.wait() => {
                Err(anyhow!("closed").into())
            }
            out = f(inner.deref()) => {
                out
            },
        }
    }
}
