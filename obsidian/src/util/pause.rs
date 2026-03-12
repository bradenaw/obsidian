use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::OwnedRwLockReadGuard;
use tokio::sync::OwnedRwLockWriteGuard;
use tokio::sync::RwLock as AsyncRwLock;

/// Pause allows pausing (usually) background tasks, that e.g. do things in a loop.
pub(crate) struct Pause {
    // (Ab)uses `tokio::sync::RwLock` to get the behavior we want. Non-blocked subscribers hold
    // onto a read guard that they periodically release and re-acquire. We can pause everybody (and
    // wait for them to be paused) just by acquiring the write lock.
    wait_lock: Arc<AsyncRwLock<()>>,
    // If paused, holds onto a write guard for the above lock.
    paused: AsyncMutex<Option<OwnedRwLockWriteGuard<()>>>,
}

impl Pause {
    /// A new Pause starts off unpaused.
    pub fn new() -> Pause {
        Pause {
            wait_lock: Arc::new(AsyncRwLock::new(())),
            paused: AsyncMutex::new(None),
        }
    }

    /// Pauses all of the associated `PauseWaiter`s and waits for them to reach their next call to
    /// `PauseWaiter::maybe()`, where they will block until unpause.
    ///
    /// Future calls to `subscribe()` will also block.
    pub async fn pause(&self) {
        let mut inner = self.paused.lock().await;
        if inner.is_some() {
            return;
        }
        let guard = Arc::clone(&self.wait_lock).write_owned().await;
        *inner = Some(guard);
    }

    /// Unblocks all waiters in `susbcribe()` or `PauseWaiter::maybe()`.
    pub fn unpause(&self) {
        // If we can't acquire it then there's a concurrent pause, and then it should be valid to
        // consider us to be first and them to be second, which ends in being paused.
        if let Ok(mut inner) = self.paused.try_lock() {
            *inner = None;
        }
    }

    pub async fn subscribe(&self) -> PauseWaiter {
        PauseWaiter {
            wait_lock: Arc::clone(&self.wait_lock),
            guard: Some(Arc::clone(&self.wait_lock).read_owned().await),
        }
    }
}

pub(crate) struct PauseWaiter {
    wait_lock: Arc<AsyncRwLock<()>>,
    guard: Option<OwnedRwLockReadGuard<()>>,
}

impl PauseWaiter {
    /// If the associated `Pause` is paused, blocks until unpaused.
    pub async fn maybe(&mut self) {
        self.guard.take();
        self.guard = Some(Arc::clone(&self.wait_lock).read_owned().await);
    }
}
