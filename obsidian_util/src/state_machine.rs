use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;

use anyhow::anyhow;
use tokio::select;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Notify;
use tokio::sync::RwLock as AsyncRwLock;

/// StateMachine allows state transitions to take priority over other operations.
///
/// There are several state machines, like election::Participant and DataTablet, that can have
/// long-running/blocking operations (which may be indefinite) that we need not to block
/// transitions behind.
///
/// To that end, access to the current state is done with `with_state`, which will early return
/// with an error if a transition is requested.
pub struct StateMachine<S> {
    state: AsyncRwLock<S>,
    transition_request: Notify,
    // (Ab)used to keep the reference count of transitions that are waiting to acquire the write
    // lock, handy because dropping them decreases the refcount which makes this cancel-safe.
    transitions_waiting: Arc<()>,
    transition_lock: AsyncMutex<()>,
}

impl<S> StateMachine<S> {
    pub fn new(state: S) -> Self {
        Self {
            state: AsyncRwLock::new(state),
            transition_request: Notify::new(),
            transitions_waiting: Arc::new(()),
            transition_lock: AsyncMutex::new(()),
        }
    }

    /// Runs f with the current state of the state machine. f is cancelled and with_state returns
    /// an error if a transition is requested concurrently.
    pub async fn with_state<F, T, E>(&self, f: F) -> Result<T, E>
    where
        F: AsyncFnOnce(&S) -> Result<T, E>,
        E: From<anyhow::Error> + Send + 'static,
    {
        let transition_requested = self.transition_request.notified();
        let guard = self.state.read().await;
        // We check the reference count here _after_ we've acquired the lock because it's possible
        // to get a missed wakeup with this interleaving:
        //
        // transition                        with_state
        // notify_waiters()
        //                                   notified()
        //                                   read()
        // write() (blocks)
        if Arc::strong_count(&self.transitions_waiting) > 1 {
            return Err(anyhow!("aborted operation: state transition requested").into());
        }

        select! {
            biased; // Important: causes the transition_requested to take precedence. This is
                    // important because otherwise we might cancel an `f` (causing its mutexes to
                    // be released, even if state is intermediate) but continue running another
                    // future that then acquires those mutexes.

            _ = transition_requested => {
                Err(anyhow!("aborted operation: state transition requested").into())
            },
            out = f(guard.deref()) => {
                out
            },
        }
    }

    /// Calls f with the current state of the state machine.
    pub async fn inspect<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&S) -> T,
    {
        let guard = self.state.read().await;
        f(guard.deref())
    }

    /// Interrupts ongoing calls to with_state and allows modifying the current state.
    pub async fn transition<F, T>(&self, f: F) -> T
    where
        F: AsyncFnOnce(&mut S) -> T,
    {
        self.maybe_transition(|_| true, f).await.unwrap()
    }

    /// If should_transition returns true for the current state, interrupts ongoing calls to
    /// with_state and allows modifying the current state.
    ///
    /// Returns None if should_transition returned false and no transition was attempted.
    pub async fn maybe_transition<FShould, FDo, T>(
        &self,
        should_transition: FShould,
        do_transition: FDo,
    ) -> Option<T>
    where
        FShould: FnOnce(&S) -> bool,
        FDo: AsyncFnOnce(&mut S) -> T,
    {
        let _guard = self.transition_lock.lock().await;
        if !self.inspect(should_transition).await {
            return None;
        }
        let _waiting_count = Arc::clone(&self.transitions_waiting);
        self.transition_request.notify_waiters();
        let mut guard = self.state.write().await;
        // Just for the sake of having this drop before guard, which'll allow some with_states that
        // might've been unnecessarily cancelled if we do it in the other order.
        drop(_waiting_count);

        Some(do_transition(guard.deref_mut()).await)
    }
}
