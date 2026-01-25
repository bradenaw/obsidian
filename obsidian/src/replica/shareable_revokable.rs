use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use anyhow::anyhow;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub(crate) struct ShareableRevokable<T> {
    strong: Mutex<Arc<T>>,
    inner: Mutex<ShareableRevokableInner<T>>,
}

struct ShareableRevokableInner<T> {
    weak: Weak<T>,
    handles: Vec<JoinHandle<()>>,
}

impl<T: Send + Sync + 'static> ShareableRevokable<T> {
    pub async fn revoke_and_modify(&self, f: impl FnOnce(&mut T)) {
        let mut strong = self.strong.lock().unwrap();

        let handles: Vec<JoinHandle<()>> = {
            let mut inner = self.inner.lock().unwrap();
            inner.weak = Weak::new();
            inner.handles.drain(..).collect()
        };

        for handle in &handles {
            handle.abort();
        }
        for handle in handles {
            let _ = handle.await;
        }

        // inner.weak = None guarantees no new entrants, the join handles guarantee all of the
        // tasks that were already around have dropped their Arc of the value, so we're guaranteed
        // to be the only remaining reference.

        let t = Arc::get_mut(&mut strong).unwrap();
        f(t);

        {
            let mut inner = self.inner.lock().unwrap();
            inner.weak = Arc::downgrade(&strong);
        }
    }

    pub async fn share<F, Fut, T2>(&self, f: F) -> anyhow::Result<T2>
    where
        F: FnOnce(&T) -> Fut + Send + 'static,
        Fut: Future<Output = T2> + Send + 'static,
        T2: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();

        {
            let mut inner = self.inner.lock().unwrap();
            let t = inner
                .weak
                .upgrade()
                .ok_or_else(|| anyhow!("transitioning"))?;
            let join_handle = tokio::spawn(async move {
                let out = f(&t).await;
                let _ = tx.send(out);
            });
            inner.handles.push(join_handle);
        };

        rx.await.map_err(|e| anyhow!("revoked"))
    }
}
