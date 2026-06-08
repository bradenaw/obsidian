use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use obsidian_common::RunId;

/// For the purposes of garbage collecting LSM runs, we need to know which runs are live. Runs that
/// appear in the LSM's index are live, but we have a harder time distinguishing whether runs not
/// in the index are missing because they've been removed or if they're missing because they are
/// about to be added.
pub(crate) struct InFlightRuns {
    inner: Arc<Mutex<InFlightRunsInner>>,
}
struct InFlightRunsInner {
    run_ids: BTreeSet<RunId>,
}

impl InFlightRuns {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(InFlightRunsInner {
                run_ids: BTreeSet::new(),
            })),
        }
    }

    pub fn insert(&self, run_id: RunId) -> InFlightRun {
        let mut inner = self.inner.lock().unwrap();
        inner.run_ids.insert(run_id);
        InFlightRun {
            parent: Arc::downgrade(&self.inner),
            run_id,
        }
    }

    pub fn runs(&self) -> BTreeSet<RunId> {
        let inner = self.inner.lock().unwrap();
        inner.run_ids.clone()
    }
}

pub(crate) struct InFlightRun {
    parent: Weak<Mutex<InFlightRunsInner>>,
    run_id: RunId,
}

impl Drop for InFlightRun {
    fn drop(&mut self) {
        if let Some(parent_mutex) = self.parent.upgrade() {
            let mut parent = parent_mutex.lock().unwrap();
            parent.run_ids.remove(&self.run_id);
        }
    }
}
