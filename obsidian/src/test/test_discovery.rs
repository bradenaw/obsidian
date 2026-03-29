use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use im::OrdSet;

use crate::runtime::Discovery;
use crate::runtime::Registration;
use crate::util::Watchable;
use crate::NodeId;

pub(crate) struct TestDiscovery(Arc<TestDiscoveryInner>);

struct TestDiscoveryInner {
    node_ids: Watchable<OrdSet<NodeId>>,
    refcounts: Mutex<HashMap<NodeId, usize>>,
}

impl TestDiscovery {
    pub fn new() -> Self {
        Self(Arc::new(TestDiscoveryInner {
            node_ids: Watchable::new(OrdSet::new()),
            refcounts: Mutex::new(HashMap::new()),
        }))
    }
}

impl Discovery for TestDiscovery {
    fn node_ids(&self) -> (OrdSet<NodeId>, Box<dyn Future<Output = ()> + Unpin + Send>) {
        let (node_ids, changed) = self.0.node_ids.get();
        (node_ids, Box::new(Box::pin(changed)))
    }

    fn register(&self, node_id: NodeId) -> Box<dyn Registration> {
        let mut refcounts = self.0.refcounts.lock().unwrap();
        *refcounts.entry(node_id).or_insert(0) += 1;
        let mut node_ids = self.0.node_ids.get().0;
        if node_ids.insert(node_id).is_some() {
            self.0.node_ids.set(node_ids);
        }

        Box::new(TestDiscoveryRegistration {
            node_id,
            parent: Arc::downgrade(&self.0),
        })
    }
}

struct TestDiscoveryRegistration {
    node_id: NodeId,
    parent: Weak<TestDiscoveryInner>,
}

impl Registration for TestDiscoveryRegistration {}

impl Drop for TestDiscoveryRegistration {
    fn drop(&mut self) {
        if let Some(parent) = Weak::upgrade(&self.parent) {
            let mut refcounts = parent.refcounts.lock().unwrap();
            let refcount = refcounts.get_mut(&self.node_id).unwrap();
            *refcount -= 1;
            if *refcount == 0 {
                refcounts.remove(&self.node_id);
                let mut node_ids = parent.node_ids.get().0;
                node_ids.remove(&self.node_id);
                parent.node_ids.set(node_ids);
            }
        }
    }
}
