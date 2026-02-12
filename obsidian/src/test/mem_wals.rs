use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::runtime::Wal;
use crate::runtime::Wals;
use crate::test::MemWal;
use crate::TabletId;

#[derive(Clone)]
pub(crate) struct MemWals {
    m: Arc<Mutex<HashMap<TabletId, Arc<dyn Wal>>>>,
}

impl MemWals {
    pub fn new() -> Self {
        Self {
            m: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl Wals<Arc<dyn Wal>> for MemWals {
    async fn wal(&self, tablet_id: TabletId) -> anyhow::Result<Arc<dyn Wal>> {
        let mut m = self.m.lock().unwrap();

        Ok(Arc::clone(
            m.entry(tablet_id)
                .or_insert_with(|| Arc::new(MemWal::new())),
        ))
    }
}
