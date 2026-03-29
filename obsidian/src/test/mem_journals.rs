use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::runtime::Journal;
use crate::runtime::Journals;
use crate::test::MemJournal;
use crate::ShardId;

#[derive(Clone)]
pub(crate) struct MemJournals<E> {
    m: Arc<Mutex<HashMap<ShardId, Arc<dyn Journal<E>>>>>,
}

impl<E> MemJournals<E> {
    pub fn new() -> Self {
        Self {
            m: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl<E> Journals<E> for MemJournals<E>
where
    E: Clone + Send + Sync + 'static,
{
    async fn journal(&self, shard_id: ShardId) -> Arc<dyn Journal<E>> {
        let mut m = self.m.lock().unwrap();

        Arc::clone(
            m.entry(shard_id)
                .or_insert_with(|| Arc::new(MemJournal::new())),
        )
    }
}
