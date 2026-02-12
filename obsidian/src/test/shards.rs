use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use anyhow::anyhow;

use crate::runtime::Shard;
use crate::runtime::Shards;
use crate::runtime::Storage;
use crate::runtime::Wal;
use crate::runtime::Wals;
use crate::test::meta_proxy::MetaProxy;
use crate::test::MemWals;
use crate::ShardId;

pub(super) struct TestShards {
    storage: Arc<dyn Storage>,
    meta_proxy: Arc<MetaProxy>,
    wals: MemWals,

    m: Mutex<HashMap<ShardId, Arc<crate::shard::Shard>>>,
}

impl TestShards {
    pub fn new(storage: Arc<dyn Storage>, meta_proxy: Arc<MetaProxy>) -> Self {
        Self {
            storage,
            meta_proxy,
            wals: MemWals::new(),
            m: Mutex::new(HashMap::new()),
        }
    }

    pub async fn create_shard(self: &Arc<Self>) -> anyhow::Result<ShardId> {
        let mut m = self.m.lock().unwrap();
        let shard_id = ShardId((m.len() + 1) as u32);
        m.insert(
            shard_id,
            Arc::new(
                crate::shard::Shard::new(
                    shard_id,
                    self.storage.clone(),
                    Arc::new(self.meta_proxy.clone()),
                    Arc::new(Arc::downgrade(&self)),
                    Arc::new(self.wals.clone()) as Arc<dyn Wals<Arc<dyn Wal>>>,
                    256,   // l0_max_size
                    65536, // run_size_target
                    4096,  // block_size_target
                )
                .await?,
            ),
        );
        Ok(shard_id)
    }
}

impl Shards for Arc<TestShards> {
    fn shard(&self, shard_id: ShardId) -> anyhow::Result<Arc<dyn Shard>> {
        let m = self.m.lock().unwrap();
        let shard_arc = m
            .get(&shard_id)
            .ok_or_else(|| anyhow::anyhow!("{:?} does not exist", shard_id))?;

        Ok(Arc::clone(shard_arc) as Arc<dyn Shard>)
    }

    fn shards(&self) -> Vec<Box<dyn Shard>> {
        let m = self.m.lock().unwrap();
        m.values()
            .map(|shard| -> Box<dyn Shard> { Box::new(shard.clone()) })
            .collect()
    }
}

impl Shards for Weak<TestShards> {
    fn shard(&self, shard_id: ShardId) -> anyhow::Result<Arc<dyn Shard>> {
        let shards = Weak::upgrade(self).ok_or_else(|| anyhow!(""))?;
        let m = shards.m.lock().unwrap();
        let shard_arc = m
            .get(&shard_id)
            .ok_or_else(|| anyhow::anyhow!("{:?} does not exist", shard_id))?;

        Ok(Arc::clone(shard_arc) as Arc<dyn Shard>)
    }

    fn shards(&self) -> Vec<Box<dyn Shard>> {
        let shards = match Weak::upgrade(self) {
            Some(shards) => shards,
            None => return vec![],
        };
        let m = shards.m.lock().unwrap();
        m.values()
            .map(|shard| -> Box<dyn Shard> { Box::new(shard.clone()) })
            .collect()
    }
}
