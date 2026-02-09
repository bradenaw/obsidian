use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::anyhow;
use async_trait::async_trait;

use crate::meta::MetaKey;
use crate::meta::MetaReader;
use crate::meta::MetaSubscriber;
use crate::meta::MetaSynced;
use crate::meta::MetaSyncedSnapshot;
use crate::meta::SyncType;
use crate::runtime;
use crate::runtime::Nodes;
use crate::util::Retry;
use crate::util::WithBackground;
use crate::NodeId;
use crate::ShardId;

pub(crate) struct Shards(WithBackground<ShardsInner>);

struct ShardsInner {
    nodes: Arc<dyn Nodes>,
    routing: RwLock<HashMap<ShardId, NodeId>>,
}

impl Shards {
    pub fn new(meta_synced: Arc<MetaSynced>, nodes: Arc<dyn Nodes>) -> Self{
        let shards = Shards(WithBackground::new(Arc::new(ShardsInner{
            nodes,
            routing: RwLock::new(HashMap::new()),
        })));

        meta_synced.subscribe(&shards.0);

        shards
    }
}

impl runtime::Shards for Shards {
    fn shard(&self, shard_id: ShardId) -> anyhow::Result<Arc<dyn runtime::Shard>> {
        let node_id = {
            let routing = self.0.routing.read().unwrap();
            routing.get(&shard_id).ok_or_else(|| anyhow!(""))?.clone()
        };

        self.0.nodes.node(&node_id)?.shard(shard_id)
    }

    fn shards(&self) -> Vec<Box<dyn runtime::Shard>> {
        todo!()
    }
}

impl ShardsInner {
    async fn try_sync_meta(
        &self,
        sync_type: &SyncType,
        snapshot: &MetaSyncedSnapshot,
    ) -> anyhow::Result<()> {
        match sync_type {
            SyncType::Initial => {
                let shard_ids = snapshot.shard_ids().await?;
                for shard_id in shard_ids {
                    self.sync_meta_shard_metadata(snapshot, shard_id).await?;
                }
            }
            SyncType::Tx(keys) => {
                for key in keys {
                    if let MetaKey::Shard(shard_id) = key {
                        self.sync_meta_shard_metadata(snapshot, *shard_id).await?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn sync_meta_shard_metadata(
        &self,
        snapshot: &MetaSyncedSnapshot,
        shard_id: ShardId,
    ) -> anyhow::Result<()> {
        let shard_metadata = snapshot.shard_metadata(shard_id).await?;

        let mut routing = self.routing.write().unwrap();
        if let Some(node_id) = shard_metadata.assigned_node_id {
            routing.insert(shard_id, node_id);
        } else {
            routing.remove(&shard_id);
        }

        Ok(())
    }
}

#[async_trait]
impl MetaSubscriber for ShardsInner {
    async fn sync_meta(&self, sync_type: SyncType, snapshot: MetaSyncedSnapshot) {
        Retry::new()
            .indefinitely(&async || -> anyhow::Result<()> {
                self.try_sync_meta(&sync_type, &snapshot).await
            })
            .await;
    }
}
