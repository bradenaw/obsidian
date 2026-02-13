use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::TryStreamExt;

use crate::meta::MetaKey;
use crate::meta::MetaReader;
use crate::meta::MetaSubscriber;
use crate::meta::MetaSynced;
use crate::meta::MetaSyncedSnapshot;
use crate::meta::NodeMetadata;
use crate::meta::SyncType;
use crate::runtime;
use crate::runtime::Meta;
use crate::runtime::Shard as _;
use crate::runtime::Shards;
use crate::runtime::Storage;
use crate::runtime::Wals;
use crate::shard::Shard;
use crate::supervisor::Supervisor;
use crate::util::Retry;
use crate::util::WithBackground;
use crate::Direction;
use crate::NodeId;
use crate::ShardId;

pub(crate) struct Node(WithBackground<NodeInner>);

struct NodeInner {
    node_id: NodeId,
    storage: Arc<dyn Storage>,
    meta: Arc<dyn Meta>,
    shards: Arc<dyn Shards>,
    wals: Arc<dyn Wals>,
    meta_synced: Arc<MetaSynced>,

    supervisor: Mutex<Option<Supervisor>>,
    shard: RwLock<Option<Arc<Shard>>>,
}

impl Node {
    pub async fn new(
        node_id: NodeId,
        storage: Arc<dyn Storage>,
        meta: Arc<dyn Meta>,
        shards: Arc<dyn Shards>,
        wals: Arc<dyn Wals>,
        meta_synced: Arc<MetaSynced>,
    ) -> anyhow::Result<Self> {
        meta.add_node(node_id.clone()).await?;

        let inner = Arc::new(NodeInner {
            node_id,
            storage,
            meta,
            shards,
            wals,
            meta_synced: Arc::clone(&meta_synced),
            supervisor: Mutex::new(None),
            shard: RwLock::new(None),
        });
        let node = Node(WithBackground::new(Arc::clone(&inner)));

        meta_synced.subscribe(&node.0);

        Ok(node)
    }
}

impl runtime::Node for Node {
    fn id(&self) -> NodeId {
        self.0.node_id
    }

    fn shard(&self, shard_id: ShardId) -> anyhow::Result<Arc<dyn runtime::Shard>> {
        let maybe_shard = self.0.shard.read().unwrap();
        if let Some(shard) = maybe_shard.as_ref() {
            return Ok(Arc::clone(shard) as Arc<dyn runtime::Shard>);
        } else {
            return Err(anyhow!("{:?} does not own {:?}", self.0.node_id, shard_id));
        }
    }
}

impl NodeInner {
    async fn try_sync_meta(
        &self,
        sync_type: &SyncType,
        snapshot: &MetaSyncedSnapshot,
    ) -> anyhow::Result<()> {
        match sync_type {
            SyncType::Initial => {
                let shard_ids = snapshot.shard_ids().await?;
                for shard_id in shard_ids {
                    self.shard_metadata_changed(snapshot, shard_id).await?;
                }
                self.nodes_changed(snapshot).await?;
            }
            SyncType::Tx(keys) => {
                for key in keys {
                    if let MetaKey::Shard(shard_id) = key {
                        self.shard_metadata_changed(snapshot, *shard_id).await?;
                    }
                }

                if keys.iter().any(|key| matches!(key, MetaKey::Node(_))) {
                    self.nodes_changed(snapshot).await?;
                }
            }
        }

        Ok(())
    }

    async fn nodes_changed(&self, snapshot: &MetaSyncedSnapshot) -> anyhow::Result<()> {
        let mut node_metadatas = snapshot.scan::<NodeMetadata>(MetaKey::nodes(), Direction::Asc);
        let first_node = TryStreamExt::try_next(&mut node_metadatas).await?;

        if let Some((MetaKey::Node(node_id), _)) = first_node {
            if node_id == self.node_id {
                self.maybe_spawn_supervisor().await;
            }
        }

        Ok(())
    }

    async fn shard_metadata_changed(
        &self,
        snapshot: &MetaSyncedSnapshot,
        shard_id: ShardId,
    ) -> anyhow::Result<()> {
        let shard_metadata = snapshot.shard_metadata(shard_id).await?;

        if let Some(node_id) = shard_metadata.assigned_node_id {
            if node_id == self.node_id {
                self.maybe_spawn_shard(shard_id).await?;
            }
        }

        Ok(())
    }

    async fn maybe_spawn_supervisor(&self) {
        let mut supervisor = self.supervisor.lock().unwrap();
        if supervisor.is_some() {
            return;
        }

        *supervisor = Some(Supervisor::new(
            Arc::clone(&self.meta),
            Arc::clone(&self.meta_synced),
            Arc::clone(&self.shards),
        ));
    }

    async fn maybe_spawn_shard(&self, shard_id: ShardId) -> anyhow::Result<()> {
        {
            let maybe_shard = self.shard.write().unwrap();
            if let Some(shard) = maybe_shard.as_ref() {
                if shard.id() == shard_id {
                    return Ok(());
                }
            }
        }

        let shard = Shard::new(
            shard_id,
            Arc::clone(&self.storage),
            Arc::clone(&self.meta),
            Arc::clone(&self.shards),
            Arc::clone(&self.wals),
            65536,
            65536,
            4096,
        )
        .await?;

        {
            let mut maybe_shard = self.shard.write().unwrap();
            *maybe_shard = Some(Arc::new(shard));
        }

        Ok(())
    }
}

#[async_trait]
impl MetaSubscriber for NodeInner {
    async fn sync_meta(&self, sync_type: SyncType, snapshot: MetaSyncedSnapshot) {
        Retry::new()
            .indefinitely(&async || -> anyhow::Result<()> {
                self.try_sync_meta(&sync_type, &snapshot).await
            })
            .await;
    }
}
