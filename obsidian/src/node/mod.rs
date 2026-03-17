use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::TryStreamExt;

use crate::election::Proposal;
use crate::lsm::LsmOptions;
use crate::meta::MetaKey;
use crate::meta::MetaReader;
use crate::meta::MetaSubscriber;
use crate::meta::MetaSynced;
use crate::meta::MetaSyncedSnapshot;
use crate::meta::SyncType;
use crate::replica::Replica;
use crate::replica::ShardEntry;
use crate::runtime;
use crate::runtime::Journals;
use crate::runtime::Meta;
use crate::runtime::Shard as _;
use crate::runtime::Shards;
use crate::runtime::Storage;
use crate::shard::Shard;
use crate::shard::ShardJournalWriter;
use crate::supervisor::Supervisor;
use crate::util::Retry;
use crate::util::WithBackground;
use crate::Direction;
use crate::NodeId;
use crate::ShardId;

pub(crate) struct Node(WithBackground<NodeInner>);

struct NodeInner {
    node_id: NodeId,
    lsm_options: LsmOptions,
    storage: Arc<dyn Storage>,
    meta: Arc<dyn Meta>,
    shards: Arc<dyn Shards>,
    meta_synced: Arc<MetaSynced>,
    journals: Arc<dyn Journals<Proposal<ShardEntry>>>,

    supervisor: Mutex<Option<Supervisor>>,
    replicas: RwLock<HashMap<ShardId, Arc<Replica>>>,
}

impl Node {
    pub async fn new(
        node_id: NodeId,
        storage: Arc<dyn Storage>,
        meta: Arc<dyn Meta>,
        shards: Arc<dyn Shards>,
        meta_synced: Arc<MetaSynced>,
        journals: Arc<dyn Journals<Proposal<ShardEntry>>>,
    ) -> anyhow::Result<Self> {
        meta.add_node(node_id.clone()).await?;

        let inner = Arc::new(NodeInner {
            node_id,
            lsm_options: LsmOptions {
                l0_max_size: 256,
                l1_max_size: 100_000,
                run_size_target: 32768,
                block_size_target: 4096,
            },
            storage,
            meta,
            shards,
            meta_synced: Arc::clone(&meta_synced),
            journals,

            supervisor: Mutex::new(None),
            replicas: RwLock::new(HashMap::new()),
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
        let replicas = self.0.replicas.read().unwrap();
        if let Some(shard) = replicas.get(&shard_id).as_ref() {
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
        let mut node_metadatas = snapshot.scan(MetaKey::nodes(), Direction::Asc);
        let first_node = TryStreamExt::try_next(&mut node_metadatas).await?;

        if let Some((MetaKey::Node(node_id), _)) = first_node {
            if node_id == self.node_id {
                // TODO: Actually do this. ObsidianForTest spawns its own.
                // self.maybe_spawn_supervisor().await;
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

        let action = {
            let replicas = self.replicas.read().unwrap();
            if shard_metadata.assigned_node_ids.contains(&self.node_id)
                && !replicas.contains_key(&shard_id)
            {
                Some(true)
            } else if !shard_metadata.assigned_node_ids.contains(&self.node_id)
                && replicas.contains_key(&shard_id)
            {
                Some(false)
            } else {
                None
            }
        };

        if let Some(join) = action {
            if join {
                let replica = Replica::new(
                    shard_id,
                    self.lsm_options.clone(),
                    Arc::clone(&self.storage),
                    Arc::clone(&self.meta),
                    Arc::clone(&self.shards),
                    self.journals.journal(shard_id).await?,
                );
                let mut replicas = self.replicas.write().unwrap();
                replicas.insert(shard_id, Arc::new(replica));
            } else {
                let mut replicas = self.replicas.write().unwrap();
                replicas.remove(&shard_id);
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

struct NoopShardJournalWriter {}

#[async_trait]
impl ShardJournalWriter for NoopShardJournalWriter {
    async fn append(&self, _entry: ShardEntry) -> anyhow::Result<()> {
        Ok(())
    }
}
