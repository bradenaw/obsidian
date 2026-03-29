use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::anyhow;
use async_stream::stream;
use async_trait::async_trait;
use futures::future::Either;
use futures::stream::FuturesUnordered;
use futures::Stream;
use futures::StreamExt;
use tokio::sync::Notify;

use crate::election::Proposal;
use crate::lsm::LsmOptions;
use crate::meta::Meta;
use crate::meta::MetaKey;
use crate::meta::MetaReader;
use crate::meta::MetaSubscriber;
use crate::meta::MetaSynced;
use crate::meta::MetaSyncedSnapshot;
use crate::meta::SyncType;
use crate::replica::Replica;
use crate::runtime;
use crate::runtime::Discovery;
use crate::runtime::Journals;
use crate::runtime::Registration;
use crate::runtime::Shard as _;
use crate::runtime::Shards;
use crate::runtime::Storage;
use crate::supervisor::Supervisor;
use crate::util::Retry;
use crate::util::WithBackground;
use crate::JournalEntry;
use crate::JournalSeq;
use crate::NodeId;
use crate::ShardId;
use crate::TabletId;

pub(crate) struct Node(WithBackground<NodeInner>);

struct NodeInner {
    node_id: NodeId,
    lsm_options: LsmOptions,
    discovery: Arc<dyn Discovery>,
    registration: Box<dyn Registration>,
    storage: Arc<dyn Storage>,
    meta: Arc<dyn runtime::Meta>,
    shards: Arc<dyn Shards>,
    meta_synced: Arc<MetaSynced>,
    journals: Arc<dyn Journals<Proposal<JournalEntry>>>,

    supervisor: RwLock<Option<Supervisor>>,
    maybe_meta: RwLock<Option<Meta>>,
    replicas: RwLock<HashMap<ShardId, Arc<Replica>>>,
    replicas_changed: Notify,
}

impl Node {
    pub async fn new(
        node_id: NodeId,
        discovery: Arc<dyn Discovery>,
        storage: Arc<dyn Storage>,
        meta: Arc<dyn runtime::Meta>,
        shards: Arc<dyn Shards>,
        meta_synced: Arc<MetaSynced>,
        journals: Arc<dyn Journals<Proposal<JournalEntry>>>,
    ) -> anyhow::Result<Self> {
        meta.add_node(node_id.clone()).await?;

        let inner = Arc::new(NodeInner {
            node_id,
            discovery: Arc::clone(&discovery),
            registration: discovery.register(node_id),
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

            supervisor: RwLock::new(None),
            maybe_meta: RwLock::new(None),
            replicas: RwLock::new(HashMap::new()),
            replicas_changed: Notify::new(),
        });
        let node = Node(WithBackground::new(Arc::clone(&inner)));

        meta_synced.subscribe(&node.0);

        node.0.spawn(async |inner| {
            inner.background_watch_nodes().await;
        });
        node.0.spawn(async |inner| {
            inner.background_spawn_meta().await;
        });

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

    fn meta(&self) -> anyhow::Result<Arc<dyn runtime::Meta>> {
        todo!();
    }

    fn supervisor(&self) -> anyhow::Result<Arc<dyn runtime::Supervisor>> {
        todo!();
    }

    fn became_leader_at_subscribe(
        &self,
    ) -> Box<dyn Stream<Item = anyhow::Result<HashMap<ShardId, JournalSeq>>> + Send + Unpin + '_>
    {
        Box::new(self.0.became_leader_at_subscribe().map(|shards| Ok(shards)))
    }
}

impl NodeInner {
    fn became_leader_at_subscribe(
        &self,
    ) -> Box<dyn Stream<Item = HashMap<ShardId, JournalSeq>> + Send + Unpin + '_> {
        Box::new(Box::pin(stream! {
            loop {
                let mut leader_shards = HashMap::new();
                let mut futures = FuturesUnordered::new();
                let replicas_changed = self.replicas_changed.notified();
                futures.push(Either::Left(replicas_changed));
                {
                    let replicas = self.replicas.read().unwrap();
                    for (_, replica) in replicas.iter() {
                        let (became_leader_at, changed) = replica.became_leader_at_subscribe();
                        if let Some(seq) = became_leader_at {
                            leader_shards.insert(replica.id(), seq);
                        }
                        futures.push(Either::Right(changed));
                    }
                }
                yield leader_shards;
                futures.next().await;
            }
        }))
    }

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
            }
            SyncType::Tx(keys) => {
                for key in keys {
                    if let MetaKey::Shard(shard_id) = key {
                        self.shard_metadata_changed(snapshot, *shard_id).await?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn shard_metadata_changed(
        &self,
        snapshot: &MetaSyncedSnapshot,
        shard_id: ShardId,
    ) -> anyhow::Result<()> {
        // This is taken care of by background_watch_nodes().
        if shard_id == ShardId::META {
            return Ok(());
        }

        let shard_metadata = snapshot.shard_metadata(shard_id).await?;

        if shard_metadata.assigned_node_ids.contains(&self.node_id) {
            self.ensure_replica(shard_id).await;
        } else {
            self.remove_replica(shard_id).await;
        }

        Ok(())
    }

    // If this node is the leader for ShardId::META, then we want to run Meta and Supervisor here.
    async fn background_spawn_meta(&self) {
        let mut stream = self.became_leader_at_subscribe();
        while let Some(shards) = stream.next().await {
            let mut maybe_meta = self.maybe_meta.write().unwrap();
            let mut supervisor = self.supervisor.write().unwrap();
            if shards.contains_key(&ShardId::META) {
                if maybe_meta.is_none() {
                    let replicas = self.replicas.read().unwrap();
                    // If either of these fall through it implies that we aren't actually the
                    // leader and a later entry in `stream` should tell us that.
                    if let Some(meta_shard) = replicas.get(&ShardId::META) {
                        if let Ok(meta_tablet) = meta_shard.tablet(TabletId::META) {
                            *maybe_meta = Some(Meta::new(meta_tablet));
                        }
                    }
                }

                if supervisor.is_none() {
                    *supervisor = Some(Supervisor::new(
                        Arc::clone(&self.meta),
                        Arc::clone(&self.meta_synced),
                        Arc::clone(&self.shards),
                    ));
                }
            } else {
                *maybe_meta = None;
                *supervisor = None;
            }
        }
    }

    async fn background_watch_nodes(&self) {
        loop {
            let (node_ids, nodes_changed) = self.discovery.node_ids();
            let should_join_meta = node_ids
                .iter()
                .take(3)
                .any(|node_id| *node_id == self.node_id);
            if should_join_meta {
                self.ensure_replica(ShardId::META).await;
            } else {
                self.remove_replica(ShardId::META).await;
            }

            nodes_changed.await;
        }
    }

    async fn ensure_replica(&self, shard_id: ShardId) {
        {
            let replicas = self.replicas.read().unwrap();
            if replicas.contains_key(&shard_id) {
                return;
            }
        }

        let replica = Replica::new(
            format!("{} {}", self.node_id, shard_id), // name, for logging
            shard_id,
            self.lsm_options.clone(),
            Arc::clone(&self.storage),
            Arc::clone(&self.meta),
            Arc::clone(&self.shards),
            self.journals.journal(shard_id).await,
        );

        let mut replicas = self.replicas.write().unwrap();
        replicas.insert(shard_id, Arc::new(replica));

        self.replicas_changed.notify_waiters();
    }

    async fn remove_replica(&self, shard_id: ShardId) {
        let mut replicas = self.replicas.write().unwrap();
        let removed = replicas.remove(&shard_id).is_some();
        if removed {
            self.replicas_changed.notify_waiters();
        }
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
