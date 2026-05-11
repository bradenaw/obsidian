//! Nodes represent a single process, and can be assigned shards to serve.

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
use obsidian_external::Journals;
use obsidian_external::Storage;
use obsidian_lsm::LsmOptions;
use obsidian_util::Owned;
use obsidian_util::Retry;
use obsidian_util::WeakView;
use obsidian_util::WithBackground;
use tokio::sync::Notify;

use crate::election::Proposal;
use crate::meta::Meta;
use crate::meta::MetaKey;
use crate::meta::MetaMutation;
use crate::meta::MetaReader;
use crate::meta::MetaSubscriber;
use crate::meta::MetaSynced;
use crate::meta::MetaSyncedSnapshot;
use crate::meta::SyncType;
use crate::replica::Replica;
use crate::runtime;
use crate::runtime::Nodes;
use crate::runtime::ReplicaState;
use crate::runtime::Shard as _;
use crate::runtime::Shards;
use crate::supervisor::Supervisor;
use crate::Bound;
use crate::ColoGroupId;
use crate::InternalError;
use crate::JournalEntry;
use crate::KeyspaceId;
use crate::NodeId;
use crate::Range;
use crate::Record;
use crate::Revision;
use crate::ShardId;
use crate::TabletId;
use crate::Timestamp;
use crate::TransferId;

/// Node represents one of the processes of Obsidian running in a system. Nodes register themselves
/// with meta to make themselves available to having shards assigned, at which point they join
/// leader election for the shard.
///
/// Most shards are assigned by the Supervisor via metadata stored in Meta. However, this obviously
/// cannot work for the Meta shard itself nor the Supervisor, so those are boostrapped specially.
/// Instead, the Meta shard is assigned to a handful of the nodes with the lowest NodeIds in sorted
/// order, and the Supervisor is run by whichever is elected as the leader for the Meta shard.
pub(crate) struct Node(WithBackground<NodeInner>);

struct NodeInner {
    node_id: NodeId,
    lsm_options: LsmOptions,
    nodes: Arc<dyn Nodes>,
    storage: Arc<dyn Storage>,
    meta: Arc<dyn runtime::Meta>,
    shards: Arc<dyn Shards>,
    meta_synced: Arc<MetaSynced>,
    journals: Arc<dyn Journals<Proposal<JournalEntry>>>,

    supervisor: RwLock<Option<Owned<Supervisor>>>,
    maybe_meta: RwLock<Option<Owned<Meta>>>,
    replicas: RwLock<HashMap<ShardId, Arc<Replica>>>,
    replicas_changed: Notify,
}

impl Node {
    pub fn new(
        node_id: NodeId,
        nodes: Arc<dyn Nodes>,
        storage: Arc<dyn Storage>,
        meta: Arc<dyn runtime::Meta>,
        shards: Arc<dyn Shards>,
        meta_synced: Arc<MetaSynced>,
        journals: Arc<dyn Journals<Proposal<JournalEntry>>>,
    ) -> Self {
        let inner = Arc::new(NodeInner {
            node_id,
            nodes,
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
            // We just need this to succeed once so that meta is willing to assign shards to us.
            // However, if (for example) we're the first node in a cluster, we're going to be meta,
            // so we need to wait to do this until after the leader election is finished.
            Retry::new()
                .indefinitely(&async || {
                    inner.meta.add_node(inner.node_id).await?;
                    Ok::<(), anyhow::Error>(())
                })
                .await;
        });

        node.0.spawn(async |inner| {
            inner.background_watch_nodes().await;
        });

        node.0.spawn(async |inner| {
            inner.background_spawn_meta().await;
        });

        node
    }
}

impl runtime::Node for Node {
    fn id(&self) -> NodeId {
        self.0.node_id
    }

    fn shard(&self, shard_id: ShardId) -> anyhow::Result<Arc<dyn runtime::Shard>> {
        let replicas = self.0.replicas.read().unwrap();
        if let Some(shard) = replicas.get(&shard_id).as_ref() {
            Ok(Arc::clone(shard) as Arc<dyn runtime::Shard>)
        } else {
            Err(anyhow!("{:?} does not own {:?}", self.0.node_id, shard_id))
        }
    }

    fn meta(&self) -> anyhow::Result<Arc<dyn runtime::Meta>> {
        let maybe_meta = self.0.maybe_meta.read().unwrap();
        let meta = maybe_meta
            .as_ref()
            .ok_or_else(|| anyhow!("{:?} is not currently hosting meta", self.0.node_id))?;
        Ok(Owned::weak(meta))
    }

    fn supervisor(&self) -> anyhow::Result<Arc<dyn runtime::Supervisor>> {
        let maybe_supervisor = self.0.supervisor.read().unwrap();
        let supervisor = maybe_supervisor.as_ref().ok_or_else(|| {
            anyhow!(
                "{:?} is not currently hosting the supervisor",
                self.0.node_id
            )
        })?;
        Ok(Owned::weak(supervisor))
    }

    fn shards_subscribe(
        &self,
    ) -> Box<dyn Stream<Item = anyhow::Result<HashMap<ShardId, ReplicaState>>> + Send + Unpin + '_>
    {
        Box::new(self.0.shards_subscribe().map(Ok))
    }
}

impl NodeInner {
    fn shards_subscribe(
        &self,
    ) -> Box<dyn Stream<Item = HashMap<ShardId, ReplicaState>> + Send + Unpin + '_> {
        Box::new(Box::pin(stream! {
            loop {
                let mut shards = HashMap::new();
                let mut futures = FuturesUnordered::new();
                let replicas_changed = self.replicas_changed.notified();
                futures.push(Either::Left(replicas_changed));
                {
                    let replicas = self.replicas.read().unwrap();
                    for (_, replica) in replicas.iter() {
                        let (became_leader_at, changed) = replica.became_leader_at_subscribe();
                        if let Some(seq) = became_leader_at {
                            shards.insert(replica.id(), ReplicaState::Leader(seq));
                        } else {
                            shards.insert(replica.id(), ReplicaState::Follower);
                        }
                        futures.push(Either::Right(changed));
                    }
                }
                yield shards;
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
        let mut stream = self.shards_subscribe();
        while let Some(shards) = stream.next().await {
            let mut maybe_meta = self.maybe_meta.write().unwrap();
            let mut supervisor = self.supervisor.write().unwrap();
            if matches!(shards.get(&ShardId::META), Some(&ReplicaState::Leader(_))) {
                if maybe_meta.is_none() {
                    let replicas = self.replicas.read().unwrap();
                    // If either of these fall through it implies that we aren't actually the
                    // leader and a later entry in `stream` should tell us that.
                    if let Some(meta_shard) = replicas.get(&ShardId::META) {
                        if let Ok(meta_tablet) = meta_shard.tablet(TabletId::META) {
                            log::info!(
                                "{:?} is leader for {:?}, spawning meta and supervisor",
                                self.node_id,
                                ShardId::META,
                            );
                            let meta = Owned::new(Meta::new(meta_tablet));
                            *supervisor = Some(Owned::new(Supervisor::new(
                                Owned::weak(&meta),
                                Arc::clone(&self.meta_synced),
                                Arc::clone(&self.shards),
                            )));
                            *maybe_meta = Some(meta);
                        }
                    }
                }
            } else {
                *maybe_meta = None;
                *supervisor = None;
            }
        }
    }

    async fn background_watch_nodes(&self) {
        loop {
            let (node_ids, nodes_changed) = self.nodes.node_ids();
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

        log::info!("{:?} joining {:?}", self.node_id, shard_id);

        let replica = Replica::new(
            format!("{:?} {:?}", self.node_id, shard_id), // name, for logging
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

#[async_trait]
impl runtime::Supervisor for WeakView<Supervisor> {
    async fn start_move(&self, src: TabletId, dst: ShardId) -> anyhow::Result<TransferId> {
        self.or_closed(async |inner| inner.start_move(src, dst).await)
            .await
    }

    async fn start_split(
        &self,
        src: TabletId,
        dst_a: ShardId,
        dst_b: ShardId,
    ) -> anyhow::Result<TransferId> {
        self.or_closed(async |inner| inner.start_split(src, dst_a, dst_b).await)
            .await
    }

    async fn start_merge(&self, srcs: Vec<TabletId>, dst: ShardId) -> anyhow::Result<TransferId> {
        self.or_closed(async |inner| inner.start_merge(srcs, dst).await)
            .await
    }

    async fn wait_transfer(&self, transfer_id: TransferId) -> anyhow::Result<()> {
        self.or_closed(async |inner| inner.wait_transfer(transfer_id).await)
            .await
    }
}

#[async_trait]
impl runtime::Meta for WeakView<Meta> {
    async fn add_shard(&self, shard_id: ShardId) -> anyhow::Result<()> {
        self.or_closed(async |inner| inner.add_shard(shard_id).await)
            .await
    }

    async fn add_node(&self, node_id: NodeId) -> anyhow::Result<()> {
        self.or_closed(async |inner| inner.add_node(node_id).await)
            .await
    }

    async fn create_colo_group(
        &self,
        colo_group_id: ColoGroupId,
        initial_splits: Vec<Bound<Vec<u8>>>,
    ) -> anyhow::Result<()> {
        self.or_closed(async |inner| inner.create_colo_group(colo_group_id, initial_splits).await)
            .await
    }

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        self.or_closed(async |inner| inner.create_keyspace(keyspace_id).await)
            .await
    }

    async fn latest_snapshot(&self) -> anyhow::Result<Timestamp> {
        self.or_closed(async |inner| inner.latest_snapshot().await)
            .await
    }

    async fn wait_for_newer(&self, ts: Timestamp) -> anyhow::Result<()> {
        self.or_closed(async |inner| inner.wait_for_newer(ts).await)
            .await
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        range: Range<Vec<u8>>,
    ) -> anyhow::Result<(Vec<Record>, Option<Range<Vec<u8>>>)> {
        self.or_closed(async |inner| inner.scan_page(ts, range).await)
            .await
    }

    async fn sync(&self, ts: Timestamp) -> anyhow::Result<(Vec<Revision>, Timestamp)> {
        self.or_closed(async |inner| inner.sync(ts).await).await
    }

    async fn tablet_ids(&self, ts: Timestamp) -> anyhow::Result<Vec<TabletId>> {
        self.or_closed(async |inner| inner.tablet_ids(ts).await)
            .await
    }

    async fn write(
        &self,
        snapshot_ts: Timestamp,
        muts: HashMap<MetaKey, MetaMutation>,
    ) -> Result<Timestamp, InternalError> {
        self.or_closed(async |inner| inner.write(snapshot_ts, muts).await)
            .await
    }
}
