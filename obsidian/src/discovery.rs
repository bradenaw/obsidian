//! Discovery's purpose is to locate logical objects in the system.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::TryStreamExt;
use im::OrdSet;
use obsidian_util::spawn_owned;
use obsidian_util::OwnedJoinHandle;
use obsidian_util::Retry;
use obsidian_util::WithBackground;

use crate::meta::MetaKey;
use crate::meta::MetaMutation;
use crate::runtime;
use crate::runtime::Nodes;
use crate::runtime::ReplicaState;
use crate::Bound;
use crate::ColoGroupId;
use crate::Direction;
use crate::HistoryRange;
use crate::InternalError;
use crate::JournalSeq;
use crate::Key;
use crate::KeyspaceId;
use crate::Manifest;
use crate::Mutation;
use crate::NodeId;
use crate::Precondition;
use crate::Range;
use crate::Record;
use crate::Revision;
use crate::ShardId;
use crate::TabletId;
use crate::Timestamp;
use crate::TransferId;
use crate::TxOutcome;
use crate::Txid;

/// Discovery's purpose is to locate logical objects in the system.
///
/// [`runtime::Nodes`]'s purpose, by contrast, is just to provide the 'physical' nodes in the
/// system.
///
/// Nodes get assigned shards, and then nodes assigned to the same shard elect a leader amongst
/// themselves, which means a logical shard moves around among nodes. Discovery keeps track of
/// where that logical shard is.
pub(crate) struct Discovery {
    bg: WithBackground<DiscoveryInner>,
    inner: Arc<DiscoveryInner>,
    meta_proxy: Arc<dyn runtime::Meta>,
    supervisor_proxy: Arc<dyn runtime::Supervisor>,
}

struct DiscoveryInner {
    nodes: Arc<dyn Nodes>,
    routing: Arc<RwLock<HashMap<ShardId, ShardRouting>>>,
}

impl Discovery {
    pub fn new(nodes: Arc<dyn Nodes>) -> Self {
        let inner = Arc::new(DiscoveryInner {
            nodes,
            routing: Arc::new(RwLock::new(HashMap::new())),
        });

        let discovery = Discovery {
            bg: WithBackground::new(Arc::clone(&inner)),
            meta_proxy: Arc::new(MetaProxy {
                parent: Arc::clone(&inner),
            }),
            supervisor_proxy: Arc::new(SupervisorProxy {
                parent: Arc::clone(&inner),
            }),
            inner,
        };

        discovery.bg.spawn(async |inner| {
            inner.background_watch_nodes().await;
        });

        discovery
    }

    pub fn meta(&self) -> Arc<dyn runtime::Meta> {
        Arc::clone(&self.meta_proxy)
    }

    pub fn supervisor(&self) -> Arc<dyn runtime::Supervisor> {
        Arc::clone(&self.supervisor_proxy)
    }

    #[cfg(test)]
    // This is cfg(test) because callers should really prefer to use .shard(), which will give a
    // proxy object whose methods all find the current leader on invocation. This gives direct
    // access to the node, which may not continue to be the leader into the future.
    pub fn current_leader(&self, shard_id: ShardId) -> anyhow::Result<Arc<dyn runtime::Node>> {
        self.inner.current_leader(shard_id)
    }
}

impl runtime::Shards for Discovery {
    fn shard(&self, shard_id: ShardId) -> anyhow::Result<Arc<dyn runtime::Shard>> {
        Ok(Arc::new(ShardProxy {
            parent: Arc::clone(&self.inner),
            shard_id,
        }))
    }
}

impl DiscoveryInner {
    async fn background_watch_nodes(&self) {
        let mut last_node_ids = OrdSet::new();
        let mut leader_subscriptions = HashMap::new();
        loop {
            let (node_ids, nodes_changed) = self.nodes.node_ids();

            for diff_item in last_node_ids.diff(&node_ids) {
                match diff_item {
                    im::ordset::DiffItem::Add(node_id) => {
                        if !leader_subscriptions.contains_key(node_id) {
                            log::debug!("discovery subscribing to {:?}", node_id);
                            leader_subscriptions.insert(*node_id, self.subscribe_leader(*node_id));
                        }
                    }
                    im::ordset::DiffItem::Update { .. } => {}
                    im::ordset::DiffItem::Remove(node_id) => {
                        log::debug!("discovery unsubscribing from {:?}", node_id);
                        leader_subscriptions.remove(node_id);
                    }
                }
            }

            last_node_ids = node_ids;
            nodes_changed.await;
            log::debug!("discovery: nodes changed");
        }
    }

    fn subscribe_leader(&self, node_id: NodeId) -> OwnedJoinHandle<()> {
        let routing_lock = Arc::clone(&self.routing);
        let nodes = Arc::clone(&self.nodes);
        spawn_owned(async move {
            Retry::new()
                .indefinitely(&async || {
                    let node = nodes.node(node_id)?;

                    let mut stream = node.shards_subscribe();
                    while let Some(shards) = stream.try_next().await? {
                        let mut routing = routing_lock.write().unwrap();
                        for (shard_id, replica_state) in shards {
                            if let ReplicaState::Leader(seq) = replica_state {
                                log::debug!(
                                    "discovery learned {:?} became leader for {:?} at {:?}",
                                    node_id,
                                    shard_id,
                                    seq
                                );
                                let shard_routing =
                                    routing.entry(shard_id).or_insert_with(ShardRouting::empty);
                                if let Some((_, other_seq)) = shard_routing.leader {
                                    if seq > other_seq {
                                        shard_routing.leader = Some((node_id, seq));
                                    }
                                } else {
                                    shard_routing.leader = Some((node_id, seq));
                                }
                            }
                        }
                    }

                    Ok::<(), anyhow::Error>(())
                })
                .await;
        })
    }

    fn current_leader(&self, shard_id: ShardId) -> anyhow::Result<Arc<dyn runtime::Node>> {
        self.nodes.node(self.current_leader_id(shard_id)?)
    }

    fn current_leader_id(&self, shard_id: ShardId) -> anyhow::Result<NodeId> {
        let routing = self.routing.read().unwrap();
        routing
            .get(&shard_id)
            .ok_or_else(|| anyhow!("{:?} not in the routing table", shard_id))?
            .leader
            .map(|(node_id, _)| node_id)
            .ok_or_else(|| anyhow!("{:?}'s leader is not known", shard_id))
    }
}

struct ShardRouting {
    // The node that is probably the leader, and the journal seq that it acquired its leader lease
    // at. We hold the JournalSeq so we can recognize the newest information.
    //
    // This may be out of date, it's possible that this node has already disappeared or
    // relinquished its lease.
    leader: Option<(NodeId, JournalSeq)>,
}

impl ShardRouting {
    fn empty() -> ShardRouting {
        ShardRouting { leader: None }
    }
}

struct ShardProxy {
    parent: Arc<DiscoveryInner>,
    shard_id: ShardId,
}

#[async_trait]
impl runtime::Shard for ShardProxy {
    fn id(&self) -> ShardId {
        self.shard_id
    }

    fn tablet(&self, tablet_id: TabletId) -> anyhow::Result<Arc<dyn runtime::Tablet>> {
        if tablet_id.0 != self.shard_id {
            return Err(anyhow!(
                "wrong shard {:?} for {:?}",
                self.shard_id,
                tablet_id
            ));
        }
        Ok(Arc::new(TabletProxy {
            parent: Arc::clone(&self.parent),
            tablet_id,
        }))
    }

    async fn wait_meta_sync(&self, ts: Timestamp) -> anyhow::Result<()> {
        self.parent
            .current_leader(self.shard_id)?
            .shard(self.shard_id)?
            .wait_meta_sync(ts)
            .await
    }

    async fn tx_try_commit(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<TxOutcome> {
        self.parent
            .current_leader(self.shard_id)?
            .shard(self.shard_id)?
            .tx_try_commit(txid, ts, precond_keys, mut_keys)
            .await
    }

    async fn tx_try_abort(&self, txid: Txid) -> anyhow::Result<TxOutcome> {
        self.parent
            .current_leader(self.shard_id)?
            .shard(self.shard_id)?
            .tx_try_abort(txid)
            .await
    }

    async fn tx_wait(&self, txid: Txid) -> Result<TxOutcome, InternalError> {
        self.parent
            .current_leader(self.shard_id)?
            .shard(self.shard_id)?
            .tx_wait(txid)
            .await
    }
}

// The leader for a tablet can change but we want to hand out an object that can be used
// indefinitely.
struct TabletProxy {
    parent: Arc<DiscoveryInner>,
    tablet_id: TabletId,
}

impl TabletProxy {
    fn get_tablet(&self) -> anyhow::Result<Arc<dyn runtime::Tablet>> {
        self.parent
            .current_leader(self.tablet_id.0)?
            .tablet(self.tablet_id)
    }
}

#[async_trait]
impl runtime::Tablet for TabletProxy {
    async fn get_multi(
        &self,
        ts: Timestamp,
        keys: BTreeSet<Key>,
    ) -> Result<BTreeMap<Key, Record>, InternalError> {
        self.get_tablet()?.get_multi(ts, keys).await
    }

    async fn get_latest_multi(
        &self,
        keys: BTreeSet<Key>,
    ) -> Result<(Timestamp, BTreeMap<Key, Record>), InternalError> {
        self.get_tablet()?.get_latest_multi(keys).await
    }

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> Result<Timestamp, InternalError> {
        self.get_tablet()?.latest_snapshot(keys).await
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<Range<Vec<u8>>>), InternalError> {
        self.get_tablet()?
            .scan_page(ts, keyspace_id, range, direction, limit)
            .await
    }

    async fn history_page(
        &self,
        key: Key,
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Revision>, Option<HistoryRange>), InternalError> {
        self.get_tablet()?
            .history_page(key, range, direction, limit)
            .await
    }

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        self.get_tablet()?.write(preconds, muts).await
    }

    async fn prepare(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        self.get_tablet()?.prepare(txid, preconds, muts).await
    }

    async fn cleanup_committed(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        self.get_tablet()?
            .cleanup_committed(txid, ts, precond_keys, mut_keys)
            .await
    }

    async fn manifest(&self) -> anyhow::Result<Manifest> {
        self.get_tablet()?.manifest().await
    }

    async fn wait_mostly_hydrated(&self) -> anyhow::Result<()> {
        self.get_tablet()?.wait_mostly_hydrated().await
    }

    async fn catchup(&self) -> anyhow::Result<()> {
        self.get_tablet()?.catchup().await
    }

    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>> {
        self.get_tablet()?.find_split().await
    }
}

struct MetaProxy {
    parent: Arc<DiscoveryInner>,
}

impl MetaProxy {
    fn get_meta(&self) -> anyhow::Result<Arc<dyn runtime::Meta>> {
        self.parent.current_leader(ShardId::META)?.meta()
    }
}

#[async_trait]
impl runtime::Meta for MetaProxy {
    async fn add_shard(&self, shard_id: ShardId) -> anyhow::Result<()> {
        self.get_meta()?.add_shard(shard_id).await
    }

    async fn add_node(&self, node_id: NodeId) -> anyhow::Result<()> {
        self.get_meta()?.add_node(node_id).await
    }

    async fn create_colo_group(
        &self,
        colo_group_id: ColoGroupId,
        initial_splits: Vec<Bound<Vec<u8>>>,
    ) -> anyhow::Result<()> {
        self.get_meta()?
            .create_colo_group(colo_group_id, initial_splits)
            .await
    }

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        self.get_meta()?.create_keyspace(keyspace_id).await
    }

    async fn latest_snapshot(&self) -> anyhow::Result<Timestamp> {
        self.get_meta()?.latest_snapshot().await
    }

    async fn wait_for_newer(&self, ts: Timestamp) -> anyhow::Result<()> {
        self.get_meta()?.wait_for_newer(ts).await
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        range: Range<Vec<u8>>,
    ) -> anyhow::Result<(Vec<Record>, Option<Range<Vec<u8>>>)> {
        self.get_meta()?.scan_page(ts, range).await
    }

    async fn sync(&self, ts: Timestamp) -> anyhow::Result<(Vec<Revision>, Timestamp)> {
        self.get_meta()?.sync(ts).await
    }

    async fn tablet_ids(&self, ts: Timestamp) -> anyhow::Result<Vec<TabletId>> {
        self.get_meta()?.tablet_ids(ts).await
    }

    async fn write(
        &self,
        snapshot_ts: Timestamp,
        muts: HashMap<MetaKey, MetaMutation>,
    ) -> Result<Timestamp, InternalError> {
        self.get_meta()?.write(snapshot_ts, muts).await
    }
}

struct SupervisorProxy {
    parent: Arc<DiscoveryInner>,
}

impl SupervisorProxy {
    fn get_supervisor(&self) -> anyhow::Result<Arc<dyn runtime::Supervisor>> {
        self.parent.current_leader(ShardId::META)?.supervisor()
    }
}

#[async_trait]
impl runtime::Supervisor for SupervisorProxy {
    async fn start_move(&self, src: TabletId, dst: ShardId) -> anyhow::Result<TransferId> {
        self.get_supervisor()?.start_move(src, dst).await
    }

    async fn start_split(
        &self,
        src: TabletId,
        dst_a: ShardId,
        dst_b: ShardId,
    ) -> anyhow::Result<TransferId> {
        self.get_supervisor()?.start_split(src, dst_a, dst_b).await
    }

    async fn start_merge(&self, srcs: Vec<TabletId>, dst: ShardId) -> anyhow::Result<TransferId> {
        self.get_supervisor()?.start_merge(srcs, dst).await
    }

    async fn wait_transfer(&self, transfer_id: TransferId) -> anyhow::Result<()> {
        self.get_supervisor()?.wait_transfer(transfer_id).await
    }
}
