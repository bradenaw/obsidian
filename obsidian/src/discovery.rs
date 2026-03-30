use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::TryStreamExt;
use im::OrdSet;

use crate::lsm::Manifest;
use crate::runtime;
use crate::runtime::Nodes;
use crate::util::spawn_owned;
use crate::util::OwnedJoinHandle;
use crate::util::Retry;
use crate::util::WithBackground;
use crate::Bound;
use crate::Direction;
use crate::HistoryRange;
use crate::InternalError;
use crate::JournalSeq;
use crate::Key;
use crate::KeyspaceId;
use crate::Mutation;
use crate::NodeId;
use crate::Precondition;
use crate::Range;
use crate::Record;
use crate::Revision;
use crate::ShardId;
use crate::TabletId;
use crate::Timestamp;
use crate::TxOutcome;
use crate::Txid;

pub(crate) struct Discovery {
    bg: WithBackground<DiscoveryInner>,
    inner: Arc<DiscoveryInner>,
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
            inner,
        };

        discovery.bg.spawn(async |inner| {
            inner.background_watch_nodes().await;
        });

        discovery
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
                        leader_subscriptions.insert(*node_id, self.subscribe_leader(*node_id));
                    }
                    im::ordset::DiffItem::Update { .. } => {}
                    im::ordset::DiffItem::Remove(node_id) => {
                        leader_subscriptions.remove(node_id);
                    }
                }
            }

            last_node_ids = node_ids;
            nodes_changed.await;
        }
    }

    fn subscribe_leader(&self, node_id: NodeId) -> OwnedJoinHandle<()> {
        let routing_lock = Arc::clone(&self.routing);
        let nodes = Arc::clone(&self.nodes);
        spawn_owned(async move {
            Retry::new()
                .indefinitely(&async || {
                    let node = nodes.node(node_id)?;

                    let mut stream = node.became_leader_at_subscribe();
                    while let Some(shards) = stream.try_next().await? {
                        let mut routing = routing_lock.write().unwrap();
                        for (shard_id, seq) in shards {
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

                    Ok::<(), anyhow::Error>(())
                })
                .await;
        })
    }

    fn current_leader(&self, shard_id: ShardId) -> anyhow::Result<Arc<dyn runtime::Shard>> {
        let leader_node_id = {
            let routing = self.routing.read().unwrap();
            routing
                .get(&shard_id)
                .ok_or_else(|| anyhow!("{:?} not in the routing table", shard_id))?
                .leader
                .map(|(node_id, _)| node_id)
                .ok_or_else(|| anyhow!("{:?}'s leader is not known", shard_id))?
        };

        self.nodes.node(leader_node_id)?.shard(shard_id)
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
            tablet_id: tablet_id,
        }))
    }

    async fn wait_meta_sync(&self, ts: Timestamp) -> anyhow::Result<()> {
        self.parent
            .current_leader(self.shard_id)?
            .wait_meta_sync(ts)
            .await
    }
}

// The leader for a tablet can change but we want to hand out an object that can be used
// indefinitely.
struct TabletProxy {
    parent: Arc<DiscoveryInner>,
    tablet_id: TabletId,
}

#[async_trait]
impl runtime::Tablet for TabletProxy {
    async fn get(&self, ts: Timestamp, key: &Key) -> Result<Option<Record>, InternalError> {
        self.parent
            .current_leader(self.tablet_id.0)?
            .tablet(self.tablet_id)?
            .get(ts, key)
            .await
    }

    async fn get_latest(&self, key: Key) -> Result<(Timestamp, Option<Record>), InternalError> {
        self.parent
            .current_leader(self.tablet_id.0)?
            .tablet(self.tablet_id)?
            .get_latest(key)
            .await
    }

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> Result<Timestamp, InternalError> {
        self.parent
            .current_leader(self.tablet_id.0)?
            .tablet(self.tablet_id)?
            .latest_snapshot(keys)
            .await
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<Range<Vec<u8>>>), InternalError> {
        self.parent
            .current_leader(self.tablet_id.0)?
            .tablet(self.tablet_id)?
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
        self.parent
            .current_leader(self.tablet_id.0)?
            .tablet(self.tablet_id)?
            .history_page(key, range, direction, limit)
            .await
    }

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        self.parent
            .current_leader(self.tablet_id.0)?
            .tablet(self.tablet_id)?
            .write(preconds, muts)
            .await
    }

    async fn prepare(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        self.parent
            .current_leader(self.tablet_id.0)?
            .tablet(self.tablet_id)?
            .prepare(txid, preconds, muts)
            .await
    }

    async fn try_commit(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<TxOutcome> {
        self.parent
            .current_leader(self.tablet_id.0)?
            .tablet(self.tablet_id)?
            .try_commit(txid, ts, precond_keys, mut_keys)
            .await
    }

    async fn try_abort(&self, txid: Txid) -> anyhow::Result<TxOutcome> {
        self.parent
            .current_leader(self.tablet_id.0)?
            .tablet(self.tablet_id)?
            .try_abort(txid)
            .await
    }

    async fn wait(&self, txid: Txid) -> Result<TxOutcome, InternalError> {
        self.parent
            .current_leader(self.tablet_id.0)?
            .tablet(self.tablet_id)?
            .wait(txid)
            .await
    }

    async fn cleanup_committed(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        self.parent
            .current_leader(self.tablet_id.0)?
            .tablet(self.tablet_id)?
            .cleanup_committed(txid, ts, precond_keys, mut_keys)
            .await
    }

    async fn wait_meta_sync(&self, ts: Timestamp) -> anyhow::Result<()> {
        self.parent
            .current_leader(self.tablet_id.0)?
            .tablet(self.tablet_id)?
            .wait_meta_sync(ts)
            .await
    }

    async fn manifest(&self) -> anyhow::Result<Manifest> {
        self.parent
            .current_leader(self.tablet_id.0)?
            .tablet(self.tablet_id)?
            .manifest()
            .await
    }

    async fn wait_mostly_hydrated(&self) -> anyhow::Result<()> {
        self.parent
            .current_leader(self.tablet_id.0)?
            .tablet(self.tablet_id)?
            .wait_mostly_hydrated()
            .await
    }

    async fn catchup(&self) -> anyhow::Result<()> {
        self.parent
            .current_leader(self.tablet_id.0)?
            .tablet(self.tablet_id)?
            .catchup()
            .await
    }

    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>> {
        self.parent
            .current_leader(self.tablet_id.0)?
            .tablet(self.tablet_id)?
            .find_split()
            .await
    }
}
