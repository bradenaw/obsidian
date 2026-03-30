use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::TryStreamExt;

use crate::meta::MetaKey;
use crate::meta::MetaReader;
use crate::meta::MetaSubscriber;
use crate::meta::MetaSynced;
use crate::meta::MetaSyncedSnapshot;
use crate::meta::SyncType;
use crate::runtime;
use crate::runtime::Nodes;
use crate::util::spawn_owned;
use crate::util::OwnedJoinHandle;
use crate::util::Retry;
use crate::util::WithBackground;
use crate::JournalSeq;
use crate::NodeId;
use crate::ShardId;

pub(crate) struct Shards(WithBackground<ShardsInner>);

struct ShardsInner {
    nodes: Arc<dyn Nodes>,
    routing: Arc<RwLock<HashMap<ShardId, ShardRouting>>>,
    leader_subscriptions: RwLock<HashMap<NodeId, OwnedJoinHandle<()>>>,
}

impl Shards {
    pub fn new(meta_synced: Arc<MetaSynced>, nodes: Arc<dyn Nodes>) -> Self {
        let shards = Shards(WithBackground::new(Arc::new(ShardsInner {
            nodes,
            routing: Arc::new(RwLock::new(HashMap::new())),
            leader_subscriptions: RwLock::new(HashMap::new()),
        })));

        meta_synced.subscribe(&shards.0);

        shards
    }
}

impl runtime::Shards for Shards {
    fn shard(&self, shard_id: ShardId) -> anyhow::Result<Arc<dyn runtime::Shard>> {
        let leader_node_id = {
            let routing = self.0.routing.read().unwrap();
            routing
                .get(&shard_id)
                .ok_or_else(|| anyhow!("{:?} not in the routing table", shard_id))?
                .leader
                .map(|(node_id, _)| node_id)
                .ok_or_else(|| anyhow!("{:?}'s leader is not known", shard_id))?
        };

        self.0.nodes.node(leader_node_id)?.shard(shard_id)
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
                let mut node_ids = snapshot.node_ids();
                while let Some(node_id) = node_ids.try_next().await? {
                    self.subscribe_leader(node_id);
                }
            }
            SyncType::Tx(keys) => {
                for key in keys {
                    match key {
                        MetaKey::Shard(shard_id) => {
                            self.sync_meta_shard_metadata(snapshot, *shard_id).await?;
                        }
                        MetaKey::Node(node_id) => {
                            if snapshot.node_exists(*node_id).await? {
                                self.subscribe_leader(*node_id);
                            } else {
                                self.leader_subscriptions.write().unwrap().remove(node_id);
                            }
                        }
                        _ => {}
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
        let shard_routing = routing.entry(shard_id).or_insert_with(ShardRouting::empty);
        shard_routing.participants = shard_metadata.assigned_node_ids.clone();

        Ok(())
    }

    fn subscribe_leader(&self, node_id: NodeId) {
        if self
            .leader_subscriptions
            .read()
            .unwrap()
            .contains_key(&node_id)
        {
            return;
        }

        let routing_lock = Arc::clone(&self.routing);
        let nodes = Arc::clone(&self.nodes);
        let join_handle = spawn_owned(async move {
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
        });

        self.leader_subscriptions
            .write()
            .unwrap()
            .insert(node_id, join_handle);
    }
}

#[async_trait]
impl MetaSubscriber for ShardsInner {
    async fn sync_meta(&self, sync_type: SyncType, snapshot: MetaSyncedSnapshot) {
        Retry::new()
            .indefinitely(&async || -> anyhow::Result<()> {
                self.try_sync_meta(&sync_type, &snapshot)
                    .await
                    .inspect_err(|e| log::error!("error in ShardsInner::sync_meta: {}", e))
            })
            .await;
    }
}

struct ShardRouting {
    // The node that is probably the leader, and the journal seq that it acquired its leader lease
    // at. We hold the JournalSeq so we can recognize the newest information.
    //
    // This may be out of date, it's possible that this node has already disappeared or
    // relinquished its lease.
    leader: Option<(NodeId, JournalSeq)>,
    participants: HashSet<NodeId>,
}

impl ShardRouting {
    fn empty() -> ShardRouting {
        ShardRouting {
            leader: None,
            participants: HashSet::new(),
        }
    }
}
