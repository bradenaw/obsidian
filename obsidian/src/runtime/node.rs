use std::collections::HashMap;
use std::sync::Arc;

use futures::Stream;

use crate::runtime::Meta;
use crate::runtime::Shard;
use crate::runtime::Supervisor;
use crate::runtime::Tablet;
use crate::JournalSeq;
use crate::NodeId;
use crate::ShardId;
use crate::TabletId;

pub(crate) trait Node: Send + Sync {
    fn id(&self) -> NodeId;

    fn shard(&self, shard_id: ShardId) -> anyhow::Result<Arc<dyn Shard>>;

    fn tablet(&self, tablet_id: TabletId) -> anyhow::Result<Arc<dyn Tablet>> {
        self.shard(tablet_id.0)?.tablet(tablet_id)
    }

    fn meta(&self) -> anyhow::Result<Arc<dyn Meta>>;

    fn supervisor(&self) -> anyhow::Result<Arc<dyn Supervisor>>;

    /// Subscribe to the shards held on this node.
    ///
    /// This stream will not necessarily receive every update, but will eventually receive the
    /// latest state.
    fn shards_subscribe(
        &self,
    ) -> Box<dyn Stream<Item = anyhow::Result<HashMap<ShardId, ReplicaState>>> + Send + Unpin + '_>;
}

pub(crate) enum ReplicaState {
    /// Contains the journal sequence number where the leader lease was acquired.
    Leader(JournalSeq),
    Follower,
}
