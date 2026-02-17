use std::sync::Arc;

use crate::runtime::Shard;
use crate::runtime::Tablet;
use crate::NodeId;
use crate::ShardId;
use crate::TabletId;

pub(crate) trait Node: Send + Sync {
    fn id(&self) -> NodeId;

    fn shard(&self, shard_id: ShardId) -> anyhow::Result<Arc<dyn Shard>>;

    fn tablet(&self, tablet_id: TabletId) -> anyhow::Result<Arc<dyn Tablet>> {
        self.shard(tablet_id.0)?.tablet(tablet_id)
    }
}
