use std::collections::HashMap;

use async_trait::async_trait;

use crate::NodeId;
use crate::meta::MetaKey;
use crate::Bound;
use crate::ColoGroupId;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Range;
use crate::Record;
use crate::Revision;
use crate::ShardId;
use crate::TabletId;
use crate::Timestamp;

#[async_trait]
pub(crate) trait Meta: Send + Sync {
    async fn add_shard(&self, shard_id: ShardId) -> anyhow::Result<()>;

    async fn add_node(&self, node_id: NodeId) -> anyhow::Result<()>;

    async fn create_colo_group(
        &self,
        colo_group_id: ColoGroupId,
        initial_splits: Vec<Bound<Vec<u8>>>,
    ) -> anyhow::Result<()>;
    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()>;

    async fn latest_snapshot(&self) -> anyhow::Result<Timestamp>;
    async fn wait_for_newer(&self, ts: Timestamp) -> anyhow::Result<()>;
    async fn scan_page(
        &self,
        ts: Timestamp,
        range: Range<Vec<u8>>,
    ) -> anyhow::Result<(Vec<Record>, Option<Range<Vec<u8>>>)>;
    async fn sync(&self, ts: Timestamp) -> anyhow::Result<(Vec<Revision>, Timestamp)>;

    async fn tablet_ids(&self, ts: Timestamp) -> anyhow::Result<Vec<TabletId>>;

    async fn write(
        &self,
        snapshot_ts: Timestamp,
        muts: HashMap<MetaKey, Mutation>,
    ) -> anyhow::Result<Timestamp>;
}
