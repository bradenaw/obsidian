use crate::ShardId;
use crate::TabletId;
use crate::TransferId;

pub(crate) trait Supervisor {
    async fn start_move(&self, src: TabletId, dst: ShardId) -> anyhow::Result<TransferId>;

    async fn start_split(
        &self,
        src: TabletId,
        dst_a: ShardId,
        dst_b: ShardId,
    ) -> anyhow::Result<TransferId>;

    async fn start_merge(&self, srcs: Vec<TabletId>, dst: ShardId) -> anyhow::Result<TransferId>;

    async fn wait_transfer(&self, transfer_id: TransferId) -> anyhow::Result<()>;
}
