use async_trait::async_trait;

use crate::TabletId;

#[async_trait]
pub(crate) trait Wals<W>: Send + Sync + 'static {
    async fn wal(&self, tablet_id: TabletId) -> anyhow::Result<W>;
}
