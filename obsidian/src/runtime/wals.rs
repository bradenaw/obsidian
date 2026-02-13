use std::sync::Arc;

use async_trait::async_trait;

use crate::runtime::Wal;
use crate::TabletId;

#[async_trait]
pub(crate) trait Wals: Send + Sync + 'static {
    async fn wal(&self, tablet_id: TabletId) -> anyhow::Result<Arc<dyn Wal>>;
}
