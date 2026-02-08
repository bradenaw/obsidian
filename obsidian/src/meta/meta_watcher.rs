use async_trait::async_trait;

use crate::meta::MetaSyncedSnapshot;
use crate::meta::SyncType;

#[async_trait]
pub(crate) trait MetaWatcher {
    async fn sync_meta(
        &self,
        sync_type: SyncType,
        snapshot: MetaSyncedSnapshot,
    );
}
