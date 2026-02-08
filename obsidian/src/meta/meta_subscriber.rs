use async_trait::async_trait;

use crate::meta::MetaSyncedSnapshot;
use crate::meta::SyncType;

/// A MetaSubscriber watches changes to meta via MetaSynced::subscribe.
#[async_trait]
pub(crate) trait MetaSubscriber {
    /// `sync_meta` is called when there are changes to meta. It will be called once, either
    /// immediately or when initial sync finishes, with `SyncType::Initial`. Every transaction that
    /// updates the `MetaSynced` after that point will be given as a `SyncType::Tx` with the
    /// changed keys.
    async fn sync_meta(
        &self,
        sync_type: SyncType,
        snapshot: MetaSyncedSnapshot,
    );
}
