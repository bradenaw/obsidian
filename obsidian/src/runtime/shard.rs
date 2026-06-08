use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use obsidian_common::RunId;

use crate::runtime::Tablet;
use crate::InternalError;
use crate::Key;
use crate::ShardId;
use crate::TabletId;
use crate::Timestamp;
use crate::TxOutcome;
use crate::Txid;

#[async_trait]
pub(crate) trait Shard: Send + Sync {
    fn id(&self) -> ShardId;

    fn tablet(&self, tablet_id: TabletId) -> anyhow::Result<Arc<dyn Tablet>>;

    async fn wait_meta_sync(&self, ts: Timestamp) -> anyhow::Result<()>;

    async fn tx_try_commit(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<TxOutcome>;

    async fn tx_try_abort(&self, txid: Txid) -> anyhow::Result<TxOutcome>;

    async fn tx_wait(&self, txid: Txid) -> Result<TxOutcome, InternalError>;

    /// All of the runs that this shard is using. Includes the runs present in the current manifest
    /// of each tablet, along with runs that are in the midst of being created by compaction, and
    /// runs that are not currently used but are still reachable in the journal.
    async fn live_runs(&self) -> anyhow::Result<BTreeSet<RunId>>;
}
