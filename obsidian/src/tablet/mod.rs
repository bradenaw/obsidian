mod data_tablet;
mod lock_mgr;
mod meta_tablet;
mod protected;
mod sequencer;
mod shard_meta_tablet;
mod tablet_inner;

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use async_trait::async_trait;

use crate::lsm::Manifest;
use crate::obsidian::InternalError;
use crate::obsidian::TxOutcome;
use crate::obsidian::Txid;
use crate::range::Bound;
use crate::range::Range;
use crate::types::Direction;
use crate::types::HistoryRange;
use crate::types::Key;
use crate::types::KeyspaceId;
use crate::types::Mutation;
use crate::types::Precondition;
use crate::types::Record;
use crate::types::Revision;
use crate::types::Timestamp;

#[async_trait]
pub(crate) trait Tablet: Send + Sync {
    async fn get(&self, ts: Timestamp, key: &Key) -> Result<Option<Record>, InternalError>;

    async fn get_latest(&self, key: Key) -> Result<(Timestamp, Option<Record>), InternalError>;

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> Result<Timestamp, InternalError>;

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<Range<Vec<u8>>>), InternalError>;

    async fn history_page(
        &self,
        key: Key,
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Revision>, Option<HistoryRange>), InternalError>;

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError>;

    async fn prepare(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError>;

    async fn try_commit(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<TxOutcome>;
    async fn try_abort(&self, txid: Txid) -> anyhow::Result<TxOutcome>;
    async fn wait(&self, txid: Txid) -> Result<TxOutcome, InternalError>;
    async fn cleanup_committed(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()>;

    async fn wait_meta_sync(&self, ts: Timestamp) -> anyhow::Result<()>;

    async fn manifest(&self) -> anyhow::Result<Manifest>;

    async fn wait_mostly_hydrated(&self) -> anyhow::Result<()>;

    async fn catchup(&self) -> anyhow::Result<()>;

    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>>;
}

#[allow(unused_imports)]
pub(crate) use data_tablet::DataTablet;
#[allow(unused_imports)]
pub(crate) use meta_tablet::MetaTablet;
#[allow(unused_imports)]
pub(crate) use shard_meta_tablet::ShardMetaTablet;
