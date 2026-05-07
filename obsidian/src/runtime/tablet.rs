use std::collections::BTreeMap;
use std::collections::BTreeSet;

use async_trait::async_trait;

use crate::Manifest;
use crate::Bound;
use crate::Direction;
use crate::HistoryRange;
use crate::InternalError;
use crate::Key;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Precondition;
use crate::Range;
use crate::Record;
use crate::Revision;
use crate::Timestamp;
use crate::Txid;

#[async_trait]
pub(crate) trait Tablet: Send + Sync {
    async fn get_multi(
        &self,
        ts: Timestamp,
        keys: BTreeSet<Key>,
    ) -> Result<BTreeMap<Key, Record>, InternalError>;

    async fn get(&self, ts: Timestamp, key: &Key) -> Result<Option<Record>, InternalError> {
        Ok(self
            .get_multi(ts, BTreeSet::from([key.clone()]))
            .await?
            .remove(key))
    }

    async fn get_latest_multi(
        &self,
        keys: BTreeSet<Key>,
    ) -> Result<(Timestamp, BTreeMap<Key, Record>), InternalError>;

    async fn get_latest(&self, key: Key) -> Result<(Timestamp, Option<Record>), InternalError> {
        let (ts, mut records) = self.get_latest_multi(BTreeSet::from([key.clone()])).await?;
        Ok((ts, records.remove(&key)))
    }

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

    async fn cleanup_committed(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()>;

    async fn manifest(&self) -> anyhow::Result<Manifest>;

    async fn wait_mostly_hydrated(&self) -> anyhow::Result<()>;

    async fn catchup(&self) -> anyhow::Result<()>;

    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>>;
}
