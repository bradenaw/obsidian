use std::collections::BTreeMap;
use std::collections::BTreeSet;

use async_stream::try_stream;
use async_trait::async_trait;
use futures::Stream;

use crate::Bound;
use crate::ColoGroupId;
use crate::Direction;
use crate::Key;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Precondition;
use crate::Range;
use crate::Record;
use crate::Timestamp;
use crate::WriteError;

#[async_trait]
pub trait Obsidian: Send + Sync {
    async fn get_multi(
        &self,
        ts: Timestamp,
        keys: BTreeSet<Key>,
    ) -> anyhow::Result<BTreeMap<Key, Record>>;

    async fn get(&self, ts: Timestamp, key: &Key) -> anyhow::Result<Option<Record>> {
        Ok(self
            .get_multi(ts, BTreeSet::from([key.clone()]))
            .await?
            .remove(key))
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<Record>, Option<Range<Vec<u8>>>)>;

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> anyhow::Result<Timestamp>;

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, WriteError>;

    async fn create_colo_group(
        &self,
        colo_group_id: ColoGroupId,
        initial_splits: Vec<Bound<Vec<u8>>>,
    ) -> anyhow::Result<()>;

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()>;
}

pub trait ObsidianExt {
    fn scan(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<Vec<u8>>,
        direction: Direction,
    ) -> Box<dyn Stream<Item = anyhow::Result<Record>> + Send + '_>;
}

impl<T> ObsidianExt for T
where
    T: Obsidian + ?Sized,
{
    // TODO: This needs to give access to the underlying cursor in case it gets interrupted between
    // results (e.g. timing out between yielding two results).
    fn scan(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<Vec<u8>>,
        direction: Direction,
    ) -> Box<dyn Stream<Item = anyhow::Result<Record>> + Send + '_> {
        Box::new(try_stream! {
            let mut maybe_cursor = Some(range);
            while let Some(cursor) = maybe_cursor {
                let (page, continue_cursor) = self.scan_page(
                    ts,
                    keyspace_id,
                    cursor.borrow(),
                    direction,
                    1000, // page_size
                ).await?;

                for record in page {
                    yield record;
                }

                maybe_cursor = continue_cursor;
            }
        })
    }
}
