use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use futures::Stream;
use rand::seq::SliceRandom;

use crate::meta::MetaKey;
use crate::meta::MetaMutation;
use crate::meta::MetaReader;
use crate::meta::MetaState;
use crate::meta::MetaSync;
use crate::meta::MetaValue;
use crate::meta::ShardMetadata;
use crate::meta::TabletMetadata;
use crate::meta::TabletState;
use crate::runtime;
use crate::runtime::Meta as _;
use crate::runtime::Tablet;
use crate::util::sleep_for_retry;
use crate::util::WaitableTimestamp;
use crate::Bound;
use crate::ColoGroupId;
use crate::Direction;
use crate::HistoryRange;
use crate::InternalError;
use crate::Key;
use crate::KeyspaceId;
use crate::Mutation;
use crate::NodeId;
use crate::Precondition;
use crate::Range;
use crate::Record;
use crate::Revision;
use crate::RevisionValue;
use crate::ShardId;
use crate::TabletId;
use crate::Timestamp;

pub(crate) struct Meta {
    tablet: Arc<dyn Tablet>,
    sync_key: Vec<u8>,
    ts: WaitableTimestamp,
}

#[async_trait]
impl runtime::Meta for Meta {
    async fn add_shard(&self, shard_id: ShardId) -> anyhow::Result<()> {
        self.transact(&async move |tx| {
            if tx.shard_exists(shard_id).await? {
                return Err(anyhow!("{:?} already exists", shard_id).into());
            }

            tx.put(
                MetaKey::Shard(shard_id),
                MetaValue::ShardMetadata(ShardMetadata {
                    assigned_node_ids: HashSet::new(),
                }),
            );

            Ok(())
        })
        .await
        .map_err(|e| e.into())
    }

    async fn add_node(&self, node_id: NodeId) -> anyhow::Result<()> {
        self.transact(&async move |tx| {
            if tx.node_exists(node_id).await? {
                return Err(anyhow!("{:?} already exists", node_id).into());
            }

            tx.put(MetaKey::Node(node_id), MetaValue::Empty);

            Ok(())
        })
        .await
        .map_err(|e| e.into())
    }

    async fn create_colo_group(
        &self,
        colo_group_id: ColoGroupId,
        initial_splits: Vec<Bound<Vec<u8>>>,
    ) -> anyhow::Result<()> {
        let ranges = ranges_from_splits(initial_splits)?;

        self.transact(&async move |tx| {
            if tx.colo_group_exists(colo_group_id).await? {
                return Err(anyhow!("{:?} already exists", colo_group_id).into());
            }

            let mut shard_ids: Vec<_> = tx.shard_ids().await?;
            shard_ids.shuffle(&mut rand::thread_rng());

            tx.put(MetaKey::ColoGroup(colo_group_id), MetaValue::Empty);

            let mut next_tablet_id_by_shard = BTreeMap::new();
            for shard_id in &shard_ids {
                next_tablet_id_by_shard.insert(*shard_id, tx.next_tablet_id(*shard_id).await?.1);
            }

            // Round-robin the created ranges among the shards.
            for (i, range) in ranges.iter().enumerate() {
                let shard_id = shard_ids[i % shard_ids.len()];
                let tablet_id = TabletId(
                    shard_id,
                    *next_tablet_id_by_shard.get(&shard_id).unwrap_or(&1),
                );
                next_tablet_id_by_shard.insert(shard_id, tablet_id.1 + 1);

                tx.put(
                    MetaKey::Tablet(tablet_id),
                    MetaValue::TabletMetadata(TabletMetadata {
                        colo_group_id,
                        range: range.clone(),
                        state: MetaState::Stable(TabletState::Active),
                        transfer_id: None,
                    }),
                );
            }

            Ok(())
        })
        .await
        .map_err(|e| anyhow::Error::from(e))?;

        log::info!("create_colo_group({:?})", colo_group_id);

        Ok(())
    }

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        self.transact(&async move |tx| {
            if !tx.colo_group_exists(keyspace_id.0).await? {
                return Err(anyhow!("{:?} does not exist", keyspace_id.0).into());
            }

            let keyspace_key = MetaKey::Keyspace(keyspace_id);

            if tx.exists(&keyspace_key).await? {
                return Err(anyhow!("{:?} already exists", keyspace_id).into());
            }

            tx.put(keyspace_key, MetaValue::Empty);
            Ok(())
        })
        .await
        .map_err(|e| anyhow::Error::from(e))
    }

    async fn latest_snapshot(&self) -> anyhow::Result<Timestamp> {
        let ts = self
            .tablet
            .latest_snapshot(BTreeSet::from([(KeyspaceId::META, self.sync_key.clone())]))
            .await?;
        Ok(ts)
    }

    async fn wait_for_newer(&self, ts: Timestamp) -> anyhow::Result<()> {
        log::debug!("Meta::wait_for_newer({:?})", ts);
        self.ts.wait(ts.plus_one()).await;
        log::debug!("Meta::wait_for_newer({:?}) -> done", ts);
        Ok(())
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        range: Range<Vec<u8>>,
    ) -> anyhow::Result<(Vec<Record>, Option<Range<Vec<u8>>>)> {
        let (page, continue_cursor) = self
            .tablet
            .scan_page(ts, KeyspaceId::META, range.borrow(), Direction::Asc, 1000)
            .await?;
        Ok((page, continue_cursor))
    }

    async fn sync(&self, ts: Timestamp) -> anyhow::Result<(Vec<Revision>, Timestamp)> {
        let (page, _) = self
            .tablet
            .history_page(
                (KeyspaceId::META, self.sync_key.clone()),
                HistoryRange::Since(ts),
                Direction::Asc,
                100,
            )
            .await?;

        let mut out_page = vec![];
        let mut new_ts = ts;
        for revision in page {
            if let RevisionValue::Regular(value) = revision.value {
                let meta_tx = match MetaValue::decode(&value)? {
                    MetaValue::MetaSync(meta_tx) => meta_tx,
                    other => return Err(anyhow!("unexpected MetaValue {:?}", other)),
                };

                for meta_key in meta_tx.keys {
                    let key = (KeyspaceId::META, meta_key.encode());

                    let rev_value = match self.tablet.get(revision.ts, &key).await? {
                        Some(record) => RevisionValue::Regular(record.value),
                        None => RevisionValue::Tombstone,
                    };
                    let revision = Revision {
                        key: key,
                        ts: revision.ts,
                        value: rev_value,
                    };
                    out_page.push(revision);
                }
            }
            new_ts = revision.ts;

            if out_page.len() > 1000 {
                break;
            }
        }

        Ok((out_page, new_ts))
    }

    async fn tablet_ids(&self, ts: Timestamp) -> anyhow::Result<Vec<TabletId>> {
        let snapshot = self.snapshot_at(ts);
        snapshot.tablet_ids().await
    }

    /// Writes the given mutations if `Meta` has not changed since the given snapshot.
    ///
    /// Also includes a write to MetaKey::Sync.
    async fn write(
        &self,
        snapshot_ts: Timestamp,
        mut muts: HashMap<MetaKey, MetaMutation>,
    ) -> Result<Timestamp, InternalError> {
        if muts.contains_key(&MetaKey::Sync) {
            return Err(anyhow!("write contains a mutation to sync_key already").into());
        }

        log::trace!(
            "attempting meta write on snapshot {:?}: {:?}",
            snapshot_ts,
            muts
        );

        let preconds = vec![Precondition::NotChangedSince(
            KeyspaceId::META,
            self.sync_key.clone(),
            snapshot_ts,
        )];

        let changed_keys = muts.keys().cloned().collect::<HashSet<_>>();

        muts.insert(
            MetaKey::Sync,
            MetaMutation::Put(MetaValue::MetaSync(MetaSync { keys: changed_keys })),
        );

        let raw_muts = muts
            .into_iter()
            .map(|(meta_key, meta_mutation)| {
                (
                    (KeyspaceId::META, meta_key.encode()),
                    meta_mutation.into_mutation(),
                )
            })
            .collect::<BTreeMap<Key, Mutation>>();

        let ts = self.tablet.write(preconds, raw_muts).await?;
        // TODO: Periodically poll in case we have a success-but-error above.
        _ = self.ts.set(ts);
        Ok(ts)
    }
}

impl Meta {
    pub(crate) fn new(tablet: Arc<dyn Tablet>) -> Self {
        Self {
            tablet,
            sync_key: MetaKey::Sync.encode(),
            ts: WaitableTimestamp::new(Timestamp::ZERO),
        }
    }

    pub(crate) async fn latest_snapshot_(&self) -> anyhow::Result<MetaSnapshot<'_>> {
        let ts = self.latest_snapshot().await?;

        Ok(MetaSnapshot {
            tablet: self.tablet.deref(),
            ts,
        })
    }

    fn snapshot_at(&self, ts: Timestamp) -> MetaSnapshot<'_> {
        MetaSnapshot {
            tablet: self.tablet.deref(),
            ts,
        }
    }

    async fn transact<F, T>(&self, f: &F) -> Result<T, InternalError>
    where
        F: AsyncFn(&mut MetaTx2<'_>) -> Result<T, InternalError>,
    {
        for i in 0..10 {
            let mut tx = MetaTx2 {
                snapshot: self.latest_snapshot_().await?,
                muts: HashMap::new(),
            };

            let out = f(&mut tx).await?;

            match self.write(tx.snapshot.ts, tx.muts).await {
                Ok(_) => return Ok(out),
                Err(InternalError::PreconditionFailed) => {
                    sleep_for_retry(i, Duration::from_millis(50), Duration::from_millis(5000))
                        .await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        return Err(InternalError::PreconditionFailed);
    }
}

// TODO: Rename MetaTx out of the way, since it's a better name for this.
struct MetaTx2<'a> {
    snapshot: MetaSnapshot<'a>,
    muts: HashMap<MetaKey, MetaMutation>,
}

impl<'a> MetaTx2<'a> {
    fn put(&mut self, key: MetaKey, value: MetaValue) {
        self.muts.insert(key, MetaMutation::Put(value));
    }

    fn delete(&mut self, key: MetaKey) {
        self.muts.insert(key, MetaMutation::Delete);
    }
}

#[async_trait]
impl<'a> MetaReader for MetaTx2<'a> {
    async fn get_raw(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        self.snapshot.get_raw(key).await
    }

    fn scan_raw(
        &self,
        range: Range<Vec<u8>>,
        direction: Direction,
    ) -> Box<dyn Stream<Item = anyhow::Result<(Vec<u8>, Vec<u8>)>> + Unpin + Send + '_> {
        self.snapshot.scan_raw(range, direction)
    }
}

pub(crate) struct MetaSnapshot<'a> {
    tablet: &'a dyn Tablet,
    ts: Timestamp,
}

impl<'a> MetaSnapshot<'a> {
    pub(crate) fn ts(&self) -> Timestamp {
        self.ts
    }
}

#[async_trait]
impl<'a> MetaReader for MetaSnapshot<'a> {
    async fn get_raw(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self
            .tablet
            .get(self.ts, &(KeyspaceId::META, key.to_vec()))
            .await?
            .map(|record| record.value))
    }

    fn scan_raw(
        &self,
        range: Range<Vec<u8>>,
        direction: Direction,
    ) -> Box<dyn Stream<Item = anyhow::Result<(Vec<u8>, Vec<u8>)>> + Unpin + Send + '_> {
        Box::new(Box::pin(try_stream! {
            let mut maybe_cursor = Some(range);
            while let Some(cursor) = maybe_cursor {
                let (page, continue_cursor) = self.tablet.scan_page(
                    self.ts,
                    KeyspaceId::META,
                    cursor.borrow(),
                    direction,
                    1000, // page_size
                ).await?;

                for record in page {
                    yield (record.key.1, record.value);
                }

                maybe_cursor = continue_cursor;
            }
        }))
    }
}
fn ranges_from_splits(splits: Vec<Bound<Vec<u8>>>) -> anyhow::Result<Vec<Range<Vec<u8>>>> {
    if splits.is_empty() {
        return Ok(vec![Range::all()]);
    }

    if !splits.is_sorted() {
        return Err(anyhow!("initial splits must be sorted and unique"));
    }
    for i in 0..splits.len() - 1 {
        if splits[i] == splits[i + 1] {
            return Err(anyhow!("initial splits must be sorted and unique"));
        }
    }
    if splits[0] == Bound::BeforeAll {
        return Err(anyhow!(
            "cannot split at Bound::BeforeAll because there are no keys before it"
        ));
    }
    if splits[splits.len() - 1] == Bound::AfterAll {
        return Err(anyhow!(
            "cannot split at Bound::AfterAll because there are no keys after it"
        ));
    }

    let mut out = Vec::with_capacity(splits.len() - 1);
    let mut prev = Bound::BeforeAll;
    for split in splits {
        out.push(Range {
            lower: prev,
            upper: split.clone(),
        });
        prev = split;
    }
    out.push(Range {
        lower: prev,
        upper: Bound::AfterAll,
    });

    Ok(out)
}

#[async_trait]
impl<T: runtime::Meta + ?Sized> runtime::Meta for Box<T> {
    async fn add_shard(&self, shard_id: ShardId) -> anyhow::Result<()> {
        T::add_shard(self, shard_id).await
    }

    async fn add_node(&self, node_id: NodeId) -> anyhow::Result<()> {
        T::add_node(self, node_id).await
    }

    async fn create_colo_group(
        &self,
        colo_group_id: ColoGroupId,
        initial_splits: Vec<Bound<Vec<u8>>>,
    ) -> anyhow::Result<()> {
        T::create_colo_group(self, colo_group_id, initial_splits).await
    }

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        T::create_keyspace(self, keyspace_id).await
    }

    async fn latest_snapshot(&self) -> anyhow::Result<Timestamp> {
        T::latest_snapshot(self).await
    }

    async fn wait_for_newer(&self, ts: Timestamp) -> anyhow::Result<()> {
        T::wait_for_newer(self, ts).await
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        range: Range<Vec<u8>>,
    ) -> anyhow::Result<(Vec<Record>, Option<Range<Vec<u8>>>)> {
        T::scan_page(self, ts, range).await
    }

    async fn sync(&self, ts: Timestamp) -> anyhow::Result<(Vec<Revision>, Timestamp)> {
        T::sync(self, ts).await
    }

    async fn tablet_ids(&self, ts: Timestamp) -> anyhow::Result<Vec<TabletId>> {
        T::tablet_ids(self, ts).await
    }

    async fn write(
        &self,
        snapshot_ts: Timestamp,
        muts: HashMap<MetaKey, MetaMutation>,
    ) -> Result<Timestamp, InternalError> {
        T::write(self, snapshot_ts, muts).await
    }
}

#[async_trait]
impl<T: runtime::Meta + ?Sized> runtime::Meta for Arc<T> {
    async fn add_shard(&self, shard_id: ShardId) -> anyhow::Result<()> {
        T::add_shard(self, shard_id).await
    }

    async fn add_node(&self, node_id: NodeId) -> anyhow::Result<()> {
        T::add_node(self, node_id).await
    }

    async fn create_colo_group(
        &self,
        colo_group_id: ColoGroupId,
        initial_splits: Vec<Bound<Vec<u8>>>,
    ) -> anyhow::Result<()> {
        T::create_colo_group(self, colo_group_id, initial_splits).await
    }

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        T::create_keyspace(self, keyspace_id).await
    }

    async fn latest_snapshot(&self) -> anyhow::Result<Timestamp> {
        T::latest_snapshot(self).await
    }

    async fn wait_for_newer(&self, ts: Timestamp) -> anyhow::Result<()> {
        T::wait_for_newer(self, ts).await
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        range: Range<Vec<u8>>,
    ) -> anyhow::Result<(Vec<Record>, Option<Range<Vec<u8>>>)> {
        T::scan_page(self, ts, range).await
    }

    async fn sync(&self, ts: Timestamp) -> anyhow::Result<(Vec<Revision>, Timestamp)> {
        T::sync(self, ts).await
    }

    async fn tablet_ids(&self, ts: Timestamp) -> anyhow::Result<Vec<TabletId>> {
        T::tablet_ids(self, ts).await
    }

    async fn write(
        &self,
        snapshot_ts: Timestamp,
        muts: HashMap<MetaKey, MetaMutation>,
    ) -> Result<Timestamp, InternalError> {
        T::write(self, snapshot_ts, muts).await
    }
}
