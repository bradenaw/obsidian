use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::convert::TryFrom;

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use futures::Stream;
use futures::TryStreamExt;
use prost::Message;
use rand::seq::SliceRandom;

use crate::obsidian::Shards;
use crate::obsidian::TabletId;
use crate::pb;
use crate::range::Bound;
use crate::range::Range;
use crate::tablet::Tablet;
use crate::tuple_encoding::tuple_decode;
use crate::tuple_encoding::tuple_decode_prefix;
use crate::tuple_encoding::tuple_encode;
use crate::types::ColoGroupId;
use crate::types::Direction;
use crate::types::HistoryRange;
use crate::types::Key;
use crate::types::KeyspaceId;
use crate::types::Mutation;
use crate::types::Precondition;
use crate::types::Record;
use crate::types::Revision;
use crate::types::RevisionValue;
use crate::types::ShardId;
use crate::types::Timestamp;
use crate::util::hexlify;
use crate::util::WaitableTimestamp;

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) enum MetaKey {
    Sync,
    ColoGroup(ColoGroupId),
    Keyspace(KeyspaceId),
    Tablet(TabletId),
}

impl MetaKey {
    // (PFX_SYNC) -> pb::internal::MetaTx
    const PFX_SYNC: u64 = 1;

    // (PFX_COLO_GROUPS, colo_group_id) -> []
    const PFX_COLO_GROUPS: u64 = 2;

    // (PFX_KEYSPACES, keyspace_id) -> []
    const PFX_KEYSPACES: u64 = 3;

    // (PFX_TABLETS, tablet_id) -> pb::internal::TabletMetadata
    const PFX_TABLETS: u64 = 4;

    pub(crate) fn encode(&self) -> Vec<u8> {
        match self {
            Self::Sync => tuple_encode(&(Self::PFX_SYNC,)),
            Self::ColoGroup(colo_group_id) => {
                tuple_encode(&(Self::PFX_COLO_GROUPS, colo_group_id.0 as u64))
            }
            Self::Keyspace(keyspace_id) => tuple_encode(&(
                Self::PFX_KEYSPACES,
                keyspace_id.0 .0 as u64,
                keyspace_id.1 as u64,
            )),
            Self::Tablet(tablet_id) => {
                tuple_encode(&(Self::PFX_TABLETS, tablet_id.0 .0 as u64, tablet_id.1))
            }
        }
    }

    pub(crate) fn decode(b: &[u8]) -> anyhow::Result<Self> {
        let prefix = tuple_decode_prefix::<(u64,)>(b)?.0;
        match prefix {
            Self::PFX_SYNC => Ok(Self::Sync),
            Self::PFX_COLO_GROUPS => {
                let (_, colo_group_id_raw): (u64, u64) = tuple_decode(b)?;
                Ok(Self::ColoGroup(ColoGroupId(u32::try_from(
                    colo_group_id_raw,
                )?)))
            }
            Self::PFX_KEYSPACES => {
                let (_, colo_group_id_raw, keyspace_id_raw): (u64, u64, u64) = tuple_decode(b)?;
                Ok(Self::Keyspace(KeyspaceId(
                    ColoGroupId(u32::try_from(colo_group_id_raw)?),
                    u32::try_from(keyspace_id_raw)?,
                )))
            }
            Self::PFX_TABLETS => {
                let (_, shard_id_raw, tablet_id_raw): (u64, u64, u64) = tuple_decode(b)?;
                Ok(Self::Tablet(TabletId(
                    ShardId(u32::try_from(shard_id_raw)?),
                    tablet_id_raw,
                )))
            }
            _ => Err(anyhow!("unrecognized MetaKey prefix {}", prefix)),
        }
    }

    fn colo_groups() -> Range<Vec<u8>> {
        Range::prefix(tuple_encode(&(Self::PFX_COLO_GROUPS,)))
    }

    fn keyspaces() -> Range<Vec<u8>> {
        Range::prefix(tuple_encode(&(Self::PFX_KEYSPACES,)))
    }

    fn tablets() -> Range<Vec<u8>> {
        Range::prefix(tuple_encode(&(Self::PFX_TABLETS,)))
    }
}

#[async_trait]
pub(crate) trait Meta {
    async fn create_colo_group(
        &self,
        colo_group_id: ColoGroupId,
        initial_splits: Vec<Bound<Vec<u8>>>,
    ) -> anyhow::Result<()>;
    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()>;

    async fn latest_snapshot(&self) -> anyhow::Result<Timestamp>;
    async fn wait_for_newer(&self, ts: Timestamp) -> anyhow::Result<()>;
    async fn scan_page(
        &self,
        ts: Timestamp,
        range: Range<Vec<u8>>,
    ) -> anyhow::Result<(Vec<Record>, Option<Range<Vec<u8>>>)>;
    async fn sync(&self, ts: Timestamp) -> anyhow::Result<(Vec<Revision>, Timestamp)>;

    async fn tablet_ids(&self, ts: Timestamp) -> anyhow::Result<Vec<TabletId>>;
}

pub(crate) struct MetaImpl<T> {
    tablet: T,
    sync_key: Vec<u8>,
    shards: Box<dyn Shards + Send + Sync>,
    ts: WaitableTimestamp,
}

#[async_trait]
impl<T: Tablet + Sync + Send> Meta for MetaImpl<T> {
    async fn create_colo_group(
        &self,
        colo_group_id: ColoGroupId,
        initial_splits: Vec<Bound<Vec<u8>>>,
    ) -> anyhow::Result<()> {
        let ranges = ranges_from_splits(initial_splits)?;

        let snapshot = self.latest_snapshot_().await?;

        if snapshot.colo_group_exists(colo_group_id).await? {
            return Err(anyhow!("{:?} already exists", colo_group_id));
        }

        let mut shard_ids: Vec<_> = self
            .shards
            .shards()
            .iter()
            .map(|shard| shard.id())
            .collect();
        shard_ids.shuffle(&mut rand::thread_rng());

        let mut muts = HashMap::from([(MetaKey::ColoGroup(colo_group_id), Mutation::Put(vec![]))]);

        // Round-robin the created ranges among the shards.
        for (i, range) in ranges.into_iter().enumerate() {
            let shard_i = i % shard_ids.len();
            let tablet_id = self
                .shards
                .shard(shard_ids[shard_i])?
                .create_tablet(colo_group_id, range.clone())
                .await?;

            muts.insert(
                MetaKey::Tablet(tablet_id),
                Mutation::Put(
                    pb::internal::TabletMetadata {
                        colo_group_id: colo_group_id.0,
                        range: Some(range.into()),
                    }
                    .encode_to_vec(),
                ),
            );
        }

        let write_ts = self.write_syncable(snapshot, muts).await?;

        log::info!("create_colo_group({:?}) -> {:?}", colo_group_id, write_ts);

        Ok(())
    }

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        let snapshot = self.latest_snapshot_().await?;

        if !snapshot.colo_group_exists(keyspace_id.0).await? {
            return Err(anyhow!("{:?} does not exist", keyspace_id.0));
        }

        let keyspace_key = MetaKey::Keyspace(keyspace_id);

        if snapshot.exists(&keyspace_key).await? {
            return Err(anyhow!("{:?} already exists", keyspace_id));
        }

        self.write_syncable(
            snapshot,
            HashMap::from([(keyspace_key, Mutation::Put(vec![]))]),
        )
        .await?;

        Ok(())
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
        self.ts.wait(ts.plus_one()).await?;
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
                let proto_tx = pb::internal::MetaTx::decode(&value[..])?;
                let keys = BTreeSet::try_from(
                    proto_tx
                        .keys
                        .ok_or_else(|| anyhow!("ProtoTx with no keys"))?,
                )?;

                for key in keys {
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
}

impl<T: Tablet + Sync + Send> MetaImpl<T> {
    pub(crate) fn new(shards: Box<dyn Shards + Send + Sync>, tablet: T) -> Self {
        Self {
            shards,
            tablet,
            sync_key: MetaKey::Sync.encode(),
            ts: WaitableTimestamp::new(),
        }
    }

    async fn latest_snapshot_(&self) -> anyhow::Result<MetaSnapshot<'_, T>> {
        let ts = self.latest_snapshot().await?;

        Ok(MetaSnapshot {
            tablet: &self.tablet,
            ts,
        })
    }

    fn snapshot_at(&self, ts: Timestamp) -> MetaSnapshot<'_, T> {
        MetaSnapshot {
            tablet: &self.tablet,
            ts,
        }
    }

    /// Writes the given mutations if `Meta` has not changed since the given snapshot.
    async fn write_syncable<'a>(
        &'a self,
        snapshot: MetaSnapshot<'a, T>,
        muts: HashMap<MetaKey, Mutation>,
    ) -> anyhow::Result<Timestamp> {
        if muts.contains_key(&MetaKey::Sync) {
            return Err(anyhow!(
                "write_syncable contains a mutation to sync_key already"
            ));
        }

        let preconds = vec![Precondition::NotChangedSince(
            KeyspaceId::META,
            self.sync_key.clone(),
            snapshot.ts,
        )];

        let mut raw_muts = muts
            .into_iter()
            .map(|(meta_key, mutation)| ((KeyspaceId::META, meta_key.encode()), mutation))
            .collect::<BTreeMap<Key, Mutation>>();

        raw_muts.insert(
            (KeyspaceId::META, MetaKey::Sync.encode()),
            Mutation::Put(
                pb::internal::MetaTx {
                    keys: Some(pb::internal::CompressedKeySet::from(
                        raw_muts.keys().cloned().collect::<BTreeSet<_>>(),
                    )),
                }
                .encode_to_vec(),
            ),
        );

        let ts = self.tablet.write(preconds, raw_muts).await?;
        // TODO: Periodically poll in case we have a success-but-error above.
        _ = self.ts.set(ts);
        Ok(ts)
    }
}

#[async_trait]
pub(crate) trait MetaReader {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>>;

    fn scan(
        &self,
        range: Range<Vec<u8>>,
        direction: Direction,
    ) -> Box<dyn Stream<Item = anyhow::Result<(Vec<u8>, Vec<u8>)>> + Unpin + Send + '_>;

    async fn get_meta_key<V: prost::Message + Default>(
        &self,
        meta_key: &MetaKey,
    ) -> anyhow::Result<Option<V>> {
        if let Some(value) = self.get(&meta_key.encode()).await? {
            return Ok(Some(V::decode(&value[..])?));
        }
        Ok(None)
    }

    async fn exists(&self, meta_key: &MetaKey) -> anyhow::Result<bool> {
        Ok(self.get(&meta_key.encode()).await?.is_some())
    }

    async fn colo_group_exists(&self, colo_group_id: ColoGroupId) -> anyhow::Result<bool> {
        self.exists(&MetaKey::ColoGroup(colo_group_id)).await
    }

    async fn tablet_ids(&self) -> anyhow::Result<Vec<TabletId>> {
        let mut out = vec![];
        let mut s = self.scan(MetaKey::tablets(), Direction::Asc);
        while let Some((key, _)) = s.try_next().await? {
            if let MetaKey::Tablet(tablet_id) = MetaKey::decode(&key[..])? {
                out.push(tablet_id);
            } else {
                return Err(anyhow!("invalid tablet key {}", hexlify(&key)));
            }
        }
        Ok(out)
    }

    async fn keyspace_ids(&self) -> anyhow::Result<Vec<KeyspaceId>> {
        let mut out = vec![];
        let mut s = self.scan(MetaKey::keyspaces(), Direction::Asc);
        while let Some((key, _)) = s.try_next().await? {
            if let MetaKey::Keyspace(keyspace_id) = MetaKey::decode(&key[..])? {
                out.push(keyspace_id);
            } else {
                return Err(anyhow!("invalid tablet key {}", hexlify(&key)));
            }
        }
        Ok(out)
    }

    async fn tablet_metadata(
        &self,
        tablet_id: TabletId,
    ) -> anyhow::Result<pb::internal::TabletMetadata> {
        self.get_meta_key(&MetaKey::Tablet(tablet_id))
            .await?
            .ok_or_else(|| anyhow!("{:?} not found", tablet_id))
    }
}

struct MetaSnapshot<'a, T> {
    tablet: &'a T,
    ts: Timestamp,
}

#[async_trait]
impl<'a, T: Tablet + Sync> MetaReader for MetaSnapshot<'a, T> {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self
            .tablet
            .get(self.ts, &(KeyspaceId::META, key.to_vec()))
            .await?
            .map(|record| record.value))
    }

    fn scan(
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
impl<T: Meta + Sync + Send + ?Sized> Meta for Box<T> {
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
}
