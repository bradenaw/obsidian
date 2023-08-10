use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::convert::TryFrom;

use anyhow::anyhow;
use async_trait::async_trait;
use prost::Message;

use crate::obsidian::TabletId;
use crate::pb;
use crate::range::Bound;
use crate::range::Range;
use crate::tablet::Tablet;
use crate::tuple_encoding::tuple_decode;
use crate::tuple_encoding::tuple_encode;
use crate::types::ColoGroupId;
use crate::types::Direction;
use crate::types::HistoryRange;
use crate::types::KeyspaceId;
use crate::types::Mutation;
use crate::types::Precondition;
use crate::types::Record;
use crate::types::ShardId;
use crate::types::Timestamp;
use crate::types::Value;
use crate::util::longest_shared_prefix_len;

// (PFX_SYNC) -> pb::MetaTx
const PFX_SYNC: u64 = 1;

// (PFX_COLO_GROUPS, colo_group_id) -> []
const PFX_COLO_GROUPS: u64 = 2;

// (PFX_KEYSPACES, keyspace_id) -> []
const PFX_KEYSPACES: u64 = 3;

// (PFX_TABLETS, tablet_id) -> pb::MetaTablet
pub(crate) const PFX_TABLETS: u64 = 4;

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
    ) -> anyhow::Result<(Vec<(Vec<u8>, Timestamp, Vec<u8>)>, Option<Range<Vec<u8>>>)>;
    async fn sync(&self, ts: Timestamp) -> anyhow::Result<(Vec<Record>, Timestamp)>;
}

pub(crate) struct MetaImpl<T> {
    tablet: T,
    sync_key: Vec<u8>,
    ts_send: tokio::sync::watch::Sender<Timestamp>,
    ts: tokio::sync::watch::Receiver<Timestamp>,
}

#[async_trait]
impl<T: Tablet + Sync + Send> Meta for MetaImpl<T> {
    async fn create_colo_group(
        &self,
        colo_group_id: ColoGroupId,
        initial_splits: Vec<Bound<Vec<u8>>>,
    ) -> anyhow::Result<()> {
        let ranges = ranges_from_splits(initial_splits)?;

        let ts = self.latest_snapshot().await?;

        let colo_group_key = tuple_encode(&(PFX_COLO_GROUPS, colo_group_id.0 as u64));

        if self.colo_group_exists(ts, colo_group_id).await? {
            return Err(anyhow!("{:?} already exists", colo_group_id));
        }

        let tablet_ids = self.tablet_ids(ts).await?;
        let mut tablet_keys = Vec::with_capacity(tablet_ids.len());
        let mut meta_tablets = Vec::with_capacity(tablet_ids.len());
        for tablet_id in &tablet_ids {
            let tablet_key = tuple_encode(&(PFX_TABLETS, tablet_id.0 .0 as u64, tablet_id.1));
            let meta_tablet = self
                .tablet
                .get(ts, KeyspaceId::META, tablet_key.clone())
                .await?
                .map(|v| pb::MetaTablet::decode(&v[..]))
                .unwrap_or(Ok(pb::MetaTablet {
                    colo_group_ids: vec![],
                    ranges: vec![],
                }))?;
            tablet_keys.push(tablet_key);
            meta_tablets.push(meta_tablet);
        }

        // Round-robin the created ranges among the tablets.
        for (i, range) in ranges.into_iter().enumerate() {
            let tablet_i = i % tablet_ids.len();
            meta_tablets[tablet_i].colo_group_ids.push(colo_group_id.0);
            meta_tablets[tablet_i].ranges.push(range.into());
        }

        let mut muts =
            BTreeMap::from([((KeyspaceId::META, colo_group_key), Mutation::Put(vec![]))]);

        for (tablet_key, meta_tablet) in
            Iterator::zip(tablet_keys.into_iter(), meta_tablets.into_iter())
        {
            muts.insert(
                (KeyspaceId::META, tablet_key),
                Mutation::Put(meta_tablet.encode_to_vec()),
            );
        }

        self.write_syncable(
            vec![Precondition::NotChangedSince(
                KeyspaceId::META,
                self.sync_key.clone(),
                ts,
            )],
            muts,
        )
        .await?;

        Ok(())
    }

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        let ts = self.latest_snapshot().await?;

        if !self.colo_group_exists(ts, keyspace_id.0).await? {
            return Err(anyhow!("{:?} does not exist", keyspace_id.0));
        }

        let keyspace_key =
            tuple_encode(&(PFX_KEYSPACES, keyspace_id.0 .0 as u64, keyspace_id.1 as u64));

        if self
            .tablet
            .get(ts, KeyspaceId::META, keyspace_key.clone())
            .await?
            .is_some()
        {
            return Err(anyhow!("{:?} already exists", keyspace_id));
        }

        self.write_syncable(
            vec![Precondition::NotChangedSince(
                KeyspaceId::META,
                self.sync_key.clone(),
                ts,
            )],
            BTreeMap::from([((KeyspaceId::META, keyspace_key), Mutation::Put(vec![]))]),
        )
        .await?;

        Ok(())
    }

    async fn latest_snapshot(&self) -> anyhow::Result<Timestamp> {
        let ts = self
            .tablet
            .latest_snapshot(BTreeSet::from([(KeyspaceId::META, &self.sync_key[..])]))
            .await?;
        Ok(ts)
    }

    async fn wait_for_newer(&self, ts: Timestamp) -> anyhow::Result<()> {
        let mut ts_watcher = self.ts.clone();
        loop {
            {
                let curr = ts_watcher.borrow_and_update();
                if *curr > ts {
                    return Ok(());
                }
            }
            ts_watcher.changed().await?;
        }
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        range: Range<Vec<u8>>,
    ) -> anyhow::Result<(Vec<(Vec<u8>, Timestamp, Vec<u8>)>, Option<Range<Vec<u8>>>)> {
        let (page, continue_cursor) = self
            .tablet
            .scan_page(ts, KeyspaceId::META, range.borrow(), Direction::Asc, 1000)
            .await?;
        Ok((page, continue_cursor))
    }

    async fn sync(&self, ts: Timestamp) -> anyhow::Result<(Vec<Record>, Timestamp)> {
        let (page, _) = self
            .tablet
            .history_page(
                KeyspaceId::META,
                &self.sync_key[..],
                HistoryRange::Since(ts),
                Direction::Asc,
                100,
            )
            .await?;

        let mut out_page = vec![];
        let mut new_ts = ts;
        for (ts, maybe_value) in page {
            if let Value::Regular(value) = maybe_value {
                let proto_tx = pb::MetaTx::decode(&value[..])?;
                let keys = BTreeSet::try_from(
                    proto_tx
                        .keys
                        .ok_or_else(|| anyhow!("ProtoTx with no keys"))?,
                )?;

                for (keyspace_id, key) in keys {
                    let record = Record {
                        key: key.clone(),
                        ts,
                        value: match self.tablet.get(ts, keyspace_id, key).await? {
                            Some(v) => Value::Regular(v),
                            None => Value::Tombstone,
                        },
                    };
                    out_page.push(record);
                }
            }
            new_ts = ts;

            if out_page.len() > 1000 {
                break;
            }
        }

        Ok((out_page, new_ts))
    }
}

impl<T: Tablet + Sync + Send> MetaImpl<T> {
    pub(crate) fn new(tablet: T) -> Self {
        let (ts_send, ts) = tokio::sync::watch::channel(Timestamp::ZERO);
        Self {
            tablet,
            sync_key: tuple_encode(&(PFX_SYNC,)),
            ts_send,
            ts,
        }
    }

    async fn write_syncable(
        &self,
        preconds: Vec<Precondition>,
        mut muts: BTreeMap<(KeyspaceId, Vec<u8>), Mutation>,
    ) -> anyhow::Result<Timestamp> {
        muts.insert(
            (KeyspaceId::META, vec![]),
            Mutation::Put(
                pb::MetaTx {
                    keys: Some(pb::CompressedKeySet::from(
                        muts.keys().cloned().collect::<BTreeSet<_>>(),
                    )),
                }
                .encode_to_vec(),
            ),
        );

        let ts = self.tablet.write(preconds, muts).await?;
        // TODO: Periodically poll in case we have a success-but-error above.
        _ = self.ts_send.send(ts);
        Ok(ts)
    }

    async fn tablet_ids(&self, ts: Timestamp) -> anyhow::Result<Vec<TabletId>> {
        let mut out = vec![];
        let mut maybe_cursor = Some(Range::prefix(tuple_encode(&(PFX_TABLETS,))));
        while let Some(cursor) = maybe_cursor {
            let (page, continue_cursor) = self.scan_page(ts, cursor).await?;
            for (key, _, _) in page {
                let (_, tablet_shard_id, tablet_id_seq): (u64, u64, u64) = tuple_decode(&key)?;
                let tablet_id = TabletId(ShardId(tablet_shard_id as u32), tablet_id_seq);
                out.push(tablet_id);
            }
            maybe_cursor = continue_cursor;
        }

        Ok(out)
    }

    async fn colo_group_exists(
        &self,
        ts: Timestamp,
        colo_group_id: ColoGroupId,
    ) -> anyhow::Result<bool> {
        let colo_group_key = tuple_encode(&(PFX_COLO_GROUPS, colo_group_id.0 as u64));

        Ok(self
            .tablet
            .get(ts, KeyspaceId::META, colo_group_key.clone())
            .await?
            .is_some())
    }
}

impl From<BTreeSet<(KeyspaceId, Vec<u8>)>> for pb::CompressedKeySet {
    fn from(set: BTreeSet<(KeyspaceId, Vec<u8>)>) -> Self {
        let mut keyspace_id_counts = HashMap::new();
        let mut key_to_keyspace_ids = BTreeMap::new();
        for (keyspace_id, key) in set {
            *(keyspace_id_counts.entry(keyspace_id).or_insert(0)) += 1;
            key_to_keyspace_ids
                .entry(key)
                .or_insert_with(Vec::new)
                .push(keyspace_id);
        }
        let mut keyspace_ids_by_pop = keyspace_id_counts.keys().copied().collect::<Vec<_>>();
        keyspace_ids_by_pop.sort_by_key(|keyspace_id| keyspace_id_counts.get(keyspace_id));
        let keyspace_id_to_idx = keyspace_ids_by_pop
            .iter()
            .enumerate()
            .map(|(i, keyspace_id)| (*keyspace_id, i))
            .collect::<HashMap<_, _>>();

        let mut key_fragments = vec![];
        let mut key_shared_prefixes = vec![];
        let mut maybe_prev_key = None;
        for key in key_to_keyspace_ids.keys() {
            let n_shared = match maybe_prev_key {
                Some(prev_key) => longest_shared_prefix_len(key, prev_key),
                None => 0,
            };

            key_fragments.push(key[n_shared..].to_vec());
            key_shared_prefixes.push(n_shared as u32);

            maybe_prev_key = Some(key);
        }

        let mut key_keyspaces_counts = vec![];
        let mut key_keyspaces_refs = vec![];
        if keyspace_id_to_idx.len() > 1 {
            for keyspace_ids in key_to_keyspace_ids.values() {
                let mut count = 0;
                for keyspace_id in keyspace_ids {
                    let idx = *(keyspace_id_to_idx.get(keyspace_id).unwrap());
                    count += 1;
                    key_keyspaces_refs.push(idx as u32);
                }
                key_keyspaces_counts.push(count);
            }
        }

        pb::CompressedKeySet {
            keyspace_ids: keyspace_ids_by_pop
                .iter()
                .map(|keyspace_id| pb::KeyspaceId {
                    colo_group_id: keyspace_id.0 .0,
                    id: keyspace_id.1,
                })
                .collect(),
            key_fragments,
            key_shared_prefixes,
            key_keyspaces_counts,
            key_keyspaces_refs,
        }
    }
}

impl TryFrom<pb::CompressedKeySet> for BTreeSet<(KeyspaceId, Vec<u8>)> {
    type Error = anyhow::Error;

    fn try_from(value: pb::CompressedKeySet) -> Result<Self, Self::Error> {
        let keyspace_ids = value
            .keyspace_ids
            .iter()
            .map(|keyspace_id_pb| {
                KeyspaceId(ColoGroupId(keyspace_id_pb.colo_group_id), keyspace_id_pb.id)
            })
            .collect::<Vec<_>>();

        if value.key_fragments.len() != value.key_shared_prefixes.len() {
            return Err(anyhow!(""));
        }

        let mut prev_key = vec![];
        let mut j = 0;
        let mut out = BTreeSet::new();
        for (i, key_fragment) in value.key_fragments.iter().enumerate() {
            let n_shared = value.key_shared_prefixes[i] as usize;
            let n_more = key_fragment.len();

            if n_shared > prev_key.len() {
                return Err(anyhow!(""));
            }

            let mut key = vec![0u8; n_shared + n_more];
            (key[..n_shared]).copy_from_slice(&prev_key[..n_shared]);
            (key[n_shared..]).copy_from_slice(&key_fragment);

            if keyspace_ids.len() == 1 {
                out.insert((keyspace_ids[0], key.clone()));
            } else {
                for _ in 0..value.key_keyspaces_counts[i] {
                    if j >= value.key_keyspaces_refs.len() {
                        return Err(anyhow!(""));
                    }

                    let idx = value.key_keyspaces_refs[j] as usize;
                    if idx >= keyspace_ids.len() {
                        return Err(anyhow!(""));
                    }

                    let keyspace_id = keyspace_ids[idx];
                    out.insert((keyspace_id, key.clone()));
                    j += 1;
                }
            }

            prev_key = key;
        }

        Ok(out)
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
    ) -> anyhow::Result<(Vec<(Vec<u8>, Timestamp, Vec<u8>)>, Option<Range<Vec<u8>>>)> {
        T::scan_page(self, ts, range).await
    }

    async fn sync(&self, ts: Timestamp) -> anyhow::Result<(Vec<Record>, Timestamp)> {
        T::sync(self, ts).await
    }
}
