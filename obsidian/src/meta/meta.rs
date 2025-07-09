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

use crate::meta::transfer::TransferTabletTransition;
use crate::meta::TabletState;
use crate::meta::TransferState;
use crate::obsidian::Shards;
use crate::obsidian::TabletId;
use crate::pb;
use crate::range::Bound;
use crate::range::Range;
use crate::range::RangeSet;
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
use crate::types::TransferId;
use crate::util::hexlify;
use crate::util::WaitableTimestamp;

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) enum MetaKey {
    Sync,
    ColoGroup(ColoGroupId),
    Keyspace(KeyspaceId),
    Tablet(TabletId),
    Transfer(TransferId),
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

    // (PFX_TRANSFERS, transfer_id) -> pb::internal::TransferMetadata
    const PFX_TRANSFERS: u64 = 5;

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
            Self::Transfer(transfer_id) => tuple_encode(&(Self::PFX_TRANSFERS, transfer_id.0)),
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
                    TabletMetadata {
                        colo_group_id,
                        range,
                        state: MetaState::Stable(TabletState::Active),
                        transfer_id: None,
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

    async fn start_move(&self, src: TabletId, dst: ShardId) -> anyhow::Result<()> {
        let snapshot = self.latest_snapshot_().await?;
        let src_metadata = snapshot.tablet_metadata(src).await?;

        self.start_transfer(vec![src], vec![(dst, src_metadata.range)])
            .await?;

        Ok(())
    }

    async fn start_split(&self, _src: TabletId, _dsts: Vec<ShardId>) -> anyhow::Result<()> {
        // ask tablet for ranges
        todo!();
    }

    async fn start_merge(&self, srcs: Vec<TabletId>, dst: ShardId) -> anyhow::Result<()> {
        let snapshot = self.latest_snapshot_().await?;
        let mut src_range_set = RangeSet::new();
        for src in &srcs {
            let src_metadata = snapshot.tablet_metadata(*src).await?;
            src_range_set.add_range(src_metadata.range);
        }

        let dst_range = src_range_set.contiguous().ok_or_else(|| {
            anyhow!(
                "can't start merge: source tablets not contiguous: {:?} own {:?}",
                srcs,
                src_range_set
            )
        })?;

        self.start_transfer(srcs, vec![(dst, dst_range)]).await?;

        Ok(())
    }

    async fn start_transfer(
        &self,
        srcs: Vec<TabletId>,
        dsts: Vec<(ShardId, Range<Vec<u8>>)>,
    ) -> anyhow::Result<()> {
        let snapshot = self.latest_snapshot_().await?;

        if !((srcs.len() == 1 && dsts.len() >= 1) || (srcs.len() >= 1 && dsts.len() == 1)) {
            return Err(anyhow!(
                "can't do m:n transfers, only 1:1 move, 1:n split, and n:1 merge"
            ));
        }

        let transfer_id = TransferId::new();

        let mut muts = HashMap::new();

        let mut src_range_set = RangeSet::new();
        let mut colo_group_id = None;
        for src in &srcs {
            let mut src_metadata = snapshot.tablet_metadata(*src).await?;

            if let Some(existing_transfer_id) = src_metadata.transfer_id {
                return Err(anyhow!(
                    "can't start transfer: {:?} already participating in {:?}",
                    src,
                    existing_transfer_id,
                ));
            }
            if let MetaState::Transitioning(_, _) = src_metadata.state {
                return Err(anyhow!(
                    "can't start transfer: {:?} already transitioning but not part of another transfer",
                    src,
                ));
            }
            src_range_set.add_range(src_metadata.range.clone());

            src_metadata.transfer_id = Some(transfer_id);
            muts.insert(
                MetaKey::Tablet(*src),
                Mutation::Put(src_metadata.clone().encode_to_vec()),
            );

            if let Some(colo_group_id) = colo_group_id {
                if src_metadata.colo_group_id != colo_group_id {
                    return Err(anyhow!(
                        "can't start transfer: not all tablets from the same colo group: {:?} != {:?}",
                        colo_group_id,
                        src_metadata.colo_group_id,
                    ));
                }
            }
            colo_group_id = Some(src_metadata.colo_group_id);
        }

        let src_range = src_range_set.contiguous().ok_or_else(|| {
            anyhow!(
                "can't start transfer: can only merge contiguous ranges, requested {:?}",
                src_range_set,
            )
        })?;

        let mut dst_tablet_ids = vec![];
        let dst_range_set = RangeSet::from_iter(dsts.iter().map(|(_, range)| range.clone()));
        let dst_range = dst_range_set.contiguous().ok_or_else(|| {
            anyhow!(
                "can't start transfer: can only split into contiguous ranges, requested {:?}",
                dst_range_set,
            )
        })?;

        if src_range != dst_range {
            return Err(anyhow!(
                "can't start transfer: source and destination are different ranges: {:?} != {:?}",
                src_range,
                dst_range,
            ));
        }

        for (dst_shard_id, dst_range) in &dsts {
            let dst_shard = self.shards.shard(*dst_shard_id)?;
            let dst_tablet_id = dst_shard
                .create_tablet(colo_group_id.unwrap(), dst_range.clone())
                .await?;
            dst_tablet_ids.push(dst_tablet_id);
            muts.insert(
                MetaKey::Tablet(dst_tablet_id),
                Mutation::Put(
                    TabletMetadata {
                        colo_group_id: colo_group_id.unwrap(),
                        range: dst_range.clone(),
                        state: MetaState::Stable(TabletState::Hydrating),
                        transfer_id: None,
                    }
                    .encode_to_vec(),
                ),
            );
        }

        muts.insert(
            MetaKey::Transfer(transfer_id),
            Mutation::Put(
                TransferMetadata {
                    state: MetaState::Stable(TransferState::Copy),
                    srcs: srcs,
                    dsts: dst_tablet_ids,
                }
                .encode_to_vec(),
            ),
        );

        self.write_syncable(snapshot, muts).await?;

        // TODO: spawn a task to carry it out

        Ok(())
    }

    async fn transfer_step(&self, transfer_id: TransferId) -> anyhow::Result<bool> {
        let snapshot = self.latest_snapshot_().await?;
        let transfer_metadata = snapshot.transfer_metadata(transfer_id).await?;

        let transfer_state = match transfer_metadata.state {
            MetaState::Transitioning(_, _) => {
                self.roll_transition_forward(transfer_id).await?;
                return Ok(true);
            }
            MetaState::Stable(state) => state,
        };

        match transfer_state {
            TransferState::Copy => {
                // wait for destinations to nearly finish

                self.transition_transfer(snapshot, transfer_id, TransferState::Catchup)
                    .await?;
            }
            TransferState::Catchup => {
                // wait for sources to flush and destinations to fully catch up
                //
                // compare the source manifests and destination manifests
                //
                // if something is wrong then transition to aborting

                self.transition_transfer(snapshot, transfer_id, TransferState::Synced)
                    .await?;
            }
            TransferState::Synced => {
                // No action, we just have to transit this.
                self.transition_transfer(snapshot, transfer_id, TransferState::Handoff)
                    .await?;
            }
            TransferState::Handoff => {
                // No action, we just have to transit this.
                self.transition_transfer(snapshot, transfer_id, TransferState::Complete)
                    .await?;
            }
            TransferState::Complete => return Ok(false),

            TransferState::Aborting => {
                // No action, we just have to transit this.
                self.transition_transfer(snapshot, transfer_id, TransferState::Aborted)
                    .await?;
            }
            TransferState::Aborted => return Ok(false),
        }

        Ok(true)
    }

    async fn transition_transfer<'a>(
        &'a self,
        snapshot: MetaSnapshot<'a, T>,
        transfer_id: TransferId,
        next_state: TransferState,
    ) -> anyhow::Result<()> {
        let transfer_metadata = snapshot.transfer_metadata(transfer_id).await?;

        let curr_state = match transfer_metadata.state {
            MetaState::Stable(curr) => curr,
            MetaState::Transitioning(curr, next) => {
                return Err(anyhow!(
                    "can't transition {:?}: already transitioning {:?} -> {:?}",
                    transfer_id,
                    curr,
                    next
                ));
            }
        };

        let (tablet_ids_to_transition, next_tablet_state) =
            match curr_state.tablet_transition(&next_state) {
                Some(TransferTabletTransition::Srcs(srcs_next_state)) => {
                    (&transfer_metadata.srcs, srcs_next_state)
                }
                Some(TransferTabletTransition::Dsts(dsts_next_state)) => {
                    (&transfer_metadata.dsts, dsts_next_state)
                }
                None => {
                    return Err(anyhow!(
                        "can't transition {:?}: {:?} -> {:?} not allowed by state machine",
                        transfer_id,
                        curr_state,
                        next_state
                    ));
                }
            };

        let mut muts = HashMap::new();
        let next_transfer_metadata = {
            let mut next_transfer_metadata = transfer_metadata.clone();
            next_transfer_metadata.state = MetaState::Transitioning(curr_state, next_state);
            next_transfer_metadata
        };
        muts.insert(
            MetaKey::Transfer(transfer_id),
            Mutation::Put(next_transfer_metadata.encode_to_vec()),
        );

        for tablet_id in tablet_ids_to_transition {
            let mut tablet_metadata = snapshot.tablet_metadata(*tablet_id).await?;

            let curr_tablet_state = match tablet_metadata.state {
                MetaState::Stable(curr_state) => curr_state,
                MetaState::Transitioning(_, _) => {
                    return Err(anyhow!(
                        "can't transition {:?}: participant {:?} already transitioning",
                        transfer_id,
                        tablet_id
                    ));
                }
            };

            tablet_metadata.state = MetaState::Transitioning(curr_tablet_state, next_tablet_state);

            muts.insert(
                MetaKey::Tablet(*tablet_id),
                Mutation::Put(tablet_metadata.encode_to_vec()),
            );
        }

        let _ = self.write_syncable(snapshot, muts).await?;

        self.roll_transition_forward(transfer_id).await?;

        Ok(())
    }

    async fn roll_transition_forward(&self, transfer_id: TransferId) -> anyhow::Result<()> {
        let snapshot = self.latest_snapshot_().await?;
        let transfer_metadata = snapshot.transfer_metadata(transfer_id).await?;

        let (curr_state, next_state) = match transfer_metadata.state {
            // Already done.
            MetaState::Stable(_) => return Ok(()),
            MetaState::Transitioning(curr_state, next_state) => (curr_state, next_state),
        };

        let (tablet_ids_to_transition, next_tablet_state) =
            match curr_state.tablet_transition(&next_state) {
                Some(TransferTabletTransition::Srcs(srcs_next_state)) => {
                    (&transfer_metadata.srcs, srcs_next_state)
                }
                Some(TransferTabletTransition::Dsts(dsts_next_state)) => {
                    (&transfer_metadata.dsts, dsts_next_state)
                }
                None => {
                    return Err(anyhow!(
                        "can't transition {:?}: {:?} -> {:?} not allowed by state machine",
                        transfer_id,
                        curr_state,
                        next_state
                    ));
                }
            };

        // IMPORTANT: Must make sure that all of the participating tablets are aware of the
        // transitioning state before continuing.
        for tablet_id in tablet_ids_to_transition {
            // This means they're at least aware of what we're seeing in `snapshot`, and
            // `write_syncable` below will fail if it turns out we were out of date.
            self.shards
                .tablet(*tablet_id)?
                .wait_meta_sync(snapshot.ts)
                .await?;
        }

        let mut muts = HashMap::new();
        let next_transfer_metadata = {
            let mut next_transfer_metadata = transfer_metadata.clone();
            next_transfer_metadata.state = MetaState::Stable(next_state);
            next_transfer_metadata
        };
        muts.insert(
            MetaKey::Transfer(transfer_id),
            Mutation::Put(next_transfer_metadata.encode_to_vec()),
        );

        for tablet_id in tablet_ids_to_transition {
            let mut tablet_metadata = snapshot.tablet_metadata(*tablet_id).await?;

            match tablet_metadata.state {
                MetaState::Stable(curr_state) => {
                    return Err(anyhow!(
                        "can't roll forward transition for {:?}: participant {:?} already stable in {:?}",
                        transfer_id,
                        tablet_id,
                        curr_state,
                    ));
                }
                MetaState::Transitioning(_, attempting_transition_to) => {
                    if next_tablet_state != attempting_transition_to {
                        return Err(anyhow!(
                            "can't roll forward transition for {:?}: participant {:?} trying to transition to {:?} instead of {:?}",
                            transfer_id,
                            tablet_id,
                            attempting_transition_to,
                            next_tablet_state,
                        ));
                    }
                }
            }

            tablet_metadata.state = MetaState::Stable(next_tablet_state);
            muts.insert(
                MetaKey::Tablet(*tablet_id),
                Mutation::Put(tablet_metadata.encode_to_vec()),
            );
        }

        let _ = self.write_syncable(snapshot, muts).await?;

        Ok(())
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

    async fn tablet_metadata(&self, tablet_id: TabletId) -> anyhow::Result<TabletMetadata>
    where
        Self: Sized,
    {
        get_meta_key::<Self, TabletMetadata, pb::internal::TabletMetadata>(
            self,
            &MetaKey::Tablet(tablet_id),
        )
        .await?
        .ok_or_else(|| anyhow!("{:?} not found", tablet_id))
    }

    async fn transfer_metadata(&self, transfer_id: TransferId) -> anyhow::Result<TransferMetadata>
    where
        Self: Sized,
    {
        get_meta_key::<Self, TransferMetadata, pb::internal::TransferMetadata>(
            self,
            &MetaKey::Transfer(transfer_id),
        )
        .await?
        .ok_or_else(|| anyhow!("{:?} not found", transfer_id))
    }
}

async fn get_meta_key<R: MetaReader, V: Value<PB = PB> + TryFrom<PB, Error = anyhow::Error>, PB>(
    reader: &R,
    meta_key: &MetaKey,
) -> anyhow::Result<Option<V>> {
    if let Some(value) = reader.get(&meta_key.encode()).await? {
        return Ok(Some(V::decode(&value[..])?));
    }
    Ok(None)
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

trait Value {
    type PB: prost::Message + Default;

    fn encode_to_vec(self) -> Vec<u8>
    where
        Self: Into<Self::PB> + Sized,
    {
        Into::<Self::PB>::into(self).encode_to_vec()
    }

    fn decode(b: &[u8]) -> anyhow::Result<Self>
    where
        Self: TryFrom<Self::PB, Error = anyhow::Error> + Sized,
    {
        Ok(Self::try_from(Self::PB::decode(b)?)?)
    }
}

#[derive(Clone)]
pub(crate) enum MetaState<T> {
    Stable(T),
    Transitioning(T, T),
}

impl<T> MetaState<T> {
    fn current(&self) -> &T {
        match self {
            Self::Stable(curr) => curr,
            Self::Transitioning(curr, _) => curr,
        }
    }

    fn next(&self) -> Option<&T> {
        match self {
            Self::Stable(_) => None,
            Self::Transitioning(_, next) => Some(next),
        }
    }
}

impl<T> From<(T, Option<T>)> for MetaState<T> {
    fn from(value: (T, Option<T>)) -> Self {
        match value.1 {
            None => Self::Stable(value.0),
            Some(next) => Self::Transitioning(value.0, next),
        }
    }
}

#[derive(Clone)]
pub(crate) struct TabletMetadata {
    pub(crate) colo_group_id: ColoGroupId,
    pub(crate) range: Range<Vec<u8>>,
    pub(crate) state: MetaState<TabletState>,
    pub(crate) transfer_id: Option<TransferId>,
}

impl Value for TabletMetadata {
    type PB = pb::internal::TabletMetadata;
}

impl TryFrom<pb::internal::TabletMetadata> for TabletMetadata {
    type Error = anyhow::Error;

    fn try_from(value_pb: pb::internal::TabletMetadata) -> Result<Self, Self::Error> {
        Ok(Self {
            colo_group_id: ColoGroupId(value_pb.colo_group_id),
            range: Range::try_from(value_pb.range.ok_or_else(|| anyhow!("missing range"))?)?,
            state: MetaState::from((
                TabletState::try_from(
                    pb::internal::TabletState::from_i32(value_pb.state)
                        .ok_or_else(|| anyhow!("missing state"))?,
                )?,
                match pb::internal::TabletState::from_i32(value_pb.next_state) {
                    Some(pb::internal::TabletState::Unknown) => None,
                    None => None,
                    Some(state_pb) => Some(TabletState::try_from(state_pb)?),
                },
            )),
            transfer_id: value_pb.transfer_id.map(TransferId::try_from).transpose()?,
        })
    }
}

impl From<TabletMetadata> for pb::internal::TabletMetadata {
    fn from(value: TabletMetadata) -> Self {
        Self {
            colo_group_id: value.colo_group_id.0,
            range: Some(value.range.into()),
            state: pb::internal::TabletState::from(*value.state.current()) as i32,
            next_state: value
                .state
                .next()
                .map(|state| pb::internal::TabletState::from(*state) as i32)
                .unwrap_or(0),
            transfer_id: value.transfer_id.map(TransferId::into),
        }
    }
}

#[derive(Clone)]
pub(crate) struct TransferMetadata {
    pub(crate) state: MetaState<TransferState>,
    pub(crate) srcs: Vec<TabletId>,
    pub(crate) dsts: Vec<TabletId>,
}

impl Value for TransferMetadata {
    type PB = pb::internal::TransferMetadata;
}

impl TryFrom<pb::internal::TransferMetadata> for TransferMetadata {
    type Error = anyhow::Error;

    fn try_from(value_pb: pb::internal::TransferMetadata) -> Result<Self, Self::Error> {
        Ok(Self {
            state: MetaState::from((
                TransferState::try_from(
                    pb::internal::TransferState::from_i32(value_pb.state)
                        .ok_or_else(|| anyhow!("missing state"))?,
                )?,
                match pb::internal::TransferState::from_i32(value_pb.next_state) {
                    Some(pb::internal::TransferState::Unknown) => None,
                    None => None,
                    Some(state_pb) => Some(TransferState::try_from(state_pb)?),
                },
            )),
            srcs: value_pb
                .srcs
                .into_iter()
                .map(TabletId::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            dsts: value_pb
                .dsts
                .into_iter()
                .map(TabletId::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl From<TransferMetadata> for pb::internal::TransferMetadata {
    fn from(value: TransferMetadata) -> Self {
        Self {
            state: pb::internal::TransferState::from(*value.state.current()) as i32,
            next_state: value
                .state
                .next()
                .map(|state| pb::internal::TransferState::from(*state) as i32)
                .unwrap_or(0),
            srcs: value
                .srcs
                .into_iter()
                .map(pb::internal::TabletId::from)
                .collect(),
            dsts: value
                .dsts
                .into_iter()
                .map(pb::internal::TabletId::from)
                .collect(),
        }
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
