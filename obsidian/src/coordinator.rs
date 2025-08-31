use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::anyhow;
use futures::stream::FuturesUnordered;
use futures::TryStreamExt;

use crate::lsm::Manifest;
use crate::meta::MetaImpl;
use crate::meta::MetaKey;
use crate::meta::MetaReader;
use crate::meta::MetaSnapshot;
use crate::meta::MetaState;
use crate::meta::TabletMetadata;
use crate::meta::TabletState;
use crate::meta::TransferMetadata;
use crate::meta::TransferState;
use crate::meta::TransferTabletTransition;
use crate::meta::Value;
use crate::obsidian::Shards;
use crate::obsidian::TabletId;
use crate::range::Range;
use crate::range::RangeSet;
use crate::tablet::Tablet;
use crate::types::Mutation;
use crate::types::ShardId;
use crate::types::TransferId;
use crate::util::Retry;
use crate::util::WithBackground;

const CATCHUP_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct Coordinator<T>(WithBackground<CoordinatorInner<T>>);

struct CoordinatorInner<T> {
    meta: Arc<MetaImpl<T>>,
    shards: Arc<dyn Shards + Send + Sync>,
}

impl<T> Coordinator<T>
where
    T: Tablet + Sync + Send + 'static,
{
    pub(crate) fn new(meta: Arc<MetaImpl<T>>, shards: Arc<dyn Shards + Send + Sync>) -> Self {
        // TODO: scan for transfers
        Self(WithBackground::new(Arc::new(CoordinatorInner {
            meta,
            shards,
        })))
    }

    pub(crate) async fn start_move(
        &self,
        src: TabletId,
        dst: ShardId,
    ) -> anyhow::Result<TransferId> {
        let snapshot = self.0.meta.latest_snapshot_().await?;
        let src_metadata = snapshot.tablet_metadata(src).await?;

        Ok(self
            .start_transfer(vec![src], vec![(dst, src_metadata.range)])
            .await?)
    }

    async fn start_split(&self, _src: TabletId, _dsts: Vec<ShardId>) -> anyhow::Result<()> {
        // ask tablet for ranges
        todo!();
    }

    async fn start_merge(&self, srcs: Vec<TabletId>, dst: ShardId) -> anyhow::Result<()> {
        let snapshot = self.0.meta.latest_snapshot_().await?;
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

    pub(crate) async fn wait_transfer(&self, transfer_id: TransferId) -> anyhow::Result<()> {
        loop {
            let snapshot = self.0.meta.latest_snapshot_().await?;
            let transfer_metadata = snapshot.transfer_metadata(transfer_id).await?;

            if matches!(
                transfer_metadata.state,
                MetaState::Stable(TransferState::Complete)
                    | MetaState::Stable(TransferState::Aborted)
            ) {
                return Ok(());
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn start_transfer(
        &self,
        srcs: Vec<TabletId>,
        dsts: Vec<(ShardId, Range<Vec<u8>>)>,
    ) -> anyhow::Result<TransferId> {
        let snapshot = self.0.meta.latest_snapshot_().await?;

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
            let dst_tablet_id = snapshot.next_tablet_id(*dst_shard_id).await?;
            dst_tablet_ids.push(dst_tablet_id);
            muts.insert(
                MetaKey::Tablet(dst_tablet_id),
                Mutation::Put(
                    TabletMetadata {
                        colo_group_id: colo_group_id.unwrap(),
                        range: dst_range.clone(),
                        state: MetaState::Stable(TabletState::Hydrating),
                        transfer_id: Some(transfer_id),
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
                    srcs: srcs.clone(),
                    dsts: dst_tablet_ids.clone(),
                    timestamp: SystemTime::now(),
                }
                .encode_to_vec(),
            ),
        );

        self.0.meta.write_syncable(snapshot, muts).await?;

        log::info!(
            "{:?} started {:?} -> {:?}",
            transfer_id,
            srcs,
            dst_tablet_ids
        );

        self.0.spawn(async move |inner| {
            inner.transfer(transfer_id).await;
        });

        Ok(transfer_id)
    }
}

impl<T> CoordinatorInner<T>
where
    T: Tablet + Sync + Send + 'static,
{
    async fn transfer(&self, transfer_id: TransferId) {
        Retry::new()
            .indefinitely(&async || -> anyhow::Result<()> {
                loop {
                    let should_continue = self.transfer_step(transfer_id).await?;
                    if !should_continue {
                        return Ok(());
                    }
                }
            })
            .await;
    }

    async fn transfer_step(&self, transfer_id: TransferId) -> anyhow::Result<bool> {
        let snapshot = self.meta.latest_snapshot_().await?;
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
                // Wait until the destinations are all mostly caught up, so that we don't have to
                // freeze writes at the sources for very long.
                for dst_tablet_id in transfer_metadata.dsts {
                    let tablet = self.shards.tablet(dst_tablet_id)?;
                    tablet.wait_mostly_hydrated().await?;
                }

                // Stop accepting writes at the sources so the destinations can catch up.
                self.transition_transfer(snapshot, transfer_id, TransferState::Catchup)
                    .await?;
            }
            TransferState::Catchup => {
                let time_in_state = transfer_metadata.timestamp.elapsed()?;
                if time_in_state > CATCHUP_TIMEOUT {
                    log::error!(
                        "{:?} timed out waiting for catchup after {:?}, aborting",
                        transfer_id,
                        time_in_state,
                    );
                    self.transition_transfer(snapshot, transfer_id, TransferState::Aborting)
                        .await?;
                    return Ok(true);
                }

                tokio::time::timeout(
                    CATCHUP_TIMEOUT,
                    // Wait for the sources to fully catch up.
                    transfer_metadata
                        .dsts
                        .iter()
                        .map(async |tablet_id| -> anyhow::Result<()> {
                            self.shards.tablet(*tablet_id)?.catchup().await?;
                            Ok(())
                        })
                        .collect::<FuturesUnordered<_>>()
                        .try_collect::<Vec<_>>(),
                )
                .await??;

                // Make sure the destinations actually have all of the data that the sources did.
                let manifests: HashMap<_, _> =
                    Iterator::chain(transfer_metadata.srcs.iter(), transfer_metadata.dsts.iter())
                        .map(async |tablet_id| {
                            Ok::<_, anyhow::Error>((
                                *tablet_id,
                                self.shards.tablet(*tablet_id)?.manifest().await?,
                            ))
                        })
                        .collect::<FuturesUnordered<_>>()
                        .try_collect()
                        .await?;

                let src_manifests: Vec<_> = transfer_metadata
                    .srcs
                    .iter()
                    .map(|tablet_id| &manifests[tablet_id])
                    .collect();
                let dst_manifests: Vec<_> = transfer_metadata
                    .dsts
                    .iter()
                    .map(|tablet_id| &manifests[tablet_id])
                    .collect();

                if !manifests_equal(&src_manifests[..], &dst_manifests[..]) {
                    self.transition_transfer(snapshot, transfer_id, TransferState::Aborting)
                        .await?;
                    log::error!(
                        "{:?} destination manifests didn't match sources",
                        transfer_id
                    );
                    return Ok(true);
                }

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
            next_transfer_metadata.timestamp = SystemTime::now();
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

        log::info!("{:?} transitioning to {:?}", transfer_id, next_state);

        let _ = self.meta.write_syncable(snapshot, muts).await?;

        self.roll_transition_forward(transfer_id).await?;

        Ok(())
    }

    async fn roll_transition_forward(&self, transfer_id: TransferId) -> anyhow::Result<()> {
        let snapshot = self.meta.latest_snapshot_().await?;
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
                .wait_meta_sync(snapshot.ts())
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

        let _ = self.meta.write_syncable(snapshot, muts).await?;

        log::info!("{:?} transitioned to {:?}", transfer_id, next_state);

        Ok(())
    }
}

fn manifests_equal(a: &[&Manifest], b: &[&Manifest]) -> bool {
    let mut a_runs = HashMap::new();
    for manifest in a {
        for (keyspace_id, keyspace) in &manifest.keyspaces {
            for (i, level) in keyspace.levels.iter().enumerate() {
                for run_manifest in &level.runs {
                    // There are duplicates of this run ID in a.
                    if a_runs.contains_key(&run_manifest.run_id) {
                        return false;
                    }

                    a_runs.insert(run_manifest.run_id, (*keyspace_id, i));
                }
            }
        }
    }

    let mut found = HashSet::new();
    for manifest in b {
        for (keyspace_id, keyspace) in &manifest.keyspaces {
            for (i, level) in keyspace.levels.iter().enumerate() {
                for run_manifest in &level.runs {
                    if found.contains(&run_manifest.run_id) {
                        // There are duplicates of this run ID in b.
                        return false;
                    }

                    if !a_runs
                        .get(&run_manifest.run_id)
                        .map(|(a_keyspace_id, a_level)| {
                            (*keyspace_id, i) == (*a_keyspace_id, *a_level)
                        })
                        .unwrap_or(false)
                    {
                        // This run isn't in a or it's not in the same (keyspace, level).
                        return false;
                    }

                    found.insert(run_manifest.run_id);
                }
            }
        }
    }

    a_runs.len() == found.len()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::meta::MetaReader;
    use crate::obsidian::Obsidian;
    use crate::range::Bound;
    use crate::test::ObsidianForTest;
    use crate::types::ColoGroupId;
    use crate::types::KeyspaceId;
    use crate::types::Mutation;

    #[tokio::test]
    async fn test_transfer() -> anyhow::Result<()> {
        let _ = pretty_env_logger::init();

        let obs = ObsidianForTest::new(2 /*n_shards*/).await?;
        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);

        obs.frontend
            .create_colo_group(
                keyspace_id.0,
                vec![Bound::Before(b"b".to_vec())], // splits
            )
            .await?;
        obs.frontend.create_keyspace(keyspace_id).await?;

        let kvs = [
            (b"aa", b"foo"),
            (b"ab", b"bar"),
            (b"ba", b"baz"),
            (b"ba", b"baz"),
        ];

        for (key, value) in &kvs {
            obs.frontend
                .write(
                    vec![],
                    BTreeMap::from([((keyspace_id, key.to_vec()), Mutation::Put(value.to_vec()))]),
                )
                .await?;
        }

        let meta_snapshot = obs.meta.latest_snapshot_().await?;
        let tablet_ids = meta_snapshot.tablet_ids().await?;
        let shard_ids = meta_snapshot.shard_ids().await?;

        let transfer_id = obs
            .coordinator
            .start_move(
                tablet_ids[0],
                shard_ids
                    .iter()
                    .filter(|shard_id| **shard_id != tablet_ids[0].0)
                    .copied()
                    .next()
                    .unwrap(),
            )
            .await?;

        obs.coordinator.wait_transfer(transfer_id).await?;

        let ts = obs
            .frontend
            .latest_snapshot(kvs.iter().map(|(k, _)| (keyspace_id, k.to_vec())).collect())
            .await?;
        for (key, value) in &kvs {
            let actual = obs.frontend.get(ts, &(keyspace_id, key.to_vec())).await?;

            assert_eq!(Some(&value.to_vec()), actual.as_ref().map(|record| &record.value));
        }

        Ok(())
    }
}
