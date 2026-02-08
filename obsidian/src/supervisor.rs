use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::TryStreamExt;

use crate::lsm::Manifest;
use crate::meta::MetaKey;
use crate::meta::MetaReader;
use crate::meta::MetaState;
use crate::meta::MetaSynced;
use crate::meta::MetaSyncedSnapshot;
use crate::meta::MetaValue;
use crate::meta::MetaWatcher;
use crate::meta::SyncType;
use crate::meta::TabletMetadata;
use crate::meta::TabletState;
use crate::meta::TransferMetadata;
use crate::meta::TransferState;
use crate::runtime::Meta;
use crate::runtime::Shards;
use crate::util::Retry;
use crate::util::WithBackground;
use crate::Mutation;
use crate::Range;
use crate::RangeSet;
use crate::ShardId;
use crate::TabletId;
use crate::TransferId;

const CATCHUP_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct Supervisor(WithBackground<SupervisorInner>);

struct SupervisorInner {
    meta: Arc<dyn Meta>,
    meta_synced: Arc<MetaSynced>,
    shards: Arc<dyn Shards>,
}

impl Supervisor {
    pub(crate) fn new(
        meta: Arc<dyn Meta>,
        meta_synced: Arc<MetaSynced>,
        shards: Arc<dyn Shards>,
    ) -> Self {
        // TODO: scan for transfers
        let supervisor = Self(WithBackground::new(Arc::new(SupervisorInner {
            meta,
            meta_synced: Arc::clone(&meta_synced),
            shards,
        })));

        meta_synced.subscribe2(&supervisor.0);

        supervisor
    }

    pub(crate) async fn start_move(
        &self,
        src: TabletId,
        dst: ShardId,
    ) -> anyhow::Result<TransferId> {
        let snapshot = self.0.latest_snapshot().await?;
        let src_metadata = snapshot.tablet_metadata(src).await?;

        Ok(self
            .start_transfer(vec![src], vec![(dst, src_metadata.range)])
            .await?)
    }

    pub(crate) async fn start_split(
        &self,
        src: TabletId,
        dst_a: ShardId,
        dst_b: ShardId,
    ) -> anyhow::Result<TransferId> {
        let snapshot = self.0.latest_snapshot().await?;
        let src_metadata = snapshot.tablet_metadata(src).await?;

        let src_tablet = self.0.shards.tablet(src)?;
        let split_point = src_tablet.find_split().await?;

        log::debug!("selected {:?} for split of {:?}", split_point, src);

        let (range_a, range_b) = src_metadata.range.split(&split_point);

        self.start_transfer(vec![src], vec![(dst_a, range_a), (dst_b, range_b)])
            .await
    }

    pub(crate) async fn start_merge(
        &self,
        srcs: Vec<TabletId>,
        dst: ShardId,
    ) -> anyhow::Result<TransferId> {
        let snapshot = self.0.latest_snapshot().await?;
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

        Ok(self.start_transfer(srcs, vec![(dst, dst_range)]).await?)
    }

    pub(crate) async fn wait_transfer(&self, transfer_id: TransferId) -> anyhow::Result<()> {
        loop {
            let snapshot = self.0.latest_snapshot().await?;
            let transfer_metadata = snapshot.transfer_metadata(transfer_id).await?;

            match transfer_metadata.state {
                MetaState::Stable(TransferState::Complete) => {
                    return Ok(());
                }
                MetaState::Stable(TransferState::Aborted) => {
                    return Err(anyhow!("{:?} aborted", transfer_id));
                }
                _ => {}
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn start_transfer(
        &self,
        srcs: Vec<TabletId>,
        dsts: Vec<(ShardId, Range<Vec<u8>>)>,
    ) -> anyhow::Result<TransferId> {
        let snapshot = self.0.latest_snapshot().await?;

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

        self.0.meta.write(snapshot.ts(), muts).await?;

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

impl SupervisorInner {
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
        let snapshot = self.latest_snapshot().await?;
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
                    self.transition_transfer(snapshot, transfer_id, TransferState::Aborted)
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

                if let Err(e) = check_manifests_equal(&src_manifests[..], &dst_manifests[..]) {
                    self.transition_transfer(snapshot, transfer_id, TransferState::Aborted)
                        .await?;
                    log::error!(
                        "{:?} destination manifests didn't match sources:\nerr:\n{:?}\nsources:\n{:?}\ndestinations:\n{:?}",
                        transfer_id,
                        e,
                        src_manifests,
                        dst_manifests,
                    );
                    return Ok(true);
                }

                self.transition_transfer(snapshot, transfer_id, TransferState::Synced)
                    .await?;
            }
            TransferState::Synced => {
                // TODO: advance destination sequencers

                // No action, we just have to transit this to ensure we add
                // TabletStateProperties::Complete to the desinations before we remove it from the
                // sources below.
                self.transition_transfer(snapshot, transfer_id, TransferState::Handoff)
                    .await?;
            }
            TransferState::Handoff => {
                // No action, we just have to transit this to ensure we've already removed
                // TabletStateProperties::Readable from the sources before granting
                // TabletStateProperties::Writable to the destination.
                self.transition_transfer(snapshot, transfer_id, TransferState::Complete)
                    .await?;
            }
            TransferState::Complete => return Ok(false),
            TransferState::Aborted => return Ok(false),
        }

        Ok(true)
    }

    async fn transition_transfer(
        &self,
        snapshot: MetaSyncedSnapshot,
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

        if !curr_state.can_transition(next_state) {
            return Err(anyhow!(
                "can't transition {:?}: {:?} -> {:?} not allowed by state machine",
                transfer_id,
                curr_state,
                next_state
            ));
        }

        let (srcs_next_state, dsts_next_state) = next_state.tablet_states();

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

        for (tablet_ids, next_tablet_state) in [
            (transfer_metadata.srcs, srcs_next_state),
            (transfer_metadata.dsts, dsts_next_state),
        ] {
            for tablet_id in &tablet_ids {
                let mut tablet_metadata = snapshot.tablet_metadata(*tablet_id).await?;

                let curr_tablet_state = match tablet_metadata.state {
                    MetaState::Stable(curr_state) => {
                        if curr_state == next_tablet_state {
                            continue;
                        }
                        curr_state
                    }
                    MetaState::Transitioning(_, _) => {
                        return Err(anyhow!(
                            "can't transition {:?}: participant {:?} already transitioning",
                            transfer_id,
                            tablet_id
                        ));
                    }
                };

                tablet_metadata.state =
                    MetaState::Transitioning(curr_tablet_state, next_tablet_state);

                muts.insert(
                    MetaKey::Tablet(*tablet_id),
                    Mutation::Put(tablet_metadata.encode_to_vec()),
                );
            }
        }

        log::info!("{:?} transitioning to {:?}", transfer_id, next_state);

        let _ = self.meta.write(snapshot.ts(), muts).await?;

        log::info!("{:?} transition to {:?} persisted", transfer_id, next_state);

        self.roll_transition_forward(transfer_id).await?;

        log::info!(
            "{:?} transition to {:?} rolled forward",
            transfer_id,
            next_state
        );

        Ok(())
    }

    async fn roll_transition_forward(&self, transfer_id: TransferId) -> anyhow::Result<()> {
        let snapshot = self.latest_snapshot().await?;
        let transfer_metadata = snapshot.transfer_metadata(transfer_id).await?;

        let (curr_state, next_state) = match transfer_metadata.state {
            // Already done.
            MetaState::Stable(_) => return Ok(()),
            MetaState::Transitioning(curr_state, next_state) => (curr_state, next_state),
        };

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

        let (curr_srcs_state, curr_dsts_state) = curr_state.tablet_states();
        let (next_srcs_state, next_dsts_state) = next_state.tablet_states();

        let srcs_expected_state = if curr_srcs_state == next_srcs_state {
            MetaState::Stable(curr_srcs_state)
        } else {
            MetaState::Transitioning(curr_srcs_state, next_srcs_state)
        };
        let dsts_expected_state = if curr_dsts_state == next_dsts_state {
            MetaState::Stable(curr_dsts_state)
        } else {
            MetaState::Transitioning(curr_dsts_state, next_dsts_state)
        };

        for (tablet_ids, expected_state) in [
            (&transfer_metadata.srcs, srcs_expected_state),
            (&transfer_metadata.dsts, dsts_expected_state),
        ] {
            for tablet_id in tablet_ids {
                let mut tablet_metadata = snapshot.tablet_metadata(*tablet_id).await?;

                if tablet_metadata.state != expected_state {
                    return Err(anyhow!(
                        "can't roll forward transition for {:?}: participant {:?} not in expected state: {:?} != {:?}",
                        transfer_id,
                        tablet_id,
                        tablet_metadata.state,
                        expected_state,
                    ));
                }

                if let MetaState::Transitioning(_, next_tablet_state) = tablet_metadata.state {
                    // IMPORTANT: Must make sure that all of the participating tablets are aware of
                    // the transitioning state before continuing.
                    self.shards
                        .tablet(*tablet_id)?
                        .wait_meta_sync(snapshot.ts())
                        .await?;

                    tablet_metadata.state = MetaState::Stable(next_tablet_state);
                    muts.insert(
                        MetaKey::Tablet(*tablet_id),
                        Mutation::Put(tablet_metadata.encode_to_vec()),
                    );
                }
            }
        }

        let _ = self.meta.write(snapshot.ts(), muts).await?;

        log::info!("{:?} transitioned to {:?}", transfer_id, next_state);

        Ok(())
    }

    async fn latest_snapshot(&self) -> anyhow::Result<MetaSyncedSnapshot> {
        let ts = self.meta.latest_snapshot().await?;
        self.meta_synced.wait(ts).await?;
        Ok(self.meta_synced.snapshot())
    }
}

#[async_trait]
impl MetaWatcher for SupervisorInner
{
    async fn sync_meta(&self, sync_type: SyncType, snapshot: MetaSyncedSnapshot) {}
}

fn check_manifests_equal(a: &[&Manifest], b: &[&Manifest]) -> anyhow::Result<()> {
    let mut a_runs = HashMap::new();
    for manifest in a {
        for (keyspace_id, level, run_manifest) in manifest.runs() {
            // There are duplicates of this run ID in a.
            if a_runs.contains_key(&run_manifest.run_id) {
                return Err(anyhow!("left has duplicate run {:?}", run_manifest.run_id));
            }

            a_runs.insert(run_manifest.run_id, (keyspace_id, level));
        }
    }

    let mut found = HashSet::new();
    for manifest in b {
        for (keyspace_id, level, run_manifest) in manifest.runs() {
            if found.contains(&run_manifest.run_id) {
                // There are duplicates of this run ID in b.
                return Err(anyhow!("right has duplicate run {:?}", run_manifest.run_id));
            }

            if !a_runs
                .get(&run_manifest.run_id)
                .map(|(a_keyspace_id, a_level)| (keyspace_id, level) == (*a_keyspace_id, *a_level))
                .unwrap_or(false)
            {
                // This run isn't in a or it's not in the same (keyspace, level).
                return Err(anyhow!("left is missing run {:?}", run_manifest.run_id));
            }

            found.insert(run_manifest.run_id);
        }
    }

    if a_runs.len() != found.len() {
        for run_id in a_runs.keys() {
            if !found.contains(run_id) {
                return Err(anyhow!("right is missing run {:?}", run_id));
            }
        }
        return Err(anyhow!("right is missing run"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::collections::HashMap;

    use byteorder::BigEndian;
    use byteorder::ByteOrder;
    use rand::RngCore;

    use crate::meta::MetaReader;
    use crate::test::ObsidianForTest;
    use crate::Bound;
    use crate::ColoGroupId;
    use crate::KeyspaceId;
    use crate::Mutation;
    use crate::Obsidian;

    #[tokio::test]
    async fn test_move() -> anyhow::Result<()> {
        let _ = pretty_env_logger::try_init();

        let obs = ObsidianForTest::new(2 /*n_shards*/).await?;
        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);

        obs.gateway
            .create_colo_group(
                keyspace_id.0,
                vec![Bound::Before(b"b".to_vec())], // splits
            )
            .await?;
        obs.gateway.create_keyspace(keyspace_id).await?;

        let kvs = [
            (b"aa", b"foo"),
            (b"ab", b"bar"),
            (b"ba", b"baz"),
            (b"ba", b"baz"),
        ];

        for (key, value) in &kvs {
            obs.gateway
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
            .supervisor
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

        obs.supervisor.wait_transfer(transfer_id).await?;

        // TODO: jank, because we need to wait for the gateway to find out about the routing
        // change
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let ts = obs
            .gateway
            .latest_snapshot(kvs.iter().map(|(k, _)| (keyspace_id, k.to_vec())).collect())
            .await?;
        for (key, value) in &kvs {
            let actual = obs.gateway.get(ts, &(keyspace_id, key.to_vec())).await?;

            assert_eq!(
                Some(&value.to_vec()),
                actual.as_ref().map(|record| &record.value)
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_merge() -> anyhow::Result<()> {
        let _ = pretty_env_logger::try_init();

        let obs = ObsidianForTest::new(3 /*n_shards*/).await?;
        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);

        obs.gateway
            .create_colo_group(
                keyspace_id.0,
                vec![Bound::Before(b"b".to_vec())], // splits
            )
            .await?;
        obs.gateway.create_keyspace(keyspace_id).await?;

        let kvs = [
            (b"aa", b"foo"),
            (b"ab", b"bar"),
            (b"ba", b"baz"),
            (b"ba", b"baz"),
        ];

        for (key, value) in &kvs {
            obs.gateway
                .write(
                    vec![],
                    BTreeMap::from([((keyspace_id, key.to_vec()), Mutation::Put(value.to_vec()))]),
                )
                .await?;
        }

        let meta_snapshot = obs.meta.latest_snapshot_().await?;
        let tablet_ids = meta_snapshot.tablet_ids().await?;
        let shard_ids = meta_snapshot.shard_ids().await?;

        let target_shard_id = shard_ids
            .iter()
            .filter(|shard_id| !tablet_ids.iter().any(|tablet_id| tablet_id.0 == **shard_id))
            .copied()
            .next()
            .unwrap();

        let transfer_id = obs
            .supervisor
            .start_merge(tablet_ids, target_shard_id)
            .await?;

        obs.supervisor.wait_transfer(transfer_id).await?;

        // TODO: jank, because we need to wait for the gateway to find out about the routing
        // change
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let ts = obs
            .gateway
            .latest_snapshot(kvs.iter().map(|(k, _)| (keyspace_id, k.to_vec())).collect())
            .await?;
        for (key, value) in &kvs {
            let actual = obs.gateway.get(ts, &(keyspace_id, key.to_vec())).await?;

            assert_eq!(
                Some(&value.to_vec()),
                actual.as_ref().map(|record| &record.value)
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_split() -> anyhow::Result<()> {
        let _ = pretty_env_logger::try_init();

        let obs = ObsidianForTest::new(3 /*n_shards*/).await?;
        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);

        obs.gateway
            .create_colo_group(
                keyspace_id.0,
                vec![], // splits
            )
            .await?;
        obs.gateway.create_keyspace(keyspace_id).await?;

        let mut expected = HashMap::new();
        let mut writes_done = 0;
        // We have to do a bunch of writes for there to be enough for a split estimate.
        for prefix in [0u8, 1u8] {
            for i in 0..5 {
                let mut muts = BTreeMap::new();
                for j in 0..100 {
                    let mut key = [0u8; 8];
                    BigEndian::write_u64(&mut key, i * 100 + j);
                    key[0] = prefix;

                    let mut value = [0u8; 32];
                    rand::thread_rng().fill_bytes(&mut value);

                    expected.insert(key.to_vec(), value.to_vec());
                    muts.insert((keyspace_id, key.to_vec()), Mutation::Put(value.to_vec()));
                    writes_done += 1;
                    if writes_done % 100 == 0 {
                        log::debug!("{} writes done", writes_done);
                    }
                }

                obs.gateway.write(vec![], muts).await?;
            }
        }

        let meta_snapshot = obs.meta.latest_snapshot_().await?;
        let tablet_ids = meta_snapshot.tablet_ids().await?;
        let shard_ids = meta_snapshot.shard_ids().await?;

        let target_shard_ids: Vec<_> = shard_ids
            .iter()
            .filter(|shard_id| !tablet_ids.iter().any(|tablet_id| tablet_id.0 == **shard_id))
            .copied()
            .collect();

        let transfer_id = obs
            .supervisor
            .start_split(tablet_ids[0], target_shard_ids[0], target_shard_ids[1])
            .await?;

        obs.supervisor.wait_transfer(transfer_id).await?;

        // TODO: jank, because we need to wait for the gateway to find out about the routing
        // change
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        for (key, value) in &expected {
            let ts = obs
                .gateway
                .latest_snapshot(BTreeSet::from([(keyspace_id, key.clone())]))
                .await?;

            let actual = obs.gateway.get(ts, &(keyspace_id, key.to_vec())).await?;

            assert_eq!(
                Some(&value.to_vec()),
                actual.as_ref().map(|record| &record.value)
            );
        }

        Ok(())
    }
}
