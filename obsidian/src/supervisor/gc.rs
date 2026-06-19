use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use futures::stream::FuturesUnordered;
use futures::TryStreamExt;
use obsidian_common::ColoGroupId;
use obsidian_common::Key;
use obsidian_common::KeyspaceId;
use obsidian_common::Mutation;
use obsidian_common::RunId;
use obsidian_common::ShardId;
use obsidian_common::TabletId;
use obsidian_common::Timestamp;
use obsidian_external::FileName;
use obsidian_external::Storage;
use obsidian_util::Retry;
use obsidian_util::WithBackground;
use tokio::time::sleep;

use crate::meta::MetaReader;
use crate::meta::MetaSynced;
use crate::meta::MetaSyncedSnapshot;
use crate::meta::TabletState;
use crate::runtime::Meta;
use crate::runtime::Shards;
use crate::Obsidian;

const GC_PRUNE_WAIT: Duration = Duration::from_mins(15);

/// [`Gc`] is the garbage collector for runs in storage.
///
/// As LSMs receive writes, they compact revisions into runs. This creates many unreachable runs,
/// which contain data that is often (mostly) redundant with the runs that the LSM is still using.
///
/// [`Gc`] finds and removes unreachable runs by repeatedly moving through three phases:
///
/// 1. Gather a list of all runs that exist in storage into a candidate set.
/// 2. Prune the candidate set by removing runs that are still live.
/// 3. Delete all runs in the candidate set.
///
/// There is some subtlety in the prune phase, since we need to be sure to distinguish between runs
/// that are not currently referenced because they have already been discarded, and runs that are
/// just yet to be referenced but about to be.
struct Gc(WithBackground<GcInner>);

struct GcInner {
    meta: Arc<dyn Meta>,
    meta_synced: Arc<MetaSynced>,
    shards: Arc<dyn Shards>,
    storage: Arc<dyn Storage>,

    gc_storage: GcStorage,
}

impl GcInner {
    async fn background_gc_cycle(&self) {
        loop {
            Retry::new()
                .indefinitely(&async move || self.try_next_gc_phase().await)
                .await;
        }
    }

    async fn try_next_gc_phase(&self) -> anyhow::Result<()> {
        match self.gc_storage.phase().await? {
            GcPhase::Gather => {
                self.gather().await?;
                self.gc_storage.transition_wait().await?;
            }
            GcPhase::Wait { start } => {
                let end = UNIX_EPOCH
                    .saturating_add(Duration::from_micros(start.as_micros()))
                    .saturating_add(GC_PRUNE_WAIT);
                let remaining = end
                    .duration_since(SystemTime::now())
                    .unwrap_or(Duration::ZERO);
                sleep(remaining).await;

                self.gc_storage.transition_prune().await?;
            }
            GcPhase::Prune => {
                self.prune().await?;
                self.gc_storage.transition_sweep().await?;
            }
            GcPhase::Sweep => {
                self.sweep().await?;
                self.gc_storage.transition_gather().await?;
            }
        }
        Ok(())
    }

    async fn gather(&self) -> anyhow::Result<()> {
        let mut s = self.storage.list();

        while let Some(file_name) = s.try_next().await? {
            let FileName::Run(run_id) = file_name;
            self.gc_storage.insert_candidate(run_id).await?;
        }

        Ok(())
    }

    async fn prune(&self) -> anyhow::Result<()> {
        let meta_snapshot = self.meta_synced.snapshot();
        let mut shard_ids = meta_snapshot.shard_ids().await?;

        while !shard_ids.is_empty() {
            let meta_snapshot = self.meta_synced.snapshot();
            let starting_tablet_ids = {
                let mut starting_tablet_ids = active_frozen_tablet_ids(&meta_snapshot).await?;
                starting_tablet_ids.retain(|tablet_id| shard_ids.contains(&tablet_id.0));
                starting_tablet_ids
            };

            self.prune_from(shard_ids.iter().copied()).await?;

            self.meta_synced
                .wait(self.meta.latest_snapshot().await?)
                .await?;
            let meta_snapshot = self.meta_synced.snapshot();
            let ending_tablet_ids = active_frozen_tablet_ids(&meta_snapshot).await?;

            // If a range moved from one tablet to another while we were pruning, it's possible we
            // saw neither the source nor the destination as being live on their respective shards.
            //
            // e.g. we get live_runs() from shard 1, then range moves from shard 2 to shard 1, then
            // we do live_runs() on shard 2.
            //
            // To avoid this race, we just see if the set of tablets changed between start and
            // finish and re-check the shards involved.
            let shards_with_possible_move = ending_tablet_ids
                .difference(&starting_tablet_ids)
                .map(|tablet_id| tablet_id.0);
            shard_ids = shards_with_possible_move.collect();
        }

        Ok(())
    }

    async fn prune_from(&self, shard_ids: impl Iterator<Item = ShardId>) -> anyhow::Result<()> {
        let mut futures: FuturesUnordered<_> = shard_ids
            .map(|shard_id| async move {
                Ok::<_, anyhow::Error>(self.shards.shard(shard_id)?.live_runs().await?)
            })
            .collect();

        while let Some(shard_live_runs) = futures.try_next().await? {
            for run_id in shard_live_runs {
                // TODO: Batch
                self.gc_storage.remove_candidate(run_id).await?;
            }
        }

        Ok(())
    }

    async fn sweep(&self) -> anyhow::Result<()> {
        let mut maybe_cursor = Some(ListCandidatesCursor::Start);
        while let Some(cursor) = maybe_cursor {
            let (page, next_cursor) = self.gc_storage.list_candidates_page(cursor).await?;
            for run_id in page {
                // TODO: Batch. We're going to have possibly millions of these to get through.
                self.storage.delete(FileName::Run(run_id)).await?;
                self.gc_storage.remove_candidate(run_id).await?;
            }
            maybe_cursor = next_cursor;
        }

        Ok(())
    }
}

/// Returns all of the tablets in the given snapshot that are in [`TabletState::Active`] or
/// [`TabletState::Frozen`].
async fn active_frozen_tablet_ids(
    meta_snapshot: &MetaSyncedSnapshot,
) -> anyhow::Result<BTreeSet<TabletId>> {
    let tablet_ids = meta_snapshot.tablet_ids().await?;
    let mut active_frozen_tablet_ids = BTreeSet::new();
    for tablet_id in tablet_ids {
        let tablet_metadata = meta_snapshot.tablet_metadata(tablet_id).await?;
        if matches!(
            tablet_metadata.state.current(),
            TabletState::Active | TabletState::Frozen,
        ) {
            active_frozen_tablet_ids.insert(tablet_id);
        }
    }
    Ok(active_frozen_tablet_ids)
}

enum GcPhase {
    /// Gather the list of candidates, that is, all of the runs that exist in storage.
    Gather,
    /// Pause between gathering and pruning for retention. The `live_runs` algorithm is safe even
    /// if the wait time is zero, but this wait is the minimum amount of time a run has to be
    /// considered 'dead' to be garbage collected.
    Wait { start: Timestamp },
    /// Prune the candidate list by removing runs that are still live, leaving only dead runs in
    /// the candidate list.
    Prune,
    /// Delete the dead runs from storage.
    Sweep,
}

enum ListCandidatesCursor {
    Start,
    Continue(RunId),
}

struct GcStorage {
    obsidian: Arc<dyn Obsidian>,
}

enum GcStorageKey {
    Phase(u8),
    Candidate(RunId),
}

impl GcStorageKey {
    fn encode(&self) -> Key {
        match self {
            GcStorageKey::Phase(pfx) => (KeyspaceId::INTERNAL_GC_PHASE, vec![*pfx]),
            GcStorageKey::Candidate(run_id) => (
                KeyspaceId::INTERNAL_GC_CANDIDATE,
                run_id.encode_fixed().to_vec(),
            ),
        }
    }
}

impl GcStorage {
    async fn phase(&self) -> anyhow::Result<GcPhase> {
        todo!();
    }

    /// Adds a candidate to the set for this cycle. Errors in phases other than [`GcPhase::Gather`].
    async fn insert_candidate(&self, run_id: RunId) -> anyhow::Result<()> {
        let pfx_key = GcStorageKey::Phase(run_id.encode_fixed()[0]).encode();
        let snapshot_ts = self
            .obsidian
            .latest_snapshot(BTreeSet::from([pfx_key.clone()]))
            .await?;
        let record = self.obsidian.get(snapshot_ts, &pfx_key).await?;
        self.obsidian
            .write(
                vec![],
                BTreeMap::from([
                    (pfx_key, Mutation::Put(vec![])),
                    (
                        GcStorageKey::Candidate(run_id).encode(),
                        Mutation::Put(vec![]),
                    ),
                ]),
            )
            .await?;
        todo!();
    }

    /// Removes a candidate from the set for this cycle. Errors in phases other than
    /// [`GcPhase::Prune`], [`GcPhase::Sweep`].
    async fn remove_candidate(&self, run_id: RunId) -> anyhow::Result<()> {
        todo!();
    }

    async fn list_candidates_page(
        &self,
        cursor: ListCandidatesCursor,
    ) -> anyhow::Result<(Vec<RunId>, Option<ListCandidatesCursor>)> {
        todo!();
    }

    /// Transitions to [`GcPhase::Wait`], erroring if not in [`GcPhase::Gather`].
    async fn transition_wait(&self) -> anyhow::Result<()> {
        todo!();
    }

    /// Transitions to [`GcPhase::Prune`], erroring if not in [`GcPhase::Wait`].
    async fn transition_prune(&self) -> anyhow::Result<()> {
        todo!();
    }

    /// Transitions to [`GcPhase::Sweep`], erroring if not in [`GcPhase::Prune`].
    async fn transition_sweep(&self) -> anyhow::Result<()> {
        todo!();
    }

    /// Transitions to [`GcPhase::Gather`], erroring if not in [`GcPhase::Sweep`] or if the
    /// candidate set is non-empty.
    async fn transition_gather(&self) -> anyhow::Result<()> {
        todo!();
    }
}

struct GcStorageSnapshot {
    ts: Timestamp,
    obsidian: Arc<dyn Obsidian>,
}

impl GcStorageSnapshot {}
