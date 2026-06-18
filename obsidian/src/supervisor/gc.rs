use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::anyhow;
use futures::stream::FuturesUnordered;
use futures::TryStreamExt;
use obsidian_common::RunId;
use obsidian_common::TabletId;
use obsidian_external::FileName;
use obsidian_external::Storage;
use obsidian_util::Retry;
use obsidian_util::WithBackground;

use crate::meta::MetaReader;
use crate::meta::MetaSynced;
use crate::meta::MetaSyncedSnapshot;
use crate::meta::TabletState;
use crate::runtime::Meta;
use crate::runtime::Shards;

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
        let shard_ids = meta_snapshot.shard_ids().await?;
        let starting_tablet_ids = active_frozen_tablet_ids(&meta_snapshot).await?;

        let mut futures: FuturesUnordered<_> = shard_ids
            .iter()
            .map(|shard_id| async {
                Ok::<_, anyhow::Error>(self.shards.shard(*shard_id)?.live_runs().await?)
            })
            .collect();

        while let Some(shard_live_runs) = futures.try_next().await? {
            for run_id in shard_live_runs {
                // TODO: batch
                self.gc_storage.remove_candidate(run_id).await?;
            }
        }

        self.meta_synced
            .wait(self.meta.latest_snapshot().await?)
            .await?;
        let meta_snapshot = self.meta_synced.snapshot();
        let ending_tablet_ids = active_frozen_tablet_ids(&meta_snapshot).await?;

        let mut new_tablet_ids = ending_tablet_ids.difference(&starting_tablet_ids);
        if new_tablet_ids.next().is_some() {
            return Err(anyhow!("tablets changed during prune"));
        }

        // TODO: Need to make sure that no ranges moved tablets during this phase, else we may have
        // missed them.

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
    Gather,
    Prune,
    Sweep,
}

enum ListCandidatesCursor {
    Start,
}

struct GcStorage {}

impl GcStorage {
    async fn phase(&self) -> anyhow::Result<GcPhase> {
        todo!();
    }

    /// Adds a candidate to the set for this cycle. Errors in phases other than [`GcPhase::Gather`].
    async fn insert_candidate(&self, run_id: RunId) -> anyhow::Result<()> {
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

    /// Transitions to [`GcPhase::Prune`], erroring if not in [`GcPhase::Gather`].
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
