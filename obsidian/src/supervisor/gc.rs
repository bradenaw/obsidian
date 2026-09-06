use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::anyhow;
use futures::stream::FuturesUnordered;
use futures::TryStreamExt;
use obsidian_common::Direction;
use obsidian_common::Key;
use obsidian_common::KeyspaceId;
use obsidian_common::Mutation;
use obsidian_common::Precondition;
use obsidian_common::Range;
use obsidian_common::RunId;
use obsidian_common::ShardId;
use obsidian_common::TabletId;
use obsidian_common::Timestamp;
use obsidian_external::FileName;
use obsidian_external::Storage;
use obsidian_pb as pb;
use obsidian_util::Decode;
use obsidian_util::Retry;
use obsidian_util::WithBackground;
use prost::Message as _;
use tokio::time::sleep;
use uuid::Uuid;

use crate::meta::MetaReader;
use crate::meta::MetaSynced;
use crate::meta::MetaSyncedSnapshot;
use crate::meta::TabletState;
use crate::runtime::Meta;
use crate::runtime::Shards;
use crate::Obsidian;

const GC_PRUNE_WAIT: Duration = Duration::from_mins(15);

/// [`StorageGc`] is the garbage collector for runs in storage.
///
/// As LSMs receive writes, they compact revisions into runs. This creates many unreachable runs,
/// which contain data that is often (mostly) redundant with the runs that the LSM is still using.
///
/// [`StorageGc`] finds and removes unreachable runs by repeatedly moving through three phases:
///
/// 1. Gather a list of all runs that exist in storage into a candidate set.
/// 2. Prune the candidate set by removing runs that are still live.
/// 3. Delete all runs in the candidate set.
///
/// There is some subtlety in the prune phase, since we need to be sure to distinguish between runs
/// that are not currently referenced because they have already been discarded, and runs that are
/// just yet to be referenced but about to be.
pub(crate) struct StorageGc(WithBackground<StorageGcInner>);

impl StorageGc {
    pub fn new(
        meta: Arc<dyn Meta>,
        meta_synced: Arc<MetaSynced>,
        shards: Arc<dyn Shards>,
        storage: Arc<dyn Storage>,
        obsidian: Arc<dyn Obsidian>,
    ) -> Self {
        let bg = WithBackground::new(StorageGcInner {
            meta,
            meta_synced,
            shards,
            storage,
            gc_storage: GcStorage { obsidian },
        });

        bg.spawn(async |inner| {
            inner.background_gc_cycle().await;
        });

        Self(bg)
    }
}

struct StorageGcInner {
    meta: Arc<dyn Meta>,
    meta_synced: Arc<MetaSynced>,
    shards: Arc<dyn Shards>,
    storage: Arc<dyn Storage>,

    gc_storage: GcStorage,
}

impl StorageGcInner {
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
        for pfx in 0..=255 {
            let mut maybe_cursor = Some(ListCandidatesCursor::Start(pfx));
            while let Some(cursor) = maybe_cursor {
                let (page, next_cursor) = self.gc_storage.list_candidates_page(cursor).await?;
                for run_id in page {
                    // TODO: Batch. We're going to have possibly millions of these to get through.
                    self.storage.delete(FileName::Run(run_id)).await?;
                    self.gc_storage.remove_candidate(run_id).await?;
                }
                maybe_cursor = next_cursor;
            }
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

#[derive(Clone, Debug)]
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

impl GcPhase {
    fn can_transition(&self, to: &GcPhase) -> bool {
        match self {
            GcPhase::Gather => matches!(to, GcPhase::Wait { .. }),
            GcPhase::Wait { .. } => matches!(to, GcPhase::Prune),
            GcPhase::Prune => matches!(to, GcPhase::Sweep),
            GcPhase::Sweep => matches!(to, GcPhase::Gather),
        }
    }
}

impl TryFrom<pb::internal::GcPhase> for GcPhase {
    type Error = anyhow::Error;

    fn try_from(value: pb::internal::GcPhase) -> Result<Self, Self::Error> {
        Ok(match value.phase.ok_or_else(|| anyhow!("missing phase"))? {
            obsidian_pb::internal::gc_phase::Phase::Gather(_) => Self::Gather,
            obsidian_pb::internal::gc_phase::Phase::Wait(wait) => Self::Wait {
                start: Timestamp::from_micros(wait.start),
            },
            obsidian_pb::internal::gc_phase::Phase::Prune(_) => Self::Prune,
            obsidian_pb::internal::gc_phase::Phase::Sweep(_) => Self::Sweep,
        })
    }
}

impl From<GcPhase> for pb::internal::GcPhase {
    fn from(value: GcPhase) -> Self {
        Self {
            phase: Some(match value {
                GcPhase::Gather => obsidian_pb::internal::gc_phase::Phase::Gather(()),
                GcPhase::Wait { start } => obsidian_pb::internal::gc_phase::Phase::Wait(
                    obsidian_pb::internal::gc_phase::Wait {
                        start: start.as_micros(),
                    },
                ),
                GcPhase::Prune => obsidian_pb::internal::gc_phase::Phase::Prune(()),
                GcPhase::Sweep => obsidian_pb::internal::gc_phase::Phase::Sweep(()),
            }),
        }
    }
}

impl Decode for GcPhase {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        GcPhase::try_from(pb::internal::GcPhase::decode(b)?)
    }
}

enum ListCandidatesCursor {
    Start(u8),
    Continue(u8, Range<Vec<u8>>),
}

struct GcStorage {
    obsidian: Arc<dyn Obsidian>,
}

#[derive(Eq, Hash, PartialEq)]
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
        let (_, _, phase) = self.get_or_init_phase(0).await?;
        Ok(phase)
    }

    /// Adds a candidate to the set for this cycle. Errors in phases other than [`GcPhase::Gather`].
    async fn insert_candidate(&self, run_id: RunId) -> anyhow::Result<()> {
        self.transact(run_id.encode_fixed()[0], async |phase, _| {
            if !matches!(phase, GcPhase::Gather) {
                return Err(anyhow!("cannot insert_candidate not in GcPhase::Gather"));
            }

            Ok(HashMap::from([(
                GcStorageKey::Candidate(run_id),
                Mutation::Put(vec![]),
            )]))
        })
        .await?;

        Ok(())
    }

    /// Removes a candidate from the set for this cycle. Errors in phases other than
    /// [`GcPhase::Prune`], [`GcPhase::Sweep`].
    async fn remove_candidate(&self, run_id: RunId) -> anyhow::Result<()> {
        self.transact(run_id.encode_fixed()[0], async |phase, _| {
            if !matches!(phase, GcPhase::Prune | GcPhase::Sweep) {
                return Err(anyhow!(
                    "cannot remove_candidate not in GcPhase::Prune or GcPhase::Sweep"
                ));
            }

            Ok(HashMap::from([(
                GcStorageKey::Candidate(run_id),
                Mutation::Put(vec![]),
            )]))
        })
        .await?;

        Ok(())
    }

    async fn list_candidates_page(
        &self,
        cursor: ListCandidatesCursor,
    ) -> anyhow::Result<(Vec<RunId>, Option<ListCandidatesCursor>)> {
        let (pfx, range) = match cursor {
            ListCandidatesCursor::Start(pfx) => (pfx, Range::prefix(vec![pfx])),
            ListCandidatesCursor::Continue(pfx, range) => (pfx, range),
        };

        let (snapshot_ts, _, _) = self.get_or_init_phase(pfx).await?;

        let (records, maybe_continue_cursor_raw) = self
            .obsidian
            .scan_page(
                snapshot_ts,
                KeyspaceId::INTERNAL_GC_CANDIDATE,
                range.borrow(),
                Direction::Asc,
                1000, // page_size
            )
            .await?;

        let run_ids = records
            .into_iter()
            .map(|record| -> anyhow::Result<_> {
                Ok(RunId::from(
                    Uuid::from_bytes_ref((&record.key.1[..]).try_into()?).clone(),
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        let maybe_continue_cursor =
            maybe_continue_cursor_raw.map(|range| ListCandidatesCursor::Continue(pfx, range));

        Ok((run_ids, maybe_continue_cursor))
    }

    /// Transitions to [`GcPhase::Wait`], erroring if not in [`GcPhase::Gather`].
    async fn transition_wait(&self) -> anyhow::Result<()> {
        self.transition(GcPhase::Wait {
            start: Timestamp::now(),
        })
        .await
    }

    /// Transitions to [`GcPhase::Prune`], erroring if not in [`GcPhase::Wait`].
    async fn transition_prune(&self) -> anyhow::Result<()> {
        self.transition(GcPhase::Prune).await
    }

    /// Transitions to [`GcPhase::Sweep`], erroring if not in [`GcPhase::Prune`].
    async fn transition_sweep(&self) -> anyhow::Result<()> {
        self.transition(GcPhase::Sweep).await
    }

    /// Transitions to [`GcPhase::Gather`], erroring if not in [`GcPhase::Sweep`].
    async fn transition_gather(&self) -> anyhow::Result<()> {
        self.transition(GcPhase::Gather).await
    }

    async fn transition(&self, target_phase: GcPhase) -> anyhow::Result<()> {
        let mut preconds = Vec::new();
        let mut muts = BTreeMap::new();
        for pfx in 0..255 {
            let (snapshot_ts, phase_key, phase) = self.get_or_init_phase(pfx).await?;

            if !phase.can_transition(&target_phase) {
                return Err(anyhow!(
                    "cannot transition from {:?} to {:?}",
                    phase,
                    target_phase
                ));
            }

            preconds.push(Precondition::NotChangedSince(
                phase_key.0,
                phase_key.1,
                snapshot_ts,
            ));
            muts.insert(
                GcStorageKey::Phase(pfx).encode(),
                Mutation::Put(pb::internal::GcPhase::from(target_phase.clone()).encode_to_vec()),
            );
        }

        self.obsidian.write(preconds, muts).await?;

        Ok(())
    }

    async fn get_phase(&self, pfx: u8) -> anyhow::Result<(Timestamp, Key, Option<GcPhase>)> {
        let phase_key = GcStorageKey::Phase(pfx).encode();
        let snapshot_ts = self
            .obsidian
            .latest_snapshot(BTreeSet::from([phase_key.clone()]))
            .await?;
        let maybe_phase_record = self.obsidian.get(snapshot_ts, &phase_key).await?;
        let maybe_phase = maybe_phase_record
            .map(|phase_record| GcPhase::decode(&phase_record.value))
            .transpose()?;

        Ok((snapshot_ts, phase_key, maybe_phase))
    }

    async fn get_or_init_phase(&self, pfx: u8) -> anyhow::Result<(Timestamp, Key, GcPhase)> {
        let (snapshot_ts, phase_key, maybe_phase) = self.get_phase(pfx).await?;

        if let Some(phase) = maybe_phase {
            return Ok((snapshot_ts, phase_key, phase));
        }

        let write_ts = self
            .obsidian
            .write(
                vec![Precondition::NotChangedSince(
                    phase_key.0,
                    phase_key.1.clone(),
                    snapshot_ts,
                )],
                BTreeMap::from([(
                    phase_key.clone(),
                    Mutation::Put(pb::internal::GcPhase::from(GcPhase::Gather).encode_to_vec()),
                )]),
            )
            .await?;

        Ok((write_ts, phase_key, GcPhase::Gather))
    }

    async fn transact(
        &self,
        pfx: u8,
        f: impl AsyncFnOnce(GcPhase, Timestamp) -> anyhow::Result<HashMap<GcStorageKey, Mutation>>,
    ) -> anyhow::Result<()> {
        let (snapshot_ts, phase_key, phase) = self.get_or_init_phase(pfx).await?;

        let mut muts = f(phase.clone(), snapshot_ts).await?;

        muts.insert(
            GcStorageKey::Phase(pfx),
            Mutation::Put(pb::internal::GcPhase::from(phase).encode_to_vec()),
        );

        self.obsidian
            .write(
                vec![Precondition::NotChangedSince(
                    phase_key.0,
                    phase_key.1,
                    snapshot_ts,
                )],
                muts.into_iter()
                    .map(|(storage_key, mutation)| (storage_key.encode(), mutation))
                    .collect(),
            )
            .await?;

        Ok(())
    }
}

struct GcStorageSnapshot {
    ts: Timestamp,
    obsidian: Arc<dyn Obsidian>,
}

impl GcStorageSnapshot {}
