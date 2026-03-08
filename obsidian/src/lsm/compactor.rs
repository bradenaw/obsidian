use std::cmp;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::future::Future;
use std::iter;
use std::pin::Pin;
use std::sync::Arc;

use async_stream::try_stream;
use futures::future::Either;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use rand::Rng;
use tokio::io::AsyncWriteExt;

use crate::lsm::index::Index;
use crate::lsm::index::IndexSnapshot;
use crate::lsm::index::Keyspace;
use crate::lsm::memtable::Memtable;
use crate::lsm::run::RunBuilder;
use crate::lsm::LsmRevision;
use crate::lsm::Run;
use crate::lsm::RunId;
use crate::runtime::Storage;
use crate::util::merge_sorted_streams;
use crate::util::spawn_owned;
use crate::util::WithBackground;
use crate::Bound;
use crate::KeyspaceId;
use crate::Range;
use crate::RevisionValue;
use crate::Timestamp;

/// The compactor's purpose is to mutate an `Index` to a more efficient physical represesentation,
/// but with the same logical content, as well as persisting from memory (where new writes go in
/// addition to the WAL) into storage so that tablets don't have to replay as much of the WAL on
/// startup.
pub(super) struct Compactor(WithBackground<CompactorInner>);

impl Compactor {
    pub(super) fn new(
        storage: Arc<dyn Storage>,
        index: Arc<Index>,
        concurrency: usize,
        run_size_target: u64,
        block_size_target: u64,
    ) -> Self {
        let bg = WithBackground::new(Arc::new(CompactorInner {
            index,
            storage,
            run_size_target,
            block_size_target,
        }));

        bg.spawn(async move |inner| {
            inner.schedule(concurrency).await;
        });

        Self(bg)
    }
}

struct CompactorInner {
    index: Arc<Index>,
    storage: Arc<dyn Storage>,
    run_size_target: u64,
    block_size_target: u64,
}

impl CompactorInner {
    async fn schedule(self: Arc<Self>, concurrency: usize) {
        let mut compact_futures = FuturesUnordered::new();
        let mut in_flight: HashSet<RunId> = HashSet::new();

        loop {
            let (snapshot, snapshot_changed) = self.index.snapshot_subscribe();

            while compact_futures.len() < concurrency {
                match self.schedule_next(&snapshot, &in_flight) {
                    Some((compaction, join_handle)) => {
                        in_flight.extend(compaction.run_ids.iter());

                        compact_futures.push(join_handle.map(|result| {
                            if let Err(e) = result {
                                log::error!("error in compaction: {:?}", e);
                            }
                            compaction
                        }));
                    }
                    None => {
                        break;
                    }
                }
            }

            tokio::select! {
                Some(compaction) = compact_futures.next() => {
                    for run_id in compaction.run_ids {
                        in_flight.remove(&run_id);
                    }
                },
                // This happens when compactions 'finish', though the tasks might not be done yet,
                // which we'll spuriously wake up for.
                //
                // But it also happens when l0 gets rotated, which might mean we need to spawn
                // another compaction.
                _ = snapshot_changed => {},
            }
        }
    }

    fn schedule_next(
        self: &Arc<Self>,
        snapshot: &Arc<IndexSnapshot>,
        in_flight: &HashSet<RunId>,
    ) -> Option<(Compaction, impl Future<Output = anyhow::Result<()>>)> {
        for (keyspace_id, keyspace) in &snapshot.keyspaces {
            if let Some(out) =
                self.schedule_next_keyspace(*keyspace_id, keyspace, &snapshot.splits, in_flight)
            {
                return Some(out);
            }
        }
        None
    }

    fn schedule_next_keyspace(
        self: &Arc<Self>,
        keyspace_id: KeyspaceId,
        keyspace: &Keyspace,
        splits: &[Bound<Vec<u8>>],
        in_flight: &HashSet<RunId>,
    ) -> Option<(Compaction, impl Future<Output = anyhow::Result<()>>)> {
        if let Some((compaction, future)) =
            self.schedule_for_splits(keyspace_id, keyspace, splits, in_flight)
        {
            return Some((compaction, Either::Left(future)));
        }

        if let Some((compaction, future)) =
            self.schedule_for_size(keyspace_id, keyspace, splits, in_flight)
        {
            return Some((compaction, Either::Right(Either::Left(future))));
        }

        if let Some((compaction, future)) =
            self.schedule_for_ingest(keyspace_id, keyspace, splits, in_flight)
        {
            return Some((compaction, Either::Right(Either::Right(future))));
        }

        // TODO: lmax compactions for garbage collection

        None
    }

    fn schedule_for_splits(
        self: &Arc<Self>,
        keyspace_id: KeyspaceId,
        keyspace: &Keyspace,
        splits: &[Bound<Vec<u8>>],
        in_flight: &HashSet<RunId>,
    ) -> Option<(Compaction, impl Future<Output = anyhow::Result<()>>)> {
        if splits.is_empty() {
            return None;
        }

        for (level_idx, run) in keyspace.runs() {
            if !splits.iter().any(|bound| run.range().contains_bound(bound)) {
                continue;
            }

            if let Some((compaction, future)) =
                self.try_schedule(keyspace_id, keyspace, splits, in_flight, level_idx, run)
            {
                return Some((compaction, future));
            }
        }

        None
    }

    fn schedule_for_size(
        self: &Arc<Self>,
        keyspace_id: KeyspaceId,
        keyspace: &Keyspace,
        splits: &[Bound<Vec<u8>>],
        in_flight: &HashSet<RunId>,
    ) -> Option<(Compaction, impl Future<Output = anyhow::Result<()>>)> {
        let level_size_estimates = {
            let mut level_size_estimates: Vec<_> =
                keyspace.levels.iter().map(|level| level.size()).collect();

            for i in 1..keyspace.levels.len() - 1 {
                for run in &keyspace.levels[i].runs {
                    if in_flight.contains(&run.id()) {
                        level_size_estimates[i] -= run.size();
                        level_size_estimates[i + 1] += run.size();
                    }
                }
            }

            level_size_estimates
        };

        // Prefer starting lower-level compactions first, since otherwise l0 compactions might
        // regularly lock up all of l1 and we might never be able to compact from l1.
        for i in (1..keyspace.levels.len() - 1).rev() {
            let level = &keyspace.levels[i];
            if level.runs.len() == 0 {
                continue;
            }
            if level_size_estimates[i] * 10 < level_size_estimates[i + 1] {
                continue;
            }

            // Start at a random position so we don't always e.g. choose the lowest run in sorted
            // order.
            let offset = rand::thread_rng().gen_range(0..level.runs.len());
            for j in 0..level.runs.len() {
                let run = &level.runs[(j + offset) % level.runs.len()];

                if let Some((compaction, future)) =
                    self.try_schedule(keyspace_id, keyspace, splits, in_flight, i, run)
                {
                    return Some((compaction, future));
                }
            }
        }

        None
    }

    fn try_schedule(
        self: &Arc<Self>,
        keyspace_id: KeyspaceId,
        keyspace: &Keyspace,
        splits: &[Bound<Vec<u8>>],
        in_flight: &HashSet<RunId>,
        level_idx: usize,
        run: &Arc<Run>,
    ) -> Option<(Compaction, impl Future<Output = anyhow::Result<()>>)> {
        if in_flight.contains(&run.id()) {
            return None;
        }

        if level_idx < keyspace.levels.len() - 1 {
            // If we're compacting anything but the lowest level, then we're compacting into the
            // next level down.
            let intersecting_runs = keyspace.levels[level_idx + 1].range(run.range());

            let run_ids = {
                let mut run_ids: HashSet<_> =
                    intersecting_runs.iter().map(|run| run.id()).collect();
                run_ids.insert(run.id());
                run_ids
            };

            let conflict = run_ids.iter().any(|run_id| in_flight.contains(&run_id));
            if conflict {
                return None;
            }

            return Some((
                Compaction {
                    keyspace_id,
                    from_level: level_idx,
                    run_ids,
                },
                Either::Left(self.compact_from(
                    keyspace_id,
                    splits,
                    level_idx,
                    run,
                    intersecting_runs,
                )),
            ));
        } else {
            let run_idx = keyspace.levels[level_idx]
                .runs
                .iter()
                .position(|other_run| other_run.id() == run.id())?;

            // If we're compacting the lowest level, then there's nothing to compact "into". We
            // compact the run along with its two neighbors since compaction can shrink a run via
            // garbage collection of unreachable revisions or by cleaving at a split point.
            let siblings = &keyspace.levels[level_idx].runs[run_idx.saturating_sub(1)
                ..cmp::min(run_idx + 1, keyspace.levels[level_idx].runs.len())];

            let conflict = siblings.iter().any(|run| in_flight.contains(&run.id()));

            if conflict {
                return None;
            }

            return Some((
                Compaction {
                    keyspace_id,
                    from_level: level_idx,
                    run_ids: siblings.iter().map(|run| run.id()).collect(),
                },
                Either::Right(self.compact_max(keyspace_id, splits, siblings)),
            ));
        }
    }

    fn schedule_for_ingest(
        self: &Arc<Self>,
        keyspace_id: KeyspaceId,
        keyspace: &Keyspace,
        splits: &[Bound<Vec<u8>>],
        in_flight: &HashSet<RunId>,
    ) -> Option<(Compaction, impl Future<Output = anyhow::Result<()>>)> {
        let l0_available: Vec<_> = keyspace
            .l0_sealed
            .iter()
            .filter(|memtable| !in_flight.contains(&memtable.id()))
            .cloned()
            .collect();
        if l0_available.is_empty() {
            return None;
        }

        // With random inserts, we'll always be compacting all of l0 into all of l1, so it's
        // all but guaranteed that all l0 compactions with conflict with each other.
        //
        // However, if inserts are mostly in sorted order, then we can compact multiple in
        // parallel.
        let l0_available_range =
            bounding_range(l0_available.iter().map(|memtable| memtable.range()));
        let l0_in_flight_range = bounding_range(
            keyspace
                .l0_sealed
                .iter()
                .filter(|memtable| in_flight.contains(&memtable.id()))
                .map(|memtable| memtable.range()),
        );

        // 1) These would end up conflicting later anyway.
        // 2) It would be incorrect to compact later writes for a single key into l1 before
        //    earlier. No intersecting ranges guarantees this.
        if l0_available_range
            .intersection(&l0_in_flight_range)
            .is_empty()
        {
            let intersecting_runs = keyspace.levels[1].range(l0_available_range);

            let run_ids: HashSet<_> = Iterator::chain(
                l0_available.iter().map(|memtable| memtable.id()),
                intersecting_runs.iter().map(|run| run.id()),
            )
            .collect();

            let conflict = run_ids.iter().any(|run_id| in_flight.contains(run_id));
            if !conflict {
                return Some((
                    Compaction {
                        keyspace_id,
                        from_level: 0,
                        run_ids,
                    },
                    self.compact_l0(keyspace_id, &splits, &l0_available[..], intersecting_runs),
                ));
            }
        }

        None
    }

    fn compact_l0(
        self: &Arc<Self>,
        keyspace_id: KeyspaceId,
        splits: &[Bound<Vec<u8>>],
        from: &[Arc<Memtable>],
        into: &[Arc<Run>],
    ) -> impl Future<Output = anyhow::Result<()>> {
        let remove: HashSet<_> = Iterator::chain(
            from.iter().map(|memtable| memtable.id()),
            into.iter().map(|run| run.id()),
        )
        .collect();

        log::trace!(
            "compacting l0 {:?} into {:?}",
            from.iter()
                .map(|memtable| memtable.id())
                .collect::<Vec<_>>(),
            into.iter().map(|run| run.id()).collect::<Vec<_>>(),
        );

        spawn_owned({
            let self_ = Arc::clone(self);
            let splits = splits.to_vec();
            let from = from.to_vec();
            let into = into.to_vec();
            async move {
                let add = self_.merge_l0(keyspace_id, &splits, &from, &into).await?;

                log::trace!(
                    "compacted l0 {:?} + {:?}, producing {:?}",
                    from.iter()
                        .map(|memtable| memtable.id())
                        .collect::<Vec<_>>(),
                    into.iter().map(|run| run.id()).collect::<Vec<_>>(),
                    add.iter().map(|run| run.id()).collect::<Vec<_>>(),
                );

                self_.index.replace(keyspace_id, add, remove)?;

                Ok(())
            }
        })
    }

    async fn merge_l0(
        &self,
        keyspace_id: KeyspaceId,
        splits: &[Bound<Vec<u8>>],
        memtables: &[Arc<Memtable>],
        runs: &[Arc<Run>],
    ) -> anyhow::Result<Vec<Run>> {
        let streams = {
            let mut streams = Vec::with_capacity(memtables.len() + 1);
            for memtable in memtables {
                streams.push(
                    futures::stream::iter(memtable.iter().map(|revision| Ok(revision))).boxed(),
                );
            }
            streams.push(
                futures::stream::iter(runs.iter().map(|run| run.stream()))
                    .flatten()
                    .boxed(),
            );

            streams
        };

        self.runs_from_revisions(keyspace_id, splits, merge_sorted_streams(streams))
            .await
    }

    fn compact_from(
        self: &Arc<Self>,
        keyspace_id: KeyspaceId,
        splits: &[Bound<Vec<u8>>],
        from_level: usize,
        from: &Arc<Run>,
        into: &[Arc<Run>],
    ) -> impl Future<Output = anyhow::Result<()>> {
        log::trace!(
            "compacting l{} {:?} into {:?}",
            from_level,
            from.id(),
            into.iter().map(|run| run.id()).collect::<Vec<_>>(),
        );

        let remove =
            Iterator::chain(iter::once(from.id()), into.iter().map(|run| run.id())).collect();

        spawn_owned({
            let from = Arc::clone(from);
            let into = into.to_vec();
            let splits = splits.to_vec();
            let self_ = Arc::clone(self);
            async move {
                let add = self_.merge_runs(keyspace_id, &splits, &from, &into).await?;
                log::trace!(
                    "compacted l{} {:?} + {:?}, producing {:?}",
                    from_level,
                    from.id(),
                    into.iter().map(|run| run.id()).collect::<Vec<_>>(),
                    add.iter().map(|run| run.id()).collect::<Vec<_>>(),
                );
                self_.index.replace(keyspace_id, add, remove)?;
                Ok(())
            }
        })
    }

    async fn merge_runs(
        &self,
        keyspace_id: KeyspaceId,
        splits: &[Bound<Vec<u8>>],
        a: &Run,
        b: &[Arc<Run>],
    ) -> anyhow::Result<Vec<Run>> {
        self.runs_from_revisions(
            keyspace_id,
            splits,
            merge_sorted_streams(vec![
                a.stream().boxed(),
                futures::stream::iter(b.iter().map(|run| run.stream()))
                    .flatten()
                    .boxed(),
            ]),
        )
        .await
    }

    fn compact_max(
        self: &Arc<Self>,
        keyspace_id: KeyspaceId,
        splits: &[Bound<Vec<u8>>],
        runs: &[Arc<Run>],
    ) -> impl Future<Output = anyhow::Result<()>> {
        log::trace!(
            "compacting lmax {:?}",
            runs.iter().map(|run| run.id()).collect::<Vec<_>>(),
        );

        let remove = runs.iter().map(|run| run.id()).collect();

        spawn_owned({
            let self_ = Arc::clone(self);
            let splits = splits.to_vec();
            let runs = runs.to_vec();
            async move {
                let add = self_
                    .runs_from_revisions(
                        keyspace_id,
                        &splits,
                        futures::stream::iter(runs.iter().map(|run| run.stream()))
                            .flatten()
                            .boxed(),
                    )
                    .await?;
                log::trace!(
                    "compacted lmax {:?}, producing {:?}",
                    runs.iter().map(|run| run.id()).collect::<Vec<_>>(),
                    add.iter().map(|run| run.id()).collect::<Vec<_>>(),
                );
                self_.index.replace(keyspace_id, add, remove)?;
                Ok(())
            }
        })
    }

    async fn runs_from_revisions(
        &self,
        keyspace_id: KeyspaceId,
        splits: &[Bound<Vec<u8>>],
        entries: impl Stream<Item = anyhow::Result<LsmRevision>> + Send,
    ) -> anyhow::Result<Vec<Run>> {
        let mut revs_by_key = group_by_key(entries.boxed()).boxed().peekable();
        let mut runs = Vec::new();

        // The current key is before the bound at splits[split_idx].
        let mut split_idx = 0;

        loop {
            if Pin::new(&mut revs_by_key).peek().await.is_none() {
                break;
            }

            let id = RunId::new();
            let mut writer = Box::pin(self.storage.put(&id.to_string()).await?);
            let mut run = RunBuilder::new(&mut writer, id, keyspace_id, self.block_size_target);

            while let Some((key, mut revs)) = Pin::new(&mut revs_by_key).next().await.transpose()? {
                while split_idx < splits.len() && splits[split_idx].cmp_key(&key) == Ordering::Less
                {
                    split_idx += 1;
                }

                for (ts, value) in revs.drain(..) {
                    run.push(LsmRevision {
                        key: key.clone(),
                        ts,
                        value,
                    })
                    .await?;
                }

                if let Some(Ok((key, revs))) =
                    Pin::new(&mut revs_by_key).peek().await.map(Result::as_ref)
                {
                    let next_size_estimate = (key.len()
                        + revs.iter().map(|(_, value)| 8 + value.len()).sum::<usize>())
                        as u64;
                    if run.size_estimate() + next_size_estimate > self.run_size_target {
                        break;
                    }

                    if split_idx < splits.len() && splits[split_idx].cmp_key(key) == Ordering::Less
                    {
                        break;
                    }
                }
            }

            run.finish().await?;
            writer.shutdown().await?;
            runs.push(Run::open(self.storage.get(&id.to_string()).await?).await?);
        }
        Ok(runs)
    }
}

fn group_by_key(
    mut entries: impl Stream<Item = anyhow::Result<LsmRevision>> + Send + Unpin,
) -> impl Stream<Item = anyhow::Result<(Vec<u8>, Vec<(Timestamp, RevisionValue)>)>> {
    try_stream! {
        let mut maybe_key: Option<Vec<u8>> = None;
        let mut revs = Vec::new();

        while let Some(rev) = entries.next().await.transpose()? {
            if let Some(prev_key) = maybe_key.take_if(|key| *key != rev.key) {
                yield (prev_key, std::mem::take(&mut revs));
            }
            if maybe_key.is_none() {
                maybe_key = Some(rev.key);
            }
            revs.push((rev.ts, rev.value));
        }

        if let Some(key) = maybe_key {
            yield (key, revs);
        }
    }
}

struct Compaction {
    keyspace_id: KeyspaceId,
    from_level: usize,
    run_ids: HashSet<RunId>,
}

/// Like a "bounding box", returns the minimal range that contains all of the given ranges.
fn bounding_range(ranges: impl Iterator<Item = Range<Vec<u8>>>) -> Range<Vec<u8>> {
    let mut lower = Bound::AfterAll;
    let mut upper = Bound::BeforeAll;
    for range in ranges {
        if range.is_empty() {
            continue;
        }
        lower = cmp::min(lower, range.lower);
        upper = cmp::max(upper, range.upper);
    }
    Range { lower, upper }
}
