use std::cmp;
use std::collections::HashSet;
use std::future::Future;
use std::iter;
use std::pin::Pin;
use std::sync::Arc;

use futures::future;
use futures::future::Either;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::SinkExt;
use futures::Stream;
use futures::StreamExt;
use rand::Rng;

use crate::lsm::index::Index;
use crate::lsm::index::IndexSnapshot;
use crate::lsm::LsmRevision;
use crate::lsm::Memtable;
use crate::lsm::Run;
use crate::lsm::RunId;
use crate::range::Bound;
use crate::range::Range;
use crate::storage::Storage;
use crate::types::KeyspaceId;
use crate::util::merge_sorted_streams;
use crate::util::spawn_owned;
use crate::util::WithBackground;

/// The compactor's purpose is to mutate an `Index` to a more efficient physical represesentation,
/// but with the same logical content, as well as persisting from memory (where new writes go in
/// addition to the WAL) into storage so that tablets don't have to replay as much of the WAL on
/// startup.
pub(super) struct Compactor<S>(WithBackground<CompactorInner<S>>)
where
    S: Storage;

impl<S> Compactor<S>
where
    S: Storage + Send + Sync + 'static,
    S::R: 'static,
{
    pub(super) fn new(
        storage: Arc<S>,
        index: Arc<Index<S::R>>,
        concurrency: usize,
        run_size_target: u64,
        block_size_target: u64,
        keyspace_id: KeyspaceId,
    ) -> Self {
        let bg = WithBackground::new(Arc::new(CompactorInner {
            index,
            storage,
            run_size_target,
            block_size_target,
            keyspace_id,
        }));

        bg.spawn(async move |inner| {
            inner.schedule(concurrency).await;
        });

        Self(bg)
    }
}

struct CompactorInner<S>
where
    S: Storage,
{
    index: Arc<Index<S::R>>,
    storage: Arc<S>,
    run_size_target: u64,
    block_size_target: u64,
    keyspace_id: KeyspaceId,
}

impl<S> CompactorInner<S>
where
    S: Storage + Send + Sync + 'static,
    S::R: 'static,
{
    async fn schedule(self: Arc<Self>, concurrency: usize) {
        let mut compact_futures = FuturesUnordered::new();
        let mut in_flight_from: HashSet<RunId> = HashSet::new();
        let mut in_flight_into: HashSet<RunId> = HashSet::new();

        loop {
            let (snapshot, snapshot_changed) = self.index.snapshot_subscribe();

            while compact_futures.len() < concurrency {
                match self
                    .schedule_next(&snapshot, &in_flight_from, &in_flight_into)
                    .await
                {
                    Some((compaction, join_handle)) => {
                        in_flight_from.extend(compaction.from.iter());
                        in_flight_into.extend(compaction.into.iter());

                        compact_futures.push(join_handle.map(|result| {
                            if let Err(e) = result {
                                log::error!("error in compaction: {}", e);
                            }
                            compaction
                        }));
                    }
                    None => {
                        log::trace!("no new compactions to schedule");
                        break;
                    }
                }
            }

            tokio::select! {
                Some(compaction) = compact_futures.next() => {
                    for run_id in compaction.from {
                        in_flight_from.remove(&run_id);
                    }
                    for run_id in compaction.into {
                        in_flight_into.remove(&run_id);
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

    async fn schedule_next(
        self: &Arc<Self>,
        snapshot: &Arc<IndexSnapshot<S::R>>,
        in_flight_from: &HashSet<RunId>,
        in_flight_into: &HashSet<RunId>,
    ) -> Option<(Compaction, impl Future<Output = anyhow::Result<()>>)> {
        let level_size_estimates = {
            let mut level_size_estimates: Vec<_> =
                snapshot.levels.iter().map(|level| level.size()).collect();

            for i in 1..snapshot.levels.len() - 1 {
                for run in &snapshot.levels[i].runs {
                    if in_flight_from.contains(&run.id()) {
                        level_size_estimates[i] -= run.size();
                        level_size_estimates[i + 1] += run.size();
                    }
                }
            }

            level_size_estimates
        };

        // Prefer starting lower-level compactions first, since otherwise l0 compactions might
        // regularly lock up all of l1 and we might never be able to compact from l1.
        for i in (1..snapshot.levels.len() - 1).rev() {
            let level = &snapshot.levels[i];
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

                if in_flight_from.contains(&run.id()) {
                    continue;
                }
                if in_flight_into.contains(&run.id()) {
                    continue;
                }

                let intersecting_runs = snapshot.levels[i + 1].range(run.range());

                let into: HashSet<_> = intersecting_runs.iter().map(|run| run.id()).collect();
                let conflict = into.iter().any(|run_id| {
                    in_flight_from.contains(&run_id) || in_flight_into.contains(run_id)
                });

                if !conflict {
                    return Some((
                        Compaction {
                            from_level: i,
                            from: HashSet::from([run.id()]),
                            into,
                        },
                        Either::Left(self.compact_from(&snapshot, i, run, intersecting_runs)),
                    ));
                }
            }
        }

        let l0_available: Vec<_> = snapshot
            .l0_sealed
            .iter()
            .filter(|memtable| {
                !in_flight_from.contains(&memtable.id()) && !in_flight_into.contains(&memtable.id())
            })
            .cloned()
            .collect();
        if !l0_available.is_empty() {
            // With random inserts, we'll always be compacting all of l0 into all of l1, so it's
            // all but guaranteed that all l0 compactions with conflict with each other.
            //
            // However, if inserts are mostly in sorted order, then we can compact multiple in
            // parallel.
            let l0_available_range =
                bounding_range(l0_available.iter().map(|memtable| memtable.range()));
            let l0_in_flight_range = bounding_range(
                snapshot
                    .l0_sealed
                    .iter()
                    .filter(|memtable| in_flight_from.contains(&memtable.id()))
                    .map(|memtable| memtable.range()),
            );

            // 1) These would end up conflicting later anyway.
            // 2) It would be incorrect to compact later writes for a single key into l1 before
            //    earlier. No intersecting ranges guarantees this.
            if l0_available_range
                .intersection(&l0_in_flight_range)
                .is_empty()
            {
                let intersecting_runs = snapshot.levels[1].range(l0_available_range);

                let from: HashSet<_> = l0_available.iter().map(|memtable| memtable.id()).collect();
                let into: HashSet<_> = intersecting_runs.iter().map(|run| run.id()).collect();

                let conflict = Iterator::chain(from.iter(), into.iter()).any(|run_id| {
                    in_flight_from.contains(run_id) || in_flight_into.contains(run_id)
                });
                if !conflict {
                    return Some((
                        Compaction {
                            from_level: 0,
                            from,
                            into,
                        },
                        Either::Right(self.compact_l0(&l0_available[..], intersecting_runs)),
                    ));
                }
            }
        }

        // TODO: lmax compactions for garbage collection

        None
    }

    fn compact_l0(
        self: &Arc<Self>,
        from: &[Arc<Memtable>],
        into: &[Arc<Run<S::R>>],
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
            let from = from.to_vec();
            let into = into.to_vec();
            async move {
                let add = self_.merge_l0(&from[..], &into[..]).await?;

                log::trace!(
                    "compacted l0 {:?} + {:?}, producing {:?}",
                    from.iter()
                        .map(|memtable| memtable.id())
                        .collect::<Vec<_>>(),
                    into.iter().map(|run| run.id()).collect::<Vec<_>>(),
                    add.iter().map(|run| run.id()).collect::<Vec<_>>(),
                );

                self_.index.replace(add, 1, remove)?;

                Ok(())
            }
        })
    }

    async fn merge_l0(
        &self,
        memtables: &[Arc<Memtable>],
        runs: &[Arc<Run<S::R>>],
    ) -> anyhow::Result<Vec<Run<S::R>>> {
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

        self.runs_from_revisions(merge_sorted_streams(streams))
            .await
    }

    fn compact_from(
        self: &Arc<Self>,
        snapshot: &Arc<IndexSnapshot<S::R>>,
        from_level: usize,
        from: &Arc<Run<S::R>>,
        into: &[Arc<Run<S::R>>],
    ) -> impl Future<Output = anyhow::Result<()>> {
        if from_level == 0 {
            panic!("can't compact_from(0), use compact_l0()");
        }
        if from_level >= snapshot.levels.len() - 1 {
            panic!(
                "can't compact_from a level past max levels: {} >= {}",
                from_level,
                snapshot.levels.len() - 1
            );
        }

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
            let self_ = Arc::clone(self);
            async move {
                let add = self_.merge_runs(&from, &into[..]).await?;
                log::trace!(
                    "compacted l{} {:?} + {:?}, producing {:?}",
                    from_level,
                    from.id(),
                    into.iter().map(|run| run.id()).collect::<Vec<_>>(),
                    add.iter().map(|run| run.id()).collect::<Vec<_>>(),
                );
                self_.index.replace(add, from_level + 1, remove)?;
                Ok(())
            }
        })
    }

    async fn merge_runs(
        &self,
        a: &Run<S::R>,
        b: &[Arc<Run<S::R>>],
    ) -> anyhow::Result<Vec<Run<S::R>>> {
        self.runs_from_revisions(merge_sorted_streams(vec![
            a.stream().boxed(),
            futures::stream::iter(b.iter().map(|run| run.stream()))
                .flatten()
                .boxed(),
        ]))
        .await
    }

    async fn runs_from_revisions(
        &self,
        entries: impl Stream<Item = anyhow::Result<LsmRevision>> + Send,
    ) -> anyhow::Result<Vec<Run<S::R>>> {
        let mut sorted = entries.boxed().peekable();

        let mut runs = Vec::new();
        while let Some(_) = Pin::new(&mut sorted).peek().await {
            let mut curr_size = 0u64;
            let (mut tx, rx) = futures::channel::mpsc::channel(256);

            let id = RunId::new();
            let (mut writer, reader) = tokio::io::duplex(16384);
            future::try_join3(
                self.storage.put(&id.to_string(), reader),
                async {
                    Run::<()>::write(
                        &mut writer,
                        id,
                        self.keyspace_id,
                        self.block_size_target,
                        rx,
                    )
                    .await?;
                    drop(writer);
                    Ok(())
                },
                async {
                    while let Some(revision) = sorted.next().await.transpose()? {
                        let revision_size =
                            (revision.key.len() as u64) + 8 + (revision.value.len() as u64);
                        curr_size += revision_size;
                        let break_after = {
                            // All of the revisions for a single key need to end up in the same run, so once
                            // we've gone over the target size look for a break between keys.
                            if curr_size > self.run_size_target {
                                if let Some(Ok(next_revision)) = Pin::new(&mut sorted).peek().await
                                {
                                    if revision.key != next_revision.key {
                                        true
                                    } else {
                                        false
                                    }
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        };
                        tx.send(Ok(revision)).await?;
                        if break_after {
                            break;
                        }
                    }
                    drop(tx);

                    Ok(())
                },
            )
            .await?;

            let run = Run::open(self.storage.get(&id.to_string()).await?).await?;
            runs.push(run);
        }

        Ok(runs)
    }
}

struct Compaction {
    from_level: usize,
    from: HashSet<RunId>,
    into: HashSet<RunId>,
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
