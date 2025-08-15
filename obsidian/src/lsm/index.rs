use std::cmp;
use std::future::Future;
use std::sync::Arc;

use anyhow::anyhow;
use crossbeam::sync::ShardedLock;
use rand::Rng;

use crate::lsm::KeyspaceManifest;
use crate::lsm::LevelManifest;
use crate::lsm::Memtable;
use crate::lsm::Run;
use crate::lsm::RunManifest;
use crate::range::Bound;
use crate::range::KeyOrBound;
use crate::range::Range;
use crate::storage::FileReader;
use crate::types::RevisionValue;
use crate::types::Timestamp;
use crate::util::binary_search_by_idx;
use crate::wal;

struct Index<R: Clone> {
    // TODO: Readers are still going to contend a lot on the refcounts when cloning the snapshot.
    // Given that IndexSnapshots are relatively small, it's probably worth just pre-cloning it a
    // bunch of times so that threads can clone off of any of the copies.
    current: ShardedLock<IndexSnapshot<R>>,

    // TODO: This is obviously too heavy of a hammer, we only need compactions to conflict if
    // they're messing with the same range, as long as we remove doing stuff by indices.
    compaction_lock: tokio::sync::Mutex<()>,
}

impl<R> Index<R>
where
    R: FileReader + Clone,
{
    fn new(n_levels: usize) -> Self {
        if n_levels < 2 {
            panic!("not enough levels: {} < 2", n_levels);
        }
        let mut levels = Vec::with_capacity(n_levels);
        for _ in 0..n_levels {
            levels.push(Level::new());
        }
        Self {
            current: ShardedLock::new(IndexSnapshot {
                l0_active: Arc::new(Memtable::new()),
                l0_sealed: Vec::new(),
                levels,
            }),
            compaction_lock: tokio::sync::Mutex::new(()),
        }
    }

    fn from_manifest(manifest: KeyspaceManifest) -> Self {
        todo!();
    }

    fn snapshot(&self) -> IndexSnapshot<R> {
        self.current.read().unwrap().clone()
    }

    fn insert(&self, seqno: wal::SeqNo, k: Vec<u8>, ts: Timestamp, v: RevisionValue) {
        let curr = self.current.write().unwrap();
        curr.l0_active.insert(seqno, k, ts, v);
    }

    fn rotate_l0(&self) {
        let curr_snapshot = self.current.write().unwrap();
        if curr_snapshot.l0_active.is_empty() {
            return;
        }

        let mut new_snapshot = (*curr_snapshot).clone();
        new_snapshot
            .l0_sealed
            .push(Arc::clone(&curr_snapshot.l0_active));
        new_snapshot.l0_active = Arc::new(Memtable::new());
    }

    async fn compact_l0<F, Fut>(&self, f: F) -> anyhow::Result<()>
    where
        F: FnOnce(&[&Memtable], &[&Run<R>]) -> Fut,
        Fut: Future<Output = anyhow::Result<Vec<Run<R>>>>,
    {
        let _g = self.compaction_lock.lock().await;

        let snapshot = self.snapshot();
        if snapshot.l0_sealed.is_empty() {
            return Ok(());
        }

        let l0_sealed: Vec<_> = snapshot
            .l0_sealed
            .iter()
            .map(|arc_memtable| &**arc_memtable)
            .collect();

        let memtable_bounding_range =
            bounding_range(l0_sealed.iter().map(|memtable| memtable.range()));

        let (start_idx, end_idx) =
            snapshot.levels[1].intersecting_runs_idxs(memtable_bounding_range);

        let intersecting_runs: Vec<&Run<_>> = (&snapshot.levels[1].runs)[start_idx..end_idx]
            .iter()
            .map(|arc_run| &**arc_run)
            .collect();

        let new_runs = f(&l0_sealed, &intersecting_runs).await?;

        let mut new_snapshot = snapshot.clone();
        new_snapshot.l0_sealed = Vec::new();
        new_snapshot.levels[1]
            .runs
            .splice(start_idx..end_idx, new_runs.into_iter().map(Arc::new));

        let mut current = self.current.write().unwrap();
        *current = new_snapshot;

        Ok(())
    }

    async fn compact_from<F, Fut>(&self, level: usize, f: F) -> anyhow::Result<()>
    where
        F: FnOnce(&Run<R>, &[&Run<R>]) -> Fut,
        Fut: Future<Output = anyhow::Result<Vec<Run<R>>>>,
    {
        let _g = self.compaction_lock.lock().await;

        let snapshot = self.snapshot();

        if level == 0 {
            return Err(anyhow!("can't compact_from(0), use compact_l0()"));
        }
        if level >= snapshot.levels.len() {
            return Err(anyhow!(
                "can't compact_from a level past max levels: {} >= {}",
                level,
                snapshot.levels.len()
            ));
        }

        if snapshot.levels[level].runs.is_empty() {
            // Nothing to compact out of this level.
            return Ok(());
        }

        let idx = rand::thread_rng().gen_range(0..snapshot.levels[level].runs.len());
        let chosen = &snapshot.levels[level].runs[idx];

        if level == snapshot.levels.len() - 1 {}

        let (start_idx, end_idx) =
            snapshot.levels[level + 1].intersecting_runs_idxs(chosen.range());

        let intersecting_runs: Vec<&Run<_>> = (&snapshot.levels[level + 1].runs)
            [start_idx..end_idx]
            .iter()
            .map(|arc_run| &**arc_run)
            .collect();

        // TODO: if intersecting runs is empty we can just promote the run

        let new_runs = f(chosen, &intersecting_runs).await?;

        let mut new_snapshot = snapshot.clone();
        new_snapshot.levels[level].runs.remove(idx);
        new_snapshot.levels[level + 1]
            .runs
            .splice(start_idx..end_idx, new_runs.into_iter().map(Arc::new));

        let mut current = self.current.write().unwrap();
        *current = new_snapshot;

        Ok(())
    }
}

#[derive(Clone)]
struct IndexSnapshot<R: Clone> {
    // TODO: This is expensive to clone. Maybe internal Arc?
    l0_active: Arc<Memtable>,
    l0_sealed: Vec<Arc<Memtable>>,
    levels: Vec<Level<R>>,
}

impl<R> IndexSnapshot<R>
where
    R: FileReader + Clone,
{
    fn manifest(&self) -> KeyspaceManifest {
        let mut level_manifests = Vec::with_capacity(self.levels.len());
        for level in &self.levels {
            let mut run_manifests = Vec::with_capacity(level.runs.len());

            for run in &level.runs {
                run_manifests.push(RunManifest {
                    run_id: run.id(),
                    key_range: run.range(),
                });
            }

            level_manifests.push(LevelManifest {
                runs: run_manifests,
            });
        }

        KeyspaceManifest {
            levels: level_manifests,
        }
    }
}

#[derive(Clone)]
struct Level<R> {
    // In sorted order by range, guaranteed non-overlapping.
    runs: Vec<Arc<Run<R>>>,
}

impl<R: FileReader> Level<R> {
    fn new() -> Self {
        Self { runs: Vec::new() }
    }

    async fn get(
        &self,
        ts: Timestamp,
        k: &[u8],
    ) -> anyhow::Result<Option<(Timestamp, RevisionValue)>> {
        let run = match self.run_for_key(k) {
            Some(run) => run,
            None => return Ok(None),
        };
        run.get(ts, k).await
    }

    fn size(&self) -> usize {
        self.runs.iter().map(|run| run.size()).sum()
    }

    fn run_for_key<'a>(&'a self, k: &[u8]) -> Option<&'a Run<R>> {
        let idx = self
            .runs
            .binary_search_by_key(&KeyOrBound::Key(k.to_vec()), |run| {
                KeyOrBound::Bound(run.range().upper)
            })
            .unwrap_or_else(core::convert::identity);
        if idx >= self.runs.len() {
            return None;
        }
        let run = &self.runs[idx];
        if !run.range().contains(&k.to_vec()) {
            return None;
        }
        Some(run)
    }

    fn intersecting_runs_idxs(&self, range: Range<Vec<u8>>) -> (usize, usize) {
        let start_idx = match binary_search_by_idx(self.runs.len(), range.lower, |idx| {
            self.runs[idx].range().upper
        }) {
            Ok(idx) => idx + 1,
            Err(idx) => idx,
        };

        let end_idx = binary_search_by_idx(self.runs.len(), range.upper, |idx| {
            self.runs[idx].range().lower
        })
        .unwrap_or_else(core::convert::identity);

        (start_idx, end_idx)
    }
}

fn bounding_range(ranges: impl Iterator<Item = Range<Vec<u8>>>) -> Range<Vec<u8>> {
    let mut lower = Bound::AfterAll;
    let mut upper = Bound::BeforeAll;
    for range in ranges {
        lower = cmp::min(lower, range.lower);
        upper = cmp::max(upper, range.upper);
    }
    Range { lower, upper }
}
