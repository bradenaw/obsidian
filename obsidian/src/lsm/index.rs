use std::array;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use anyhow::anyhow;
use crossbeam::sync::ShardedLock;
use tokio::sync::Notify;

use crate::lsm::KeyspaceManifest;
use crate::lsm::LevelManifest;
use crate::lsm::Memtable;
use crate::lsm::Run;
use crate::lsm::RunId;
use crate::lsm::RunManifest;
use crate::range::KeyOrBound;
use crate::range::Range;
use crate::range::RangeMap;
use crate::storage::FileReader;
use crate::storage::Storage;
use crate::types::RevisionValue;
use crate::types::Timestamp;
use crate::util::binary_search_by_idx;
use crate::wal;

const N_STRIPES: usize = 32;

pub(super) struct Index<R> {
    // All of the elements of this array are separate arcs with clones of the same data inside.
    // Readers can choose any of them, writers must update all of them.
    //
    // This is done so that readers don't have to contend on the atomic inside the arc.
    current: ShardedLock<[Arc<IndexSnapshot<R>>; N_STRIPES]>,
    updated: Notify,
}

impl<R> Index<R>
where
    R: FileReader + Clone,
{
    pub(super) fn new(n_levels: usize) -> Self {
        if n_levels < 2 {
            panic!("not enough levels: {} < 2", n_levels);
        }
        let mut levels = Vec::with_capacity(n_levels);
        for _ in 0..n_levels {
            levels.push(Level::new());
        }
        let snapshot = IndexSnapshot {
            l0_active: Arc::new(Memtable::new()),
            l0_sealed: Vec::new(),
            levels,
        };

        Self::from_snapshot(snapshot)
    }

    pub(super) fn from_snapshot(index_snapshot: IndexSnapshot<R>) -> Self {
        Self {
            current: ShardedLock::new(array::from_fn(|_| Arc::new(index_snapshot.clone()))),
            updated: Notify::new(),
        }
    }

    pub(super) async fn from_manifest<S>(
        storage: &S,
        manifest: KeyspaceManifest,
    ) -> anyhow::Result<Self>
    where
        S: Storage<R = R>,
    {
        let index_snapshot = IndexSnapshot::from_manifest(storage, manifest).await?;

        Ok(Self::from_snapshot(index_snapshot))
    }

    pub(super) fn snapshot(&self) -> Arc<IndexSnapshot<R>> {
        let thread_id = std::thread::current().id().as_u64().get() as usize;
        Arc::clone(&self.current.read().unwrap()[thread_id % N_STRIPES])
    }

    pub(super) fn snapshot_subscribe(
        &self,
    ) -> (Arc<IndexSnapshot<R>>, impl Future<Output = ()> + '_) {
        let notified = self.updated.notified();
        (self.snapshot(), notified)
    }

    pub(super) fn insert(&self, seqno: wal::SeqNo, k: Vec<u8>, ts: Timestamp, v: RevisionValue) {
        let current = &self.current.write().unwrap()[0];
        current.l0_active.insert(seqno, k, ts, v);
    }

    pub(super) fn rotate_l0(&self) {
        let mut current = self.current.write().unwrap();
        let snapshot = &current[0];
        if snapshot.l0_active.is_empty() {
            return;
        }

        let mut new_snapshot = (**snapshot).clone();
        new_snapshot
            .l0_sealed
            .push(Arc::clone(&new_snapshot.l0_active));
        new_snapshot.l0_active = Arc::new(Memtable::new());
        *current = array::from_fn(|_| Arc::new(new_snapshot.clone()));

        self.updated.notify_waiters();
    }

    pub(super) fn replace(
        &self,
        add: Vec<Run<R>>,
        min_level: usize,
        remove: HashSet<RunId>,
    ) -> anyhow::Result<()> {
        let mut current = self.current.write().unwrap();
        let snapshot = &current[0];

        let mut new_snapshot = (**snapshot).clone();
        new_snapshot.replace(add, min_level, remove)?;
        *current = array::from_fn(|_| Arc::new(new_snapshot.clone()));

        self.updated.notify_waiters();

        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct IndexSnapshot<R> {
    pub(super) l0_active: Arc<Memtable>,
    pub(super) l0_sealed: Vec<Arc<Memtable>>,
    // For ease of expression, levels[0] is always present but empty because it's represented above.
    pub(super) levels: Vec<Level<R>>,
}

impl<R> IndexSnapshot<R>
where
    R: FileReader + Clone,
{
    async fn from_manifest<S>(storage: &S, manifest: KeyspaceManifest) -> anyhow::Result<Self>
    where
        S: Storage<R = R>,
    {
        if !manifest.levels[0].runs.is_empty() {
            return Err(anyhow!("manifest with non-empty l0"));
        }

        let mut levels = Vec::with_capacity(manifest.levels.len());

        for level_manifest in &manifest.levels {
            let mut runs = Vec::with_capacity(level_manifest.runs.len());

            for run_manifest in &level_manifest.runs {
                let run = Run::open(storage.get(&run_manifest.run_id.0.to_string()).await?).await?;
                runs.push(Arc::new(run));
            }

            levels.push(Level { runs });
        }

        Ok(Self {
            l0_active: Arc::new(Memtable::new()),
            l0_sealed: Vec::new(),
            levels,
        })
    }

    pub(super) fn manifest(&self) -> KeyspaceManifest {
        let mut level_manifests = Vec::with_capacity(self.levels.len());
        {
            let mut l0_runs = Vec::new();
            for memtable in &self.l0_sealed {
                l0_runs.push(RunManifest {
                    run_id: memtable.id(),
                });
            }
            if !self.l0_active.is_empty() {
                l0_runs.push(RunManifest {
                    run_id: self.l0_active.id(),
                });
            }
            level_manifests.push(LevelManifest { runs: l0_runs });
        }
        for level in &self.levels[1..] {
            let mut run_manifests = Vec::with_capacity(level.runs.len());

            for run in &level.runs {
                run_manifests.push(RunManifest {
                    run_id: run.id(),
                    //key_range: run.range(),
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

    fn replace(
        &mut self,
        add: Vec<Run<R>>,
        min_level: usize,
        remove: HashSet<RunId>,
    ) -> anyhow::Result<()> {
        if min_level == 0 {
            return Err(anyhow!(
                "min_level=0 implies adding to l0, which is not allowed"
            ));
        }

        let mut removed = HashSet::new();

        let mut l0_sealed = Vec::with_capacity(self.l0_sealed.len());
        for memtable in &self.l0_sealed {
            if remove.contains(&memtable.id()) {
                removed.insert(memtable.id());
                continue;
            }

            l0_sealed.push(Arc::clone(memtable));
        }

        let mut levels_maps = Vec::new();
        for level in &self.levels {
            let mut level_map = RangeMap::new();
            for run in &level.runs {
                if remove.contains(&run.id()) {
                    removed.insert(run.id());
                    continue;
                }
                level_map.insert(run.range(), Arc::clone(run));
            }
            levels_maps.push(level_map);
        }

        for run in add.into_iter() {
            let run_id = run.id();
            let run_range = run.range();

            if levels_maps[min_level]
                .intersecting_ranges(&run_range)
                .next()
                .is_some()
            {
                return Err(anyhow!(
                    "no room for {:?} {:?} in l{}",
                    run_id,
                    run_range,
                    min_level,
                ));
            }

            levels_maps[min_level].insert(run_range.clone(), Arc::new(run));

            // Insert into the lowest level we can reach without running into any intersection.
            //for i in min_level..levels_maps.len() {
            //    if i == levels_maps.len() - 1
            //        || levels_maps[i + 1].intersecting_ranges(&run_range).next().is_some()
            //    {
            //        levels_maps[i].insert(run_range.clone(), Arc::new(run));
            //        break;
            //    }
            //}
        }

        if removed.len() != remove.len() {
            return Err(anyhow!("not all runs to be removed were present"));
        }

        let levels = levels_maps
            .into_iter()
            .map(|level_map| Level {
                runs: level_map.into_iter().map(|(_, v)| v).collect(),
            })
            .collect();

        self.l0_sealed = l0_sealed;
        self.levels = levels;

        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct Level<R> {
    // In sorted order by range, guaranteed non-overlapping.
    pub(super) runs: Vec<Arc<Run<R>>>,
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

    pub(super) fn size(&self) -> usize {
        self.runs.iter().map(|run| run.size()).sum()
    }

    pub(super) fn run_for_key<'a>(&'a self, k: &[u8]) -> Option<&'a Run<R>> {
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

    pub(super) fn range(&self, range: Range<Vec<u8>>) -> &[Arc<Run<R>>] {
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

        &self.runs[start_idx..end_idx]
    }
}
