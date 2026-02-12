use std::array;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::anyhow;
use tokio::sync::Notify;

use crate::lsm::KeyspaceManifest;
use crate::lsm::LevelManifest;
use crate::lsm::Manifest;
use crate::lsm::Memtable;
use crate::lsm::Run;
use crate::lsm::RunId;
use crate::lsm::RunManifest;
use crate::runtime::FileReader;
use crate::runtime::Storage;
use crate::util::binary_search_by_idx;
use crate::Bound;
use crate::KeyOrBound;
use crate::KeyspaceId;
use crate::Range;
use crate::RangeMap;
use crate::RevisionValue;
use crate::Timestamp;
use crate::WalSeq;

const N_STRIPES: usize = 32;

/// The LSM index holds the set of memtables and runs that make up the current state of the LSM.
///
/// It is mutable, in three ways:
///
/// 1. l0_active receives new writes.
/// 2. rotate_l0() moves the current l0_active into l0_sealed and makes a new, empty l0_active.
/// 3. compactions replace some set of memtables/runs with new runs.
///
/// Because memtables are interior-mutable, nothing actually prevents writes to l0_sealed. It's the
/// caller's responsibility to guarantee (1) and (2), most easily by having the same thread
/// responsible for both tasks.
pub(super) struct Index<R> {
    // All of the elements of this array are separate arcs with clones of the same data inside.
    // Readers can choose any of them, writers must update all of them.
    //
    // This is done so that readers don't have to contend on the atomic inside the arc.
    current: [RwLock<Arc<IndexSnapshot<R>>>; N_STRIPES],
    updated: Notify,
}

impl<R> Index<R>
where
    R: FileReader,
{
    pub(super) fn new() -> Self {
        Self::from_snapshot(IndexSnapshot {
            keyspaces: HashMap::new(),
            splits: vec![],
        })
    }

    pub(super) fn from_snapshot(index_snapshot: IndexSnapshot<R>) -> Self {
        Self {
            current: array::from_fn(|_| RwLock::new(Arc::new(index_snapshot.clone()))),
            updated: Notify::new(),
        }
    }

    pub(super) async fn from_manifest<S>(storage: &S, manifest: Manifest) -> anyhow::Result<Self>
    where
        S: Storage<Reader = R>,
    {
        let index_snapshot = IndexSnapshot::from_manifest(storage, manifest).await?;

        Ok(Self::from_snapshot(index_snapshot))
    }

    /// Returns the current snapshot of the index state.
    pub(super) fn snapshot(&self) -> Arc<IndexSnapshot<R>> {
        let thread_id = std::thread::current().id().as_u64().get() as usize;
        Arc::clone(&self.current[thread_id % N_STRIPES].read().unwrap())
    }

    /// Returns the current snapshot of the index state, and a future that will complete if the
    /// snapshot changes.
    pub(super) fn snapshot_subscribe(
        &self,
    ) -> (Arc<IndexSnapshot<R>>, impl Future<Output = ()> + '_) {
        let notified = self.updated.notified();
        (self.snapshot(), notified)
    }

    pub(super) fn insert(
        &self,
        keyspace_id: KeyspaceId,
        seqno: WalSeq,
        k: Vec<u8>,
        ts: Timestamp,
        v: RevisionValue,
    ) -> anyhow::Result<u64> {
        let snapshot = &self.current[0].read().unwrap();
        Ok(snapshot
            .keyspaces
            .get(&keyspace_id)
            .ok_or_else(|| anyhow!("{:?} not found", keyspace_id))?
            .l0_active
            .insert(seqno, k, ts, v))
    }

    /// Moves l0_active into l0_sealed and creates a new, empty l0_active.
    pub(super) fn rotate_l0(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        self.update(|snapshot| {
            let keyspace = snapshot
                .keyspaces
                .get_mut(&keyspace_id)
                .ok_or_else(|| anyhow!("{:?} not found", keyspace_id))?;

            if keyspace.l0_active.is_empty() {
                return Ok(());
            }

            keyspace.l0_sealed.push(Arc::clone(&keyspace.l0_active));
            keyspace.l0_active = Arc::new(Memtable::new());

            Ok(())
        })
    }

    pub(super) fn create_keyspace(
        &self,
        keyspace_id: KeyspaceId,
        n_levels: usize,
    ) -> anyhow::Result<()> {
        if n_levels < 2 {
            return Err(anyhow!("not enough levels: {} < 2", n_levels));
        }

        self.update(|snapshot| {
            if snapshot.keyspaces.contains_key(&keyspace_id) {
                return Ok(());
            }
            let mut levels: Vec<Level<R>> = Vec::with_capacity(n_levels);
            for _ in 0..n_levels {
                levels.push(Level::new());
            }
            let keyspace = Keyspace {
                l0_active: Arc::new(Memtable::new()),
                l0_sealed: Vec::new(),
                levels,
            };
            snapshot.keyspaces.insert(keyspace_id, keyspace);
            Ok(())
        })
    }

    /// Adds the given runs into the index and removes the runs/memtables with the given run_ids.
    pub(super) fn replace(
        &self,
        keyspace_id: KeyspaceId,
        add: Vec<Run<R>>,
        min_level: usize,
        remove: HashSet<RunId>,
    ) -> anyhow::Result<()> {
        self.update(|snapshot| {
            let keyspace = snapshot
                .keyspaces
                .get_mut(&keyspace_id)
                .ok_or_else(|| anyhow!("{:?} not found", keyspace_id))?;
            keyspace.replace(add, min_level, remove)?;
            Ok(())
        })
    }

    pub(super) fn load(&self, new_snapshot: IndexSnapshot<R>) -> anyhow::Result<()> {
        self.update(|snapshot| {
            // TODO: Error out if the existing index isn't empty.
            *snapshot = new_snapshot;
            Ok(())
        })
    }

    pub(super) fn set_splits(&self, splits: Vec<Bound<Vec<u8>>>) {
        let _ = self.update(|snapshot| {
            snapshot.splits = splits;
            Ok(())
        });
    }

    fn update<F>(&self, f: F) -> anyhow::Result<()>
    where
        F: FnOnce(&mut IndexSnapshot<R>) -> anyhow::Result<()>,
    {
        let mut current: [_; N_STRIPES] = array::from_fn(|i| self.current[i].write().unwrap());
        let snapshot = &current[0];

        let mut new_snapshot = (***snapshot).clone();
        f(&mut new_snapshot)?;
        for slot in current.iter_mut() {
            **slot = Arc::new(new_snapshot.clone());
        }

        self.updated.notify_waiters();

        Ok(())
    }
}

pub(super) struct IndexSnapshot<R> {
    pub(super) keyspaces: HashMap<KeyspaceId, Keyspace<R>>,
    /// The compactor will split runs at these bounds and schedule compactions of all runs that
    /// contain any.
    ///
    /// That is, "eventually" the LSM will have no runs that cross any of these bounds.
    pub(super) splits: Vec<Bound<Vec<u8>>>,
}

impl<R> Clone for IndexSnapshot<R> {
    fn clone(&self) -> Self {
        Self { keyspaces: self.keyspaces.clone(), splits: self.splits.clone() }
    }
}

impl<R> IndexSnapshot<R>
where
    R: FileReader,
{
    async fn from_manifest<S>(storage: &S, manifest: Manifest) -> anyhow::Result<Self>
    where
        S: Storage<Reader = R>,
    {
        let mut keyspaces = HashMap::new();
        for (keyspace_id, keyspace_manifest) in manifest.keyspaces {
            let keyspace = Keyspace::from_manifest(storage, keyspace_manifest).await?;
            keyspaces.insert(keyspace_id, keyspace);
        }

        Ok(Self {
            keyspaces,
            splits: vec![],
        })
    }

    pub(super) fn manifest(&self) -> Manifest {
        Manifest {
            keyspaces: self
                .keyspaces
                .iter()
                .map(|(keyspace_id, keyspace)| (*keyspace_id, keyspace.manifest()))
                .collect(),
        }
    }
}

pub(super) struct Keyspace<R> {
    pub(super) l0_active: Arc<Memtable>,
    pub(super) l0_sealed: Vec<Arc<Memtable>>,
    // For ease of expression, levels[0] is always present but empty because it's represented above.
    pub(super) levels: Vec<Level<R>>,
}

impl<R> Clone for Keyspace<R> {
    fn clone(&self) -> Self {
        Self { l0_active: self.l0_active.clone(), l0_sealed: self.l0_sealed.clone(), levels: self.levels.clone() }
    }
}

impl<R> Keyspace<R>
where
    R: FileReader,
{
    async fn from_manifest<S>(storage: &S, manifest: KeyspaceManifest) -> anyhow::Result<Self>
    where
        S: Storage<Reader = R>,
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
                    range: memtable.range(),
                });
            }
            if !self.l0_active.is_empty() {
                l0_runs.push(RunManifest {
                    run_id: self.l0_active.id(),
                    range: self.l0_active.range(),
                });
            }
            level_manifests.push(LevelManifest { runs: l0_runs });
        }
        for level in &self.levels[1..] {
            let mut run_manifests = Vec::with_capacity(level.runs.len());

            for run in &level.runs {
                run_manifests.push(RunManifest {
                    run_id: run.id(),
                    range: run.range(),
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

    pub fn runs(&self) -> impl Iterator<Item = (usize, &Arc<Run<R>>)> {
        self.levels
            .iter()
            .enumerate()
            .map(|(i, level)| level.runs.iter().map(move |run| (i, run)))
            .flatten()
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

            // Insert into the lowest level we can reach without running into any intersection.
            for i in min_level..levels_maps.len() {
                if i == levels_maps.len() - 1
                    || levels_maps[i + 1]
                        .intersecting_ranges(&run_range)
                        .next()
                        .is_some()
                {
                    levels_maps[i].insert(run_range.clone(), Arc::new(run));
                    break;
                }
            }
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

pub(super) struct Level<R> {
    // In sorted order by range, guaranteed non-overlapping.
    pub(super) runs: Vec<Arc<Run<R>>>,
}

impl<R> Clone for Level<R> {
    fn clone(&self) -> Self {
        Self { runs: self.runs.clone() }
    }
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
