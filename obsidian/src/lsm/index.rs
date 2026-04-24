use std::array;
use std::cmp::min;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::anyhow;
use tokio::sync::Notify;

use crate::lsm::memtable::Memtable;
use crate::lsm::run::Run;
use crate::lsm::KeyspaceManifest;
use crate::lsm::LevelManifest;
use crate::lsm::Manifest;
use crate::lsm::RunId;
use crate::lsm::RunManifest;
use crate::olf::OlfFile;
use crate::runtime::FileName;
use crate::runtime::Storage;
use crate::util::binary_search_by_idx;
use crate::Bound;
use crate::KeyOrBound;
use crate::KeyspaceId;
use crate::Range;
use crate::RangeMap;
use crate::RevisionValue;
use crate::Timestamp;

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
pub(super) struct Index {
    // All of the elements of this array are separate arcs with clones of the same data inside.
    // Readers can choose any of them, writers must update all of them.
    //
    // This is done so that readers don't have to contend on the atomic inside the arc.
    current: [RwLock<Arc<IndexSnapshot>>; N_STRIPES],
    updated: Notify,
}

impl Index {
    pub(super) fn new() -> Self {
        Self::from_snapshot(IndexSnapshot {
            keyspaces: HashMap::new(),
            splits: vec![],
        })
    }

    pub(super) fn from_snapshot(index_snapshot: IndexSnapshot) -> Self {
        Self {
            current: array::from_fn(|_| RwLock::new(Arc::new(index_snapshot.clone()))),
            updated: Notify::new(),
        }
    }

    pub(super) async fn from_manifest(
        storage: &dyn Storage,
        manifest: Manifest,
    ) -> anyhow::Result<Self> {
        let index_snapshot = IndexSnapshot::from_manifest(storage, manifest).await?;

        Ok(Self::from_snapshot(index_snapshot))
    }

    /// Returns the current snapshot of the index state.
    pub(super) fn snapshot(&self) -> Arc<IndexSnapshot> {
        let thread_id = std::thread::current().id().as_u64().get() as usize;
        Arc::clone(&self.current[thread_id % N_STRIPES].read().unwrap())
    }

    /// Returns the current snapshot of the index state, and a future that will complete if the
    /// snapshot changes.
    pub(super) fn snapshot_subscribe(&self) -> (Arc<IndexSnapshot>, impl Future<Output = ()> + '_) {
        let notified = self.updated.notified();
        (self.snapshot(), notified)
    }

    pub(super) fn insert(
        &self,
        keyspace_id: KeyspaceId,
        k: Vec<u8>,
        ts: Timestamp,
        v: RevisionValue,
    ) -> u64 {
        loop {
            {
                let snapshot = self.snapshot();
                if let Some(keyspace) = snapshot.keyspaces.get(&keyspace_id) {
                    return keyspace.l0_active.insert(k, ts, v);
                }
            }

            self.ensure_keyspace(keyspace_id);
        }
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
            keyspace.l0_active = Arc::new(Memtable::new(keyspace_id));

            Ok(())
        })
    }

    pub(super) fn ensure_keyspace(&self, keyspace_id: KeyspaceId) {
        let _ = self.update(|snapshot| {
            if snapshot.keyspaces.contains_key(&keyspace_id) {
                return Ok(());
            }
            let keyspace = Keyspace {
                l0_active: Arc::new(Memtable::new(keyspace_id)),
                l0_sealed: Vec::new(),
                levels: vec![Level::new(), Level::new()],
            };
            snapshot.keyspaces.insert(keyspace_id, keyspace);
            Ok(())
        });
    }

    /// Adds the given runs into the index and removes the runs/memtables with the given run_ids.
    pub(super) fn replace(
        &self,
        keyspace_id: KeyspaceId,
        add: Vec<Run>,
        remove: HashSet<RunId>,
    ) -> anyhow::Result<()> {
        self.update(|snapshot| {
            let keyspace = snapshot
                .keyspaces
                .get_mut(&keyspace_id)
                .ok_or_else(|| anyhow!("{:?} not found", keyspace_id))?;
            keyspace.replace(add, remove)?;
            Ok(())
        })
    }

    /// Increase the depth of the index by inserting an empty L1.
    pub(super) fn expand(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        self.update(|snapshot| {
            let keyspace = snapshot
                .keyspaces
                .get_mut(&keyspace_id)
                .ok_or_else(|| anyhow!("{:?} not found", keyspace_id))?;

            if keyspace.levels[1].runs.is_empty() {
                return Ok(());
            }

            keyspace.levels.insert(0, Level::new());
            log::trace!(
                "expanded {:?} to {} levels",
                keyspace_id,
                keyspace.levels.len()
            );

            Ok(())
        })
    }

    pub(super) fn load(&self, new_snapshot: IndexSnapshot) -> anyhow::Result<()> {
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
        F: FnOnce(&mut IndexSnapshot) -> anyhow::Result<()>,
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

#[derive(Clone)]
pub(super) struct IndexSnapshot {
    pub(super) keyspaces: HashMap<KeyspaceId, Keyspace>,
    /// The compactor will split runs at these bounds and schedule compactions of all runs that
    /// contain any.
    ///
    /// That is, "eventually" the LSM will have no runs that cross any of these bounds.
    pub(super) splits: Vec<Bound<Vec<u8>>>,
}

impl IndexSnapshot {
    pub(super) fn empty() -> Self {
        Self {
            keyspaces: HashMap::new(),
            splits: vec![],
        }
    }

    async fn from_manifest(storage: &dyn Storage, manifest: Manifest) -> anyhow::Result<Self> {
        let mut keyspaces = HashMap::new();
        for (keyspace_id, keyspace_manifest) in manifest.keyspaces {
            let keyspace = Keyspace::from_manifest(storage, keyspace_id, keyspace_manifest).await?;
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

#[derive(Clone)]
pub(super) struct Keyspace {
    pub(super) l0_active: Arc<Memtable>,
    pub(super) l0_sealed: Vec<Arc<Memtable>>,
    // For ease of expression, levels[0] is always present but empty because it's represented above.
    //
    // Always has at least two elements.
    pub(super) levels: Vec<Level>,
}

impl Keyspace {
    async fn from_manifest(
        storage: &dyn Storage,
        keyspace_id: KeyspaceId,
        manifest: KeyspaceManifest,
    ) -> anyhow::Result<Self> {
        let mut levels = Vec::with_capacity(manifest.levels().len());

        for level_manifest in manifest.levels() {
            let mut runs = Vec::with_capacity(level_manifest.runs().len());

            for run_manifest in level_manifest.runs() {
                let run = Run::new(
                    OlfFile::open(storage.get(FileName::Run(run_manifest.run_id)).await?).await?,
                );
                runs.push(Arc::new(run));
            }

            levels.push(Level { runs });
        }

        Ok(Self {
            l0_active: Arc::new(Memtable::new(keyspace_id)),
            l0_sealed: Vec::new(),
            levels,
        })
    }

    pub(super) fn manifest(&self) -> KeyspaceManifest {
        let mut level_manifests = Vec::with_capacity(self.levels.len());
        level_manifests.push(LevelManifest::empty());
        for level in &self.levels[1..] {
            let mut run_manifests = Vec::with_capacity(level.runs.len());

            for run in &level.runs {
                run_manifests.push(RunManifest {
                    run_id: run.run_id(),
                    range: run.range(),
                });
            }

            level_manifests.push(
                LevelManifest::new(run_manifests).expect("LSM didn't maintain Manifest invariants"),
            );
        }

        KeyspaceManifest::new(level_manifests).expect("LSM didn't maintain Manifest invariants")
    }

    pub fn runs(&self) -> impl Iterator<Item = (usize, &Arc<Run>)> {
        self.levels
            .iter()
            .enumerate()
            .map(|(i, level)| level.runs.iter().map(move |run| (i, run)))
            .flatten()
    }

    fn replace(&mut self, add: Vec<Run>, remove: HashSet<RunId>) -> anyhow::Result<()> {
        let mut removed = HashSet::new();

        let mut l0_sealed = Vec::with_capacity(self.l0_sealed.len());
        for memtable in &self.l0_sealed {
            if remove.contains(&memtable.run_id()) {
                removed.insert(memtable.run_id());
                continue;
            }

            l0_sealed.push(Arc::clone(memtable));
        }

        let mut min_level = 1;

        let mut levels_maps = Vec::new();
        for (i, level) in self.levels.iter().enumerate() {
            let mut level_map = RangeMap::new();
            for run in &level.runs {
                if remove.contains(&run.run_id()) {
                    removed.insert(run.run_id());
                    min_level = i;
                    continue;
                }
                level_map.insert(run.range(), Arc::clone(run));
            }
            levels_maps.push(level_map);
        }
        while levels_maps.len() < min_level + 1 {
            levels_maps.push(RangeMap::new());
        }

        min_level = min(min_level, self.levels.len() - 1);

        for run in add.into_iter() {
            let run_id = run.run_id();
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

#[derive(Clone)]
pub(super) struct Level {
    // In sorted order by range, guaranteed non-overlapping.
    pub(super) runs: Vec<Arc<Run>>,
}

impl Level {
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

    pub(super) fn run_for_key<'a>(&'a self, k: &[u8]) -> Option<&'a Run> {
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

    pub(super) fn range(&self, range: Range<Vec<u8>>) -> &[Arc<Run>] {
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
