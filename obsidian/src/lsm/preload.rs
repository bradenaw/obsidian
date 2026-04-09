use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use anyhow::anyhow;

use crate::lsm::index::IndexSnapshot;
use crate::lsm::index::Keyspace;
use crate::lsm::index::Level;
use crate::lsm::memtable::Memtable;
use crate::lsm::Manifest;
use crate::lsm::Run;
use crate::lsm::RunId;
use crate::runtime::FileName;
use crate::runtime::Storage;
use crate::util::spawn_owned;
use crate::util::OwnedJoinHandle;

pub(crate) struct Preloader {
    storage: Arc<dyn Storage>,

    semaphore: Arc<tokio::sync::Semaphore>,
    manifest: Manifest,
    runs: HashMap<RunId, PreloadRun>,
}

enum PreloadRun {
    Loading(OwnedJoinHandle<anyhow::Result<Run>>),
    Loaded(Run),
}

impl Preloader {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self {
            storage,
            semaphore: Arc::new(tokio::sync::Semaphore::new(8)),
            manifest: Manifest::empty(),
            runs: HashMap::new(),
        }
    }

    /// Sets the current manifest. Will cause the preloader to load all of the runs in this
    /// manifest in the background. Any previously-loaded runs that are not in this manifest will
    /// be unloaded.
    pub fn set_manifest(&mut self, manifest: Manifest) {
        let manifest_run_ids = manifest
            .runs()
            .map(|(_, _, run)| run.run_id)
            .collect::<HashSet<_>>();

        self.runs
            .retain(|run_id, _| manifest_run_ids.contains(run_id));

        for (_, _, run_manifest) in manifest.runs() {
            let run_id = run_manifest.run_id;
            if self.runs.contains_key(&run_id) {
                continue;
            }
            log::debug!("queued {:?} {:?} for preload", run_id, run_manifest.range);
            self.runs.insert(
                run_id,
                PreloadRun::Loading(spawn_owned({
                    let storage = Arc::clone(&self.storage);
                    let semaphore = Arc::clone(&self.semaphore);
                    async move {
                        let _permit = semaphore.acquire().await;
                        let file = storage.get(FileName::Run(run_id)).await?;
                        let run = Run::open(file).await;
                        log::debug!("{:?} finished preload", run_id);
                        run
                    }
                })),
            );
        }

        self.manifest = manifest;
    }

    /// Wait until all runs in the current manifest are loaded.
    pub async fn wait(&mut self) -> anyhow::Result<()> {
        for (_, preload_run) in self.runs.iter_mut() {
            if let PreloadRun::Loading(join_handle) = preload_run {
                let run = join_handle.await?;
                *preload_run = PreloadRun::Loaded(run);
            }
        }
        Ok(())
    }

    /// Wait until all runs in the current manifest are loaded and return a Preloaded which can be
    /// used to seed an LSM.
    pub async fn load(mut self) -> anyhow::Result<Preloaded> {
        let mut snapshot = IndexSnapshot {
            keyspaces: HashMap::new(),
            splits: vec![],
        };

        for (keyspace_id, keyspace_manifest) in &self.manifest.keyspaces {
            let mut keyspace = Keyspace {
                l0_active: Arc::new(Memtable::new()),
                l0_sealed: Vec::new(),
                levels: vec![],
            };

            for level_manifest in keyspace_manifest.levels() {
                let mut runs = vec![];
                for run_manifest in level_manifest.runs() {
                    let preload_run = self.runs.remove(&run_manifest.run_id).ok_or_else(|| {
                        anyhow!(
                            "manifest has run {:?} but missing from preload",
                            run_manifest.run_id
                        )
                    })?;
                    let run = match preload_run {
                        PreloadRun::Loading(join_handle) => join_handle.await?,
                        PreloadRun::Loaded(run) => run,
                    };
                    runs.push(Arc::new(run));
                }
                keyspace.levels.push(Level { runs });
            }
            snapshot.keyspaces.insert(*keyspace_id, keyspace);
        }

        Ok(Preloaded { snapshot })
    }

    async fn fetch(storage: Arc<dyn Storage>, run_id: RunId) -> anyhow::Result<Run> {
        let file = storage.get(FileName::Run(run_id)).await?;
        Ok(Run::open(file).await?)
    }
}

pub(crate) struct Preloaded {
    pub(super) snapshot: IndexSnapshot,
}
