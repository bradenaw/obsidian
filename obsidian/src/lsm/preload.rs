use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;

use crate::lsm::index::Level;
use crate::lsm::memtable::Memtable;
use crate::lsm::IndexSnapshot;
use crate::lsm::Keyspace;
use crate::lsm::Run;
use crate::lsm::RunId;
use crate::runtime::Storage;
use crate::util::spawn_owned;
use crate::util::OwnedJoinHandle;
use crate::KeyspaceId;

pub(crate) struct Preloader<S>
where
    S: Storage,
{
    storage: Arc<S>,

    semaphore: Arc<tokio::sync::Semaphore>,
    runs: HashMap<RunId, (usize, PreloadRun)>,
    keyspaces: HashMap<KeyspaceId, usize>,
}

enum PreloadRun {
    Loading(OwnedJoinHandle<anyhow::Result<Run>>),
    Loaded(Run),
}

impl<S> Preloader<S>
where
    S: Storage,
{
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            semaphore: Arc::new(tokio::sync::Semaphore::new(8)),
            runs: HashMap::new(),
            keyspaces: HashMap::new(),
        }
    }

    pub fn add_keyspace(&mut self, keyspace_id: KeyspaceId, depth: usize) {
        self.keyspaces.insert(keyspace_id, depth);
    }

    pub fn queue(&mut self, run_id: RunId, level: usize) {
        self.runs.insert(
            run_id,
            (
                level,
                PreloadRun::Loading(spawn_owned({
                    let storage = Arc::clone(&self.storage);
                    let semaphore = Arc::clone(&self.semaphore);
                    async move {
                        let _permit = semaphore.acquire().await;
                        let file = storage.get(&run_id.to_string()).await?;
                        Run::open(file).await
                    }
                })),
            ),
        );
    }

    pub async fn load(&mut self) -> anyhow::Result<()> {
        for (_, (_, preload_run)) in self.runs.iter_mut() {
            if let PreloadRun::Loading(join_handle) = preload_run {
                let run = join_handle.await?;
                *preload_run = PreloadRun::Loaded(run);
            }
        }
        Ok(())
    }

    pub fn unload(&mut self, run_id: RunId) {
        self.runs.remove(&run_id);
    }

    pub async fn wait(self) -> anyhow::Result<Preloaded> {
        let mut snapshot = IndexSnapshot {
            keyspaces: HashMap::new(),
            splits: vec![],
        };

        for (keyspace_id, depth) in &self.keyspaces {
            snapshot.keyspaces.insert(
                *keyspace_id,
                Keyspace {
                    l0_active: Arc::new(Memtable::new()),
                    l0_sealed: Vec::new(),
                    levels: (0..*depth)
                        .into_iter()
                        .map(|_| Level { runs: Vec::new() })
                        .collect(),
                },
            );
        }
        for (run_id, (level, preload_run)) in self.runs.into_iter() {
            let run = match preload_run {
                PreloadRun::Loading(join_handle) => join_handle.await?,
                PreloadRun::Loaded(run) => run,
            };

            if let Some(keyspace) = snapshot.keyspaces.get_mut(&run.keyspace_id) {
                if level >= keyspace.levels.len() {
                    return Err(anyhow!(
                        "{:?} in {:?} at depth {} but keyspace only has depth {}",
                        run_id,
                        run.keyspace_id,
                        level,
                        keyspace.levels.len()
                    ));
                }
                keyspace.levels[level].runs.push(Arc::new(run));
            } else {
                return Err(anyhow!(
                    "{:?} in {:?} but keyspace never added to preloader",
                    run_id,
                    run.keyspace_id,
                ));
            }
        }
        for (_, keyspace) in &mut snapshot.keyspaces {
            for level in &mut keyspace.levels {
                level.runs.sort_by_key(|run| run.range().lower);
            }
        }

        Ok(Preloaded { snapshot })
    }

    async fn fetch(storage: Arc<S>, run_id: RunId) -> anyhow::Result<Run> {
        let file = storage.get(&run_id.to_string()).await?;
        Ok(Run::open(file).await?)
    }
}

pub(crate) struct Preloaded {
    pub(super) snapshot: IndexSnapshot,
}
