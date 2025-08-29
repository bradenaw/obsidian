use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use crate::lsm::index::Level;
use crate::lsm::memtable::Memtable;
use crate::lsm::IndexSnapshot;
use crate::lsm::Keyspace;
use crate::lsm::Run;
use crate::lsm::RunId;
use crate::storage::Storage;
use crate::util::spawn_owned;
use crate::util::OwnedJoinHandle;

pub(crate) struct Preloader<S>
where
    S: Storage,
{
    storage: Arc<S>,

    semaphore: Arc<tokio::sync::Semaphore>,
    runs: Mutex<HashMap<RunId, (usize, OwnedJoinHandle<anyhow::Result<Run<S::R>>>)>>,
}

impl<S> Preloader<S>
where
    S: Storage + Sync + Send + 'static,
{
    pub fn new(storage: Arc<S>) -> Self {
        Self{
            storage,
            semaphore: Arc::new(tokio::sync::Semaphore::new(8)),
            runs: Mutex::new(HashMap::new()),
        }
    }

    pub fn load(&self, run_id: RunId, level: usize) {
        let mut runs = self.runs.lock().unwrap();

        runs.insert(
            run_id,
            (
                level,
                spawn_owned({
                    let storage = Arc::clone(&self.storage);
                    let semaphore = Arc::clone(&self.semaphore);
                    async move {
                        let _permit = semaphore.acquire().await;
                        let file = storage.get(&run_id.to_string()).await?;
                        Run::open(file).await
                    }
                }),
            ),
        );
    }

    pub fn unload(&self, run_id: RunId) {
        self.runs.lock().unwrap().remove(&run_id);
    }

    pub async fn wait(self) -> anyhow::Result<Preloaded<S::R>> {
        let mut snapshot = IndexSnapshot {
            keyspaces: HashMap::new(),
        };

        let runs = {
            let mut runs_lock = self.runs.lock().unwrap();
            std::mem::take(&mut *runs_lock)
        };
        for (_, (level, join_handle)) in runs.into_iter() {
            let run = join_handle.await?;

            let keyspace = snapshot
                .keyspaces
                .entry(run.keyspace_id)
                .or_insert_with(|| Keyspace {
                    l0_active: Arc::new(Memtable::new()),
                    l0_sealed: Vec::new(),
                    levels: Vec::new(),
                });
            // TODO this doesn't actually guarantee we have the right depth
            while keyspace.levels.len() < level {
                keyspace.levels.push(Level { runs: Vec::new() });
            }
            keyspace.levels[level].runs.push(Arc::new(run));
        }
        for (_, keyspace) in &mut snapshot.keyspaces {
            for level in &mut keyspace.levels {
                level.runs.sort_by_key(|run| run.range().lower);
            }
        }

        Ok(Preloaded { snapshot })
    }

    async fn fetch(storage: Arc<S>, run_id: RunId) -> anyhow::Result<Run<S::R>> {
        let file = storage.get(&run_id.to_string()).await?;
        Ok(Run::open(file).await?)
    }
}

pub(crate) struct Preloaded<R> {
    pub(super) snapshot: IndexSnapshot<R>,
}
