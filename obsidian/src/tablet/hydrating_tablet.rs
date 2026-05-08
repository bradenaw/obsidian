use std::collections::HashSet;
use std::ops::Deref as _;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::anyhow;
use obsidian_util::spawn_owned;
use obsidian_util::OwnedJoinHandle;

use crate::lsm::Lsm;
use crate::lsm::LsmOptions;
use crate::Manifest;
use crate::lsm::Preloader;
use crate::runtime::Shards;
use obsidian_external::Storage;
use crate::tablet::frozen_tablet::FrozenTablet;
use crate::tablet::read_only_lsm::ReadOnlyLsm;
use crate::tablet::TabletJournalWriter;
use crate::ColoGroupId;
use crate::KeyspaceId;
use crate::Range;
use crate::TabletId;

pub(super) struct HydratingTablet {
    inner: Arc<HydratingTabletInner>,
    lsm_options: LsmOptions,
    // Holds keyspace_ids that were created during hydration. Most of these will already have been
    // created on the source side and replicated to us during hydration, but if any are made
    // between when we finish hydrating (and no longer ask the sources for anything) and when we
    // transition to active, we need to make sure they exist.
    extra_keyspaces: Mutex<HashSet<KeyspaceId>>,
    task: OwnedJoinHandle<anyhow::Result<Preloader>>,
}

struct HydratingTabletInner {
    tablet_id: TabletId,
    colo_group_id: ColoGroupId,
    range: Range<Vec<u8>>,
    storage: Arc<dyn Storage>,
    shards: Arc<dyn Shards>,
    journal: Arc<dyn TabletJournalWriter>,
    manifest: Mutex<Manifest>,
    set_state: tokio::sync::watch::Sender<HydrationState>,
    state: tokio::sync::watch::Receiver<HydrationState>,
}

#[derive(Clone, Debug)]
enum HydrationState {
    // Hydration has been started but we might still have no data.
    Started,
    // We have most of the data, but the source(s) are still receiving writes, so even if we have
    // everything we know about it might not be everything.
    Mostly,
    // Source(s) are frozen, one more cycle will have everything.
    Catchup,
    // Cycle after 'catchup' finished.
    Done,
}

impl HydratingTablet {
    pub fn new(
        tablet_id: TabletId,
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
        lsm_options: LsmOptions,
        storage: Arc<dyn Storage>,
        shards: Arc<dyn Shards>,
        journal: Arc<dyn TabletJournalWriter>,
        srcs: Vec<TabletId>,
    ) -> Self {
        let (set_state, state) = tokio::sync::watch::channel(HydrationState::Started);
        let inner = Arc::new(HydratingTabletInner {
            tablet_id,
            colo_group_id,
            range,
            storage,
            shards,
            journal,
            manifest: Mutex::new(Manifest::empty()),
            set_state,
            state,
        });

        Self {
            task: spawn_owned({
                let inner = Arc::clone(&inner);
                async move { inner.hydrate(&srcs[..]).await }
            }),
            extra_keyspaces: Mutex::new(HashSet::new()),
            lsm_options,
            inner,
        }
    }

    pub fn tablet_id(&self) -> TabletId {
        self.inner.tablet_id
    }

    pub fn colo_group_id(&self) -> ColoGroupId {
        self.inner.colo_group_id
    }

    pub fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        if keyspace_id.0 != self.inner.colo_group_id {
            return Err(anyhow!(
                "cannot create {:?} in tablet for {:?}",
                keyspace_id,
                self.inner.colo_group_id
            ));
        }
        let mut extra_keyspaces = self.extra_keyspaces.lock().unwrap();
        extra_keyspaces.insert(keyspace_id);
        Ok(())
    }

    pub async fn finish(self) -> anyhow::Result<FrozenTablet> {
        let preloader = self.task.await?;
        let lsm = Lsm::open(
            self.lsm_options,
            Arc::clone(&self.inner.storage),
            preloader.load().await?,
        );
        let extra_keyspaces = {
            let mut guard = self.extra_keyspaces.lock().unwrap();
            std::mem::replace(guard.deref_mut(), HashSet::new())
        };
        for keyspace_id in extra_keyspaces.iter() {
            lsm.create_keyspace(*keyspace_id);
            if let Some(pending_keyspace_id) = keyspace_id.pending() {
                lsm.create_keyspace(pending_keyspace_id);
            }
            if let Some(precond_keyspace_id) = keyspace_id.precond() {
                lsm.create_keyspace(precond_keyspace_id);
            }
        }
        // TODO: Flush manifests to journal, else the whole thing can go poof.
        let data_tablet = FrozenTablet::new(
            self.inner.tablet_id,
            self.inner.colo_group_id,
            self.inner.range.clone(),
            ReadOnlyLsm::new(lsm).await,
            Arc::clone(&self.inner.storage),
            Arc::clone(&self.inner.shards),
        );
        Ok(data_tablet)
    }

    pub async fn manifest(&self) -> anyhow::Result<Manifest> {
        Ok(self.inner.manifest.lock().unwrap().clone())
    }

    pub async fn wait_mostly_hydrated(&self) -> anyhow::Result<()> {
        let mut state = self.inner.state.clone();
        loop {
            {
                let value = state.borrow_and_update();
                match value.deref() {
                    HydrationState::Started => {}
                    HydrationState::Mostly => return Ok(()),
                    other => return Err(anyhow!("hydration in unexpected state {:?}", other)),
                }
            }
            state.changed().await?;
        }
    }

    pub async fn catchup(&self) -> anyhow::Result<()> {
        self.inner.set_state.send_modify(|value| {
            if matches!(value, HydrationState::Mostly) {
                *value = HydrationState::Catchup;
            }
        });
        let mut state = self.inner.state.clone();
        loop {
            {
                let value = state.borrow_and_update();
                match value.deref() {
                    HydrationState::Catchup => {}
                    HydrationState::Done => return Ok(()),
                    other => return Err(anyhow!("hydration in unexpected state {:?}", other)),
                }
            }
            state.changed().await?;
        }
    }
}

impl HydratingTabletInner {
    async fn hydrate(&self, srcs: &[TabletId]) -> anyhow::Result<Preloader> {
        let mut preloader = Preloader::new(Arc::clone(&self.storage));

        let mut rounds_with_completed = 0;

        let mut src_manifests = Vec::with_capacity(srcs.len());
        for _ in 0..srcs.len() {
            src_manifests.push(Manifest::empty());
        }

        loop {
            // True if there aren't partially-overlapping runs, so that once we do preloader.load()
            // we have all of the data we were aware of.
            let mut complete = true;

            let done_after_round = matches!(*self.state.borrow(), HydrationState::Catchup);

            let mut any_changed = false;
            for (i, src_id) in srcs.iter().enumerate() {
                let src = self.shards.tablet(*src_id)?;
                let manifest = src.manifest().await?;

                for (_, _, run_manifest) in manifest.runs() {
                    if !self.range.contains_range(&run_manifest.range) {
                        // If the run partially overlaps, compaction at the source will
                        // eventually make it not.
                        if self.range.intersects(&run_manifest.range) {
                            log::debug!(
                                "{:?} hydration not complete because {:?} partially overlaps",
                                self.tablet_id,
                                run_manifest.run_id,
                            );
                            complete = false;
                        }
                        continue;
                    }
                }

                if src_manifests[i] != manifest {
                    src_manifests[i] = manifest;
                    any_changed = true;
                }
            }

            if any_changed {
                let merged_manifest = {
                    let mut merged_manifest = Manifest::empty();
                    for src_manifest in &src_manifests {
                        let mut manifest = src_manifest.clone();
                        manifest.clip(self.range.borrow());
                        merged_manifest = merged_manifest.merge(manifest)?;
                    }
                    merged_manifest
                };
                preloader.set_manifest(merged_manifest.clone());

                preloader.wait().await?;

                *self.manifest.lock().unwrap() = merged_manifest;
            }

            if done_after_round && complete {
                break;
            }

            if complete {
                rounds_with_completed += 1;
                if rounds_with_completed == 3 {
                    log::debug!(
                        "{:?} hydration transitioning to {:?}",
                        self.tablet_id,
                        HydrationState::Mostly
                    );
                    self.set_state.send_modify(|value| {
                        if matches!(value, HydrationState::Started) {
                            *value = HydrationState::Mostly;
                        }
                    });
                }
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        let _ = self.set_state.send(HydrationState::Done);

        Ok(preloader)
    }
}
