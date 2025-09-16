use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::StreamExt;
use futures::TryStreamExt;
use tokio::sync::mpsc;

use crate::lsm::Lsm;
use crate::lsm::Manifest;
use crate::lsm::Preloader;
use crate::meta::MetaKey;
use crate::meta::MetaReader;
use crate::meta::MetaState;
use crate::meta::MetaSynced;
use crate::meta::MetaSyncedSnapshot;
use crate::meta::SyncType;
use crate::meta::TabletState;
use crate::obsidian::InternalError;
use crate::obsidian::Shards;
use crate::obsidian::TxOutcome;
use crate::obsidian::Txid;
use crate::storage::Storage;
use crate::tablet::protected::LsmRead;
use crate::tablet::protected::LsmReadWrite;
use crate::tablet::protected::ProtectedLsm;
use crate::tablet::tablet_inner::PendingMutation;
use crate::tablet::tablet_inner::PrecondLocks;
use crate::tablet::tablet_inner::TabletInner;
use crate::tablet::Tablet;
use crate::tablet::TabletId;
use crate::types::ColoGroupId;
use crate::types::Direction;
use crate::types::HistoryRange;
use crate::types::Key;
use crate::types::KeyspaceId;
use crate::types::Mutation;
use crate::types::Precondition;
use crate::types::Record;
use crate::types::Revision;
use crate::types::RevisionValue;
use crate::types::Timestamp;
use crate::util::encode;
use crate::util::spawn_owned;
use crate::util::Decode;
use crate::util::OwnedJoinHandle;
use crate::util::Retry;
use crate::util::WithBackground;
use crate::Bound;
use crate::Range;

const MAX_PRECOND_VALUE_LEN: usize = 256;

pub(crate) struct DataTablet<S: Storage>(WithBackground<DataTabletInner<S>>);

struct DataTabletInner<S: Storage> {
    inner: TabletInner<S>,
    meta_synced: Arc<MetaSynced>,
    storage: Arc<S>,
    shards: Arc<dyn Shards>,
    prepare_sender: mpsc::Sender<(Txid, KeyspaceId, Vec<u8>, PrepareType)>,

    // Only Some when the TabletState::Hydrating.
    hydration: Mutex<Option<Hydration>>,
}

struct Hydration {
    task: OwnedJoinHandle<()>,
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

#[async_trait]
impl<S: Storage> Tablet for DataTablet<S> {
    async fn get(&self, ts: Timestamp, key: &Key) -> Result<Option<Record>, InternalError> {
        self.0.inner.get(ts, key).await
    }

    async fn get_latest(&self, key: Key) -> Result<(Timestamp, Option<Record>), InternalError> {
        self.0.inner.get_latest(key).await
    }

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> Result<Timestamp, InternalError> {
        self.0.inner.latest_snapshot(keys).await
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<Range<Vec<u8>>>), InternalError> {
        self.0
            .inner
            .scan_page(ts, keyspace_id, range, direction, limit)
            .await
    }

    async fn history_page(
        &self,
        key: Key,
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Revision>, Option<HistoryRange>), InternalError> {
        self.0
            .inner
            .history_page(key, range, direction, limit)
            .await
    }

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        self.0.inner.write(preconds, muts).await
    }

    async fn prepare(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        let lsm_rw = self.0.inner.lsm.read_write()?;
        let _guard = self.0.inner.acquire_write_locks(&preconds, &muts).await?;

        if let Some(conflict_txid) =
            TabletInner::<S>::check_write_conflicts(&lsm_rw, &preconds, &muts).await?
        {
            return Err(InternalError::Conflict(conflict_txid));
        }

        let ts = self.0.inner.sequencer.start_write();

        let mut actual_muts = BTreeMap::new();

        for precond in &preconds {
            let keyspace_id = precond.keyspace_id().precond().ok_or_else(|| {
                anyhow::anyhow!(
                    "attempted prepare of non-data keyspace {:?}",
                    precond.keyspace_id()
                )
            })?;
            let value =
                TabletInner::<S>::unsafe_get_latest_record(&lsm_rw, keyspace_id, precond.key())
                    .await
                    .map_err(|e| InternalError::Other(e.into()))?
                    .map(|(_, v)| match v {
                        RevisionValue::Regular(v) => v,
                        RevisionValue::Tombstone => vec![],
                    })
                    .unwrap_or(vec![]);

            let mut precond_locks = PrecondLocks::decode(&value)?;
            precond_locks.txids.insert(txid);
            let new_value = encode(&precond_locks);

            if new_value.len() > MAX_PRECOND_VALUE_LEN {
                return Err(InternalError::Other(anyhow::anyhow!("too much contention")));
            }

            actual_muts.insert(
                (keyspace_id, precond.key().to_vec()),
                Mutation::Put(new_value),
            );
        }
        for ((keyspace_id, key), m) in &muts {
            let value = encode(&PendingMutation { txid, m: m.clone() });

            actual_muts.insert(
                (
                    keyspace_id.pending().ok_or_else(|| {
                        anyhow::anyhow!("attempted prepare of non-data keyspace {:?}", keyspace_id)
                    })?,
                    key.clone(),
                ),
                Mutation::Put(value),
            );
        }

        lsm_rw
            .write(*ts, preconds.clone(), actual_muts)
            .await
            .map_err(|e| InternalError::Other(e.into()))?;

        for precond in preconds {
            _ = self
                .0
                .prepare_sender
                .send((
                    txid,
                    precond.keyspace_id(),
                    precond.key().to_vec(),
                    PrepareType::Precondition,
                ))
                .await;
        }
        for ((keyspace_id, key), _) in muts {
            _ = self
                .0
                .prepare_sender
                .send((txid, keyspace_id, key, PrepareType::Mutation))
                .await;
        }

        Ok(*ts)
    }

    async fn try_commit(
        &self,
        _txid: Txid,
        _ts: Timestamp,
        _precond_keys: BTreeSet<Key>,
        _mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<TxOutcome> {
        Err(anyhow!("DataTablet::try_commit not allowed").into())
    }

    async fn try_abort(&self, _txid: Txid) -> anyhow::Result<TxOutcome> {
        Err(anyhow!("DataTablet::try_abort not allowed").into())
    }

    async fn wait(&self, _txid: Txid) -> Result<TxOutcome, InternalError> {
        Err(anyhow!("DataTablet::wait not allowed").into())
    }

    async fn cleanup_committed(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        self.0
            .cleanup_committed(txid, ts, precond_keys, mut_keys)
            .await
    }

    async fn wait_meta_sync(&self, ts: Timestamp) -> anyhow::Result<()> {
        self.0.meta_synced.wait(ts).await
    }

    async fn manifest(&self) -> anyhow::Result<Manifest> {
        Ok(self.0.inner.manifest())
    }

    async fn wait_mostly_hydrated(&self) -> anyhow::Result<()> {
        let mut state = {
            self.0
                .hydration
                .lock()
                .unwrap()
                .as_ref()
                .ok_or_else(|| anyhow!("hydration not in progress"))?
                .state
                .clone()
        };
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

    async fn catchup(&self) -> anyhow::Result<()> {
        let mut state = {
            let hydration_lock = self.0.hydration.lock().unwrap();
            let hydration = hydration_lock
                .as_ref()
                .ok_or_else(|| anyhow!("hydration not in progress"))?;

            hydration.set_state.send_modify(|value| {
                if matches!(value, HydrationState::Mostly) {
                    *value = HydrationState::Catchup;
                }
            });
            hydration.state.clone()
        };
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

    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>> {
        self.0.find_split().await
    }
}

impl<S: Storage> DataTablet<S> {
    pub async fn new(
        tablet_id: TabletId,
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
        lsm: Lsm<S>,
        meta_synced: Arc<MetaSynced>,
        storage: Arc<S>,
        shards: Arc<dyn Shards>,
    ) -> anyhow::Result<Self> {
        let (prepare_sender, prepare_receiver) = mpsc::channel(1024);

        lsm.create_keyspace(KeyspaceId::TX_OUTCOMES).await?;

        let inner = Arc::new(DataTabletInner {
            inner: TabletInner::new(
                tablet_id,
                colo_group_id,
                range,
                // Start out in Defunct because it has no permissions to do anything and we don't
                // actually know what we should be allowed to do until the meta sync finishes.
                ProtectedLsm::new(tablet_id, lsm, TabletState::Defunct),
            ),
            meta_synced: Arc::clone(&meta_synced),
            storage: storage,
            shards: shards,
            prepare_sender: prepare_sender.clone(),
            hydration: Mutex::new(None),
        });

        let tablet = DataTablet(WithBackground::new(Arc::clone(&inner)));

        tablet.0.spawn(async |inner| {
            inner.resolve_prepared(prepare_receiver).await;
        });

        tablet.0.spawn({
            let prepare_sender = prepare_sender.clone();
            async |inner| {
                inner.scan_for_pending_mutations(prepare_sender).await;
            }
        });

        tablet.0.spawn(async |inner| {
            inner.scan_for_precond_locks(prepare_sender).await;
        });

        {
            let inner = inner.clone();
            meta_synced
                .subscribe(move |sync_type, snapshot: MetaSyncedSnapshot| {
                    let inner = inner.clone();
                    async move {
                        inner.sync_meta(sync_type, snapshot).await;
                    }
                })
                .await;
        }

        Ok(tablet)
    }
}

impl<S> DataTabletInner<S>
where
    S: Storage,
{
    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>> {
        self.inner.lsm.find_split().await
    }

    pub(super) async fn sync_meta(
        self: &Arc<Self>,
        sync_type: SyncType,
        snapshot: MetaSyncedSnapshot,
    ) {
        Retry::new()
            .indefinitely(&async || -> anyhow::Result<()> {
                let sync_type = sync_type.clone();
                match sync_type {
                    SyncType::Initial => {
                        self.refresh_metadata(&snapshot).await?;

                        let tablet_metadata =
                            snapshot.tablet_metadata(self.inner.tablet_id).await?;
                        if matches!(
                            tablet_metadata.state,
                            MetaState::Stable(TabletState::Active),
                        ) {
                            for keyspace_id in snapshot.keyspace_ids().await? {
                                if keyspace_id.0 != self.inner.colo_group_id {
                                    continue;
                                }
                                self.inner.create_keyspace(keyspace_id).await?;
                            }
                        }
                    }
                    SyncType::Tx(meta_keys) => {
                        for meta_key in meta_keys {
                            match meta_key {
                                MetaKey::Keyspace(keyspace_id) => {
                                    if keyspace_id.0 != self.inner.colo_group_id {
                                        continue;
                                    }

                                    let tablet_metadata =
                                        snapshot.tablet_metadata(self.inner.tablet_id).await?;

                                    // TODO: There's a race here where we might drop a keyspace
                                    // creation on the floor if it's done during a transition. We
                                    // could make this graceful, but it'd be a lot easier to just
                                    // make keyspace creation wait until there's an active tablet
                                    // for every range.
                                    if matches!(
                                        tablet_metadata.state,
                                        MetaState::Stable(TabletState::Active),
                                    ) {
                                        self.inner.create_keyspace(keyspace_id).await?;
                                    }
                                }
                                MetaKey::Tablet(tablet_id) => {
                                    if tablet_id == self.inner.tablet_id {
                                        self.refresh_metadata(&snapshot).await?;
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
                Ok(())
            })
            .await;
    }

    async fn refresh_metadata(
        self: &Arc<Self>,
        snapshot: &MetaSyncedSnapshot,
    ) -> anyhow::Result<()> {
        let tablet_metadata = snapshot.tablet_metadata(self.inner.tablet_id).await?;

        let apparent_tablet_state = match tablet_metadata.state {
            MetaState::Stable(state) => state,
            MetaState::Transitioning(_, next_state) => next_state,
        };
        self.inner.lsm.set_state(apparent_tablet_state).await;

        let mut has_splits = false;

        if let Some(transfer_id) = tablet_metadata.transfer_id {
            let transfer_metadata = snapshot.transfer_metadata(transfer_id).await?;
            if matches!(apparent_tablet_state, TabletState::Hydrating) {
                let mut maybe_hydration = self.hydration.lock().unwrap();
                if matches!(maybe_hydration.deref(), None) {
                    log::info!(
                        "{:?} starting hydration for {:?}",
                        self.inner.tablet_id,
                        transfer_id
                    );
                    let (tx, rx) = tokio::sync::watch::channel(HydrationState::Started);
                    *maybe_hydration = Some(Hydration {
                        task: spawn_owned({
                            let self_ = Arc::clone(self);
                            let srcs = transfer_metadata.srcs.clone();
                            async move {
                                Retry::new()
                                    .indefinitely(&async || -> anyhow::Result<()> {
                                        self_.hydrate(&srcs[..]).await?;
                                        Ok(())
                                    })
                                    .await;
                            }
                        }),
                        set_state: tx,
                        state: rx,
                    });
                }
            }

            if transfer_metadata.srcs.contains(&self.inner.tablet_id)
                && transfer_metadata.dsts.len() > 1
            {
                let mut dst_ranges = vec![];
                for dst_tablet_id in transfer_metadata.dsts {
                    let dst_tablet_metadata = snapshot.tablet_metadata(dst_tablet_id).await?;
                    dst_ranges.push(dst_tablet_metadata.range);
                }

                has_splits = true;
                let splits = ranges_to_splits(dst_ranges)?;
                self.inner.lsm.set_splits(splits);
            }
        } else {
            let mut maybe_hydration = self.hydration.lock().unwrap();
            *maybe_hydration = None;
        }

        if !has_splits {
            self.inner.lsm.set_splits(vec![]);
        }

        if let TabletState::Frozen = apparent_tablet_state {
            self.inner.lsm.flush().await?;
        }

        Ok(())
    }

    // Scans for pending mutations that exist on disk already and delivers them to `sender`.
    async fn scan_for_pending_mutations(
        &self,
        sender: mpsc::Sender<(Txid, KeyspaceId, Vec<u8>, PrepareType)>,
    ) {
        let keyspace_ids = self.inner.lsm.wait_read().await.keyspaces();
        for keyspace_id in keyspace_ids {
            if !keyspace_id.is_pending() {
                continue;
            }

            Retry::new()
                .indefinitely(&async || -> anyhow::Result<()> {
                    let mut s = self
                        .inner
                        .scan_all(
                            self.inner.sequencer.safe_read_ts(),
                            keyspace_id,
                            self.inner.range.clone(),
                            Direction::Asc,
                        )
                        .boxed();
                    while let Some(record) = s.try_next().await? {
                        let pending = PendingMutation::decode(&record.value)?;

                        let _ = sender
                            .send((
                                pending.txid,
                                keyspace_id,
                                record.key.1,
                                PrepareType::Mutation,
                            ))
                            .await;
                    }
                    Ok(())
                })
                .await;
        }
    }

    // Scans for precond locks that exist on disk already and delivers them to `sender`.
    async fn scan_for_precond_locks(
        &self,
        sender: mpsc::Sender<(Txid, KeyspaceId, Vec<u8>, PrepareType)>,
    ) {
        let keyspace_ids = self.inner.lsm.wait_read().await.keyspaces();
        for keyspace_id in keyspace_ids {
            if !keyspace_id.is_precond() {
                continue;
            }

            Retry::new()
                .indefinitely(&async || -> anyhow::Result<()> {
                    let mut s = self
                        .inner
                        .scan_all(
                            self.inner.sequencer.safe_read_ts(),
                            keyspace_id,
                            self.inner.range.clone(),
                            Direction::Asc,
                        )
                        .boxed();
                    while let Some(record) = s.try_next().await? {
                        let precond_locks = PrecondLocks::decode(&record.value)?;
                        for txid in precond_locks.txids {
                            let _ = sender
                                .send((
                                    txid,
                                    keyspace_id,
                                    record.key.1.clone(),
                                    PrepareType::Precondition,
                                ))
                                .await;
                        }
                    }
                    Ok(())
                })
                .await;
        }
    }

    async fn resolve_prepared(
        &self,
        receiver: mpsc::Receiver<(Txid, KeyspaceId, Vec<u8>, PrepareType)>,
    ) {
        crate::util::bounded_unordered_for_each(
            receiver,
            64,
            |(txid, keyspace_id, key, prepare_type)| async move {
                let owner_tablet = self.shards.tablet(txid.owner()).unwrap();
                let tx_outcome = owner_tablet.wait(txid).await.unwrap();
                // Commits get cleaned up by the owner tablet calling cleanup_committed. Ignore them
                // here to avoid duplicating work.
                // TODO: retry instead of unwrap
                if let TxOutcome::Aborted = tx_outcome {
                    let lsm_rw = self.inner.lsm.wait_read_write().await;
                    match prepare_type {
                        PrepareType::Precondition => self
                            .cleanup_precond_key(&lsm_rw, txid, keyspace_id, key)
                            .await
                            .unwrap(),
                        PrepareType::Mutation => self
                            .cleanup_pending_key(&lsm_rw, txid, tx_outcome, keyspace_id, key)
                            .await
                            .unwrap(),
                    }
                }
            },
        )
        .await;
    }

    async fn cleanup_committed(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        let tx_outcome = TxOutcome::Committed(ts);
        let lsm_rw = self.inner.lsm.read_write()?;

        for (keyspace_id, key) in precond_keys {
            self.cleanup_precond_key(&lsm_rw, txid, keyspace_id, key)
                .await?;
        }
        for (keyspace_id, key) in mut_keys {
            self.cleanup_pending_key(&lsm_rw, txid, tx_outcome, keyspace_id, key)
                .await?;
        }

        Ok(())
    }

    async fn cleanup_pending_key<RW: LsmReadWrite>(
        &self,
        lsm_rw: &RW,
        txid: Txid,
        tx_outcome: TxOutcome,
        keyspace_id: KeyspaceId,
        key: Vec<u8>,
    ) -> anyhow::Result<()> {
        let pending_keyspace_id = keyspace_id.pending().ok_or_else(|| {
            anyhow::anyhow!("attempted cleanup of non-data keyspace {:?}", keyspace_id)
        })?;

        let mut muts = BTreeMap::new();
        let _guard = self.inner.lock_mgr.write_lock(&key[..]).await;

        let (pending_ts, value) =
            match TabletInner::<S>::unsafe_get_latest_record(lsm_rw, pending_keyspace_id, &key)
                .await?
            {
                Some((pending_ts, value)) => (pending_ts, value),
                None => return Ok(()),
            };
        let m = match value {
            RevisionValue::Regular(v) => {
                let pending_m = PendingMutation::decode(&v)?;
                if pending_m.txid != txid {
                    return Ok(());
                }
                pending_m.m
            }
            RevisionValue::Tombstone => return Ok(()),
        };
        let resolve_ts = match tx_outcome {
            TxOutcome::Committed(commit_ts) => {
                if commit_ts <= pending_ts {
                    return Err(anyhow!(
                        "commit_ts <= pending_ts: {} < {}",
                        commit_ts,
                        pending_ts
                    ));
                }
                commit_ts
            }
            TxOutcome::Aborted => Timestamp(pending_ts.0 + 1),
        };
        muts.insert((pending_keyspace_id, key.clone()), Mutation::Delete);
        if let TxOutcome::Committed(_) = tx_outcome {
            muts.insert((keyspace_id, key.clone()), m);
        }
        lsm_rw.write(resolve_ts, vec![], muts).await?;
        Ok(())
    }

    async fn cleanup_precond_key<RW: LsmReadWrite>(
        &self,
        lsm_rw: &RW,
        txid: Txid,
        keyspace_id: KeyspaceId,
        key: Vec<u8>,
    ) -> anyhow::Result<()> {
        let precond_keyspace_id = keyspace_id.precond().ok_or_else(|| {
            anyhow::anyhow!("attempted cleanup of non-data keyspace {:?}", keyspace_id)
        })?;

        let mut muts = BTreeMap::new();
        let _guard = self.inner.lock_mgr.write_lock(&key[..]).await;

        let (overwrite_ts, m) = if let Some((prepare_ts, RevisionValue::Regular(bytes))) =
            TabletInner::<S>::unsafe_get_latest_record(lsm_rw, precond_keyspace_id, &key).await?
        {
            let mut precond_locks = PrecondLocks::decode(&bytes)?;
            if precond_locks.txids.remove(&txid) {
                let m = if precond_locks.txids.is_empty() {
                    Mutation::Delete
                } else {
                    Mutation::Put(encode(&precond_locks))
                };

                (prepare_ts.plus_one(), m)
            } else {
                return Ok(());
            }
        } else {
            return Ok(());
        };
        muts.insert((precond_keyspace_id, key.clone()), m);
        lsm_rw.write(overwrite_ts, vec![], muts).await?;
        Ok(())
    }

    async fn hydrate(&self, srcs: &[TabletId]) -> anyhow::Result<()> {
        let mut preloader = Preloader::new(Arc::clone(&self.storage));
        let mut loaded = HashSet::new();

        let mut rounds_with_completed = 0;

        loop {
            let mut run_ids_seen = HashSet::new();
            // True if there aren't partially-overlapping runs, so that once we do preloader.load()
            // we have all of the data we were aware of.
            let mut complete = true;

            let done_after_round = matches!(
                *self
                    .hydration
                    .lock()
                    .unwrap()
                    .as_ref()
                    .ok_or_else(|| anyhow!("hydration cancelled"))?
                    .state
                    .borrow(),
                HydrationState::Catchup,
            );

            for src_id in srcs {
                let src = self.shards.tablet(*src_id)?;

                let manifest = src.manifest().await?;

                for (keyspace_id, keyspace_manifest) in &manifest.keyspaces {
                    preloader.add_keyspace(*keyspace_id, keyspace_manifest.levels.len());
                }

                for (_, level, run_manifest) in manifest.runs() {
                    if level == 0 {
                        continue;
                    }

                    if !self.inner.range.contains_range(&run_manifest.range) {
                        // If the run partially overlaps, compaction at the source will
                        // eventually make it not.
                        if self.inner.range.intersects(&run_manifest.range) {
                            log::debug!(
                                "{:?} hydration not complete because {:?} partially overlaps",
                                self.inner.tablet_id,
                                run_manifest.run_id,
                            );
                            complete = false;
                        }
                        continue;
                    }

                    run_ids_seen.insert(run_manifest.run_id);

                    if loaded.contains(&run_manifest.run_id) {
                        continue;
                    }

                    preloader.queue(run_manifest.run_id, level);
                    loaded.insert(run_manifest.run_id);
                    log::debug!(
                        "{:?} queued {:?} {:?} for hydration",
                        self.inner.tablet_id,
                        run_manifest.run_id,
                        run_manifest.range
                    );
                }
            }

            // Unload the runs that went away because of compaction.
            for run_id in loaded.extract_if(|run_id| !run_ids_seen.contains(run_id)) {
                preloader.unload(run_id);
            }

            if done_after_round && complete {
                break;
            }

            preloader.load().await?;

            if complete {
                rounds_with_completed += 1;
                if rounds_with_completed == 3 {
                    log::debug!(
                        "{:?} hydration transitioning to {:?}",
                        self.inner.tablet_id,
                        HydrationState::Mostly
                    );
                    self.hydration
                        .lock()
                        .unwrap()
                        .as_mut()
                        .ok_or_else(|| anyhow!("hydration cancelled"))?
                        .set_state
                        .send_modify(|value| {
                            if matches!(value, HydrationState::Started) {
                                *value = HydrationState::Mostly;
                            }
                        });
                }
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // TODO: Need to block compactions and only enable after we transition.
        self.inner.lsm.load()?.load(preloader.wait().await?).await?;
        let _ = self
            .hydration
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| anyhow!("hydration cancelled"))?
            .set_state
            .send(HydrationState::Done);

        Ok(())
    }
}

fn ranges_to_splits(mut ranges: Vec<Range<Vec<u8>>>) -> anyhow::Result<Vec<Bound<Vec<u8>>>> {
    ranges.sort_unstable_by(|a, b| Ord::cmp(&a.lower, &b.lower));
    let mut out = Vec::with_capacity(ranges.len() - 1);
    let ranges_len = ranges.len();
    for (i, range) in ranges.into_iter().enumerate() {
        if out.len() > 0 && out[out.len() - 1] != range.lower {
            return Err(anyhow!(
                "can't range_to_splits, ranges not contiguous: gap at {:?} {:?}",
                out[out.len() - 1],
                range.lower
            ));
        }
        if i < ranges_len - 1 {
            out.push(range.upper);
        }
    }
    Ok(out)
}

pub(crate) enum PrepareType {
    Precondition,
    Mutation,
}
