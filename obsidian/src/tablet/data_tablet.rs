use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::StreamExt;
use futures::TryStreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::lsm::Lsm;
use crate::lsm::Manifest;
use crate::meta::TabletState;
use crate::runtime::Shards;
use crate::runtime::Storage;
use crate::runtime::Tablet;
use crate::tablet::protected::LsmRead;
use crate::tablet::protected::LsmReadWrite;
use crate::tablet::protected::ProtectedLsm;
use crate::tablet::tablet_inner::PendingMutation;
use crate::tablet::tablet_inner::PrecondLocks;
use crate::tablet::tablet_inner::TabletInner;
use crate::tablet::tablet_journal_writer::TabletJournalWriter;
use crate::util::encode;
use crate::util::Decode;
use crate::util::Retry;
use crate::util::WithBackground;
use crate::Bound;
use crate::ColoGroupId;
use crate::Direction;
use crate::HistoryRange;
use crate::InternalError;
use crate::Key;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Precondition;
use crate::Range;
use crate::Record;
use crate::Revision;
use crate::RevisionValue;
use crate::TabletId;
use crate::Timestamp;
use crate::TxOutcome;
use crate::Txid;

const MAX_PRECOND_VALUE_LEN: usize = 256;

pub(crate) struct DataTablet(WithBackground<Arc<DataTabletInner>>);

struct DataTabletInner {
    inner: TabletInner,
    storage: Arc<dyn Storage>,
    shards: Arc<dyn Shards>,
    prepare_sender: mpsc::Sender<(Txid, KeyspaceId, Vec<u8>, PrepareType)>,
}

#[async_trait]
impl Tablet for DataTablet {
    async fn get_multi(
        &self,
        ts: Timestamp,
        keys: BTreeSet<Key>,
    ) -> Result<BTreeMap<Key, Record>, InternalError> {
        self.0.inner.get_multi(ts, keys).await
    }

    async fn get_latest_multi(
        &self,
        keys: BTreeSet<Key>,
    ) -> Result<(Timestamp, BTreeMap<Key, Record>), InternalError> {
        self.0.inner.get_latest_multi(keys).await
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
            TabletInner::check_write_conflicts(&lsm_rw, &preconds, &muts).await?
        {
            return Err(InternalError::Conflict(conflict_txid));
        }

        self.0.inner.check_preconds(&lsm_rw, &preconds).await?;

        let ts = self.0.inner.sequencer.start_write();

        let mut actual_muts = BTreeMap::new();

        for precond in &preconds {
            let keyspace_id = precond.keyspace_id().precond().ok_or_else(|| {
                anyhow::anyhow!(
                    "attempted prepare of non-data keyspace {:?}",
                    precond.keyspace_id()
                )
            })?;
            let value = TabletInner::unsafe_get_latest_record(&lsm_rw, keyspace_id, precond.key())
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

        lsm_rw.write(*ts, actual_muts);

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
        todo!();
    }

    async fn manifest(&self) -> anyhow::Result<Manifest> {
        Ok(self.0.inner.manifest())
    }

    async fn wait_mostly_hydrated(&self) -> anyhow::Result<()> {
        Err(anyhow!("DataTablet::wait_mostly_hydrated not allowed").into())
    }

    async fn catchup(&self) -> anyhow::Result<()> {
        Err(anyhow!("DataTablet::catchup not allowed").into())
    }

    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>> {
        self.0.find_split().await
    }
}

impl DataTablet {
    pub fn new(
        tablet_id: TabletId,
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
        lsm: Lsm,
        journal: Arc<dyn TabletJournalWriter>,
        storage: Arc<dyn Storage>,
        shards: Arc<dyn Shards>,
    ) -> Self {
        let (prepare_sender, prepare_receiver) = mpsc::channel(1024);

        let tablet = DataTablet(WithBackground::new(Arc::new(Arc::new(DataTabletInner {
            inner: TabletInner::new(
                tablet_id,
                colo_group_id,
                range,
                // Start out in Defunct because it has no permissions to do anything and we don't
                // actually know what we should be allowed to do until the meta sync finishes.
                //
                // TODO: remove indirect
                ProtectedLsm::new(tablet_id, lsm, TabletState::Active),
                journal,
            ),
            storage: storage,
            shards: shards,
            prepare_sender: prepare_sender.clone(),
        }))));

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

        tablet
    }

    pub fn tablet_id(&self) -> TabletId {
        self.0.inner.tablet_id
    }

    pub fn set_splits(&self, splits: Vec<Bound<Vec<u8>>>) {
        self.0.inner.lsm.set_splits(splits);
    }

    pub async fn flush(&self) -> anyhow::Result<()> {
        self.0.inner.lsm.flush().await
    }
}

impl DataTabletInner {
    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>> {
        self.inner.lsm.find_split().await
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
        let stream = ReceiverStream::new(receiver);
        stream
            .for_each_concurrent(
                Some(64),
                |(txid, keyspace_id, key, prepare_type)| async move {
                    Retry::new()
                        .indefinitely(&async || {
                            self.resolve_prepared_single(
                                txid,
                                keyspace_id,
                                key.clone(),
                                prepare_type,
                            )
                            .await
                        })
                        .await
                },
            )
            .await;
    }

    async fn resolve_prepared_single(
        &self,
        txid: Txid,
        keyspace_id: KeyspaceId,
        key: Vec<u8>,
        prepare_type: PrepareType,
    ) -> anyhow::Result<()> {
        let owner_tablet = self.shards.shard(txid.owner())?;
        let tx_outcome = match owner_tablet.tx_wait(txid).await {
            Ok(tx_outcome) => tx_outcome,
            // This implies that the other side already successfully cleaned this up by calling
            // cleanup_committed on us, so we don't need to do anything.
            Err(InternalError::TxOutcomeMissing) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        // Commits get cleaned up by the owner tablet calling cleanup_committed. Ignore them
        // here to avoid duplicating work.
        if let TxOutcome::Aborted = tx_outcome {
            let lsm_rw = self.inner.lsm.wait_read_write().await;
            match prepare_type {
                PrepareType::Precondition => {
                    self.cleanup_precond_key(&lsm_rw, txid, keyspace_id, key)
                        .await?;
                }
                PrepareType::Mutation => {
                    self.cleanup_pending_key(&lsm_rw, txid, tx_outcome, keyspace_id, key)
                        .await?;
                }
            }
        }
        Ok(())
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

        let _guard = self.inner.lock_mgr.write_lock(&key[..]).await;

        let (pending_ts, value) =
            match TabletInner::unsafe_get_latest_record(lsm_rw, pending_keyspace_id, &key).await? {
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
        if let TxOutcome::Committed(_) = tx_outcome {
            // Important: this guard protects against a race in scan. Without it, it would be
            // possible for a scan to observe neither this promoted record nor the pending record,
            // and elide this key entirely in its results: the scan reads the page of records, then
            // we come and clean up, and then the scan looks for conflicts, not finding any because
            // we already removed it.
            //
            // This guard guarantees that any concurrent scans complete before we remove the
            // pending record.
            let cleanup_guard = self.inner.scan_locks.cleanup();
            lsm_rw.write(
                resolve_ts,
                BTreeMap::from([((keyspace_id, key.clone()), m)]),
            );
            log::info!("cleanup_pending_key wait");
            cleanup_guard.wait().await;
            log::info!("cleanup_pending_key wait -> done");
        }
        lsm_rw.write(
            resolve_ts,
            BTreeMap::from([((pending_keyspace_id, key.clone()), Mutation::Delete)]),
        );
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
            TabletInner::unsafe_get_latest_record(lsm_rw, precond_keyspace_id, &key).await?
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
        lsm_rw.write(overwrite_ts, muts);
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(crate) enum PrepareType {
    Precondition,
    Mutation,
}
