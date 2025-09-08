use std::cmp;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use anyhow::anyhow;
use async_stream::try_stream;
use futures::future;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use prost::Message;
use tokio::sync::mpsc;

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
use crate::obsidian::Router;
use crate::obsidian::Shards;
use crate::obsidian::TabletId;
use crate::obsidian::TxOutcome;
use crate::obsidian::Txid;
use crate::pb;
use crate::range::Range;
use crate::storage::Storage;
use crate::tablet::lock_mgr::Guard;
use crate::tablet::lock_mgr::LockMgr;
use crate::tablet::protected::LsmRead;
use crate::tablet::protected::LsmReadWrite;
use crate::tablet::protected::ProtectedLsm;
use crate::tablet::sequencer::Sequencer;
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
use crate::util::Encode;
use crate::util::OwnedJoinHandle;
use crate::util::Retry;

const MAX_PRECOND_VALUE_LEN: usize = 256;
const WAIT_ABORT_TIMEOUT: Duration = Duration::from_millis(1_000);

pub(super) struct TabletInner<S: Storage> {
    tablet_id: TabletId,
    colo_group_id: ColoGroupId,
    range: Range<Vec<u8>>,
    tablet_meta_range: Range<Vec<u8>>,

    lsm: ProtectedLsm<S>,
    sequencer: Sequencer,
    lock_mgr: LockMgr,

    prepare_sender: mpsc::Sender<(Txid, KeyspaceId, Vec<u8>, PrepareType)>,
    commit_sender: mpsc::Sender<(Txid, Timestamp, BTreeSet<Key>, BTreeSet<Key>)>,
    waiters: Waiters,

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

impl<S> TabletInner<S>
where
    S: Storage + Send + Sync + 'static,
{
    pub(super) fn new(
        tablet_id: TabletId,
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
        lsm: ProtectedLsm<S>,
        prepare_sender: mpsc::Sender<(Txid, KeyspaceId, Vec<u8>, PrepareType)>,
        commit_sender: mpsc::Sender<(Txid, Timestamp, BTreeSet<Key>, BTreeSet<Key>)>,
    ) -> Self {
        let tablet_meta_range = {
            let encoded_tablet_id = tablet_id.encode_fixed();
            Range::prefix(&encoded_tablet_id[..]).to_vec()
        };
        Self {
            tablet_id,
            colo_group_id,
            range,
            tablet_meta_range,
            lsm: lsm,
            prepare_sender,
            commit_sender,
            sequencer: Sequencer::new(),
            lock_mgr: LockMgr::new(1),
            waiters: Waiters::new(),
            hydration: Mutex::new(None),
        }
    }

    pub(super) async fn get(
        &self,
        ts: Timestamp,
        key: &Key,
    ) -> Result<Option<Record>, InternalError> {
        self.sequencer.wait_for_safe_read(ts).await?;

        let lsm_read = self.lsm.read()?;

        let keyspace_id = key.0;
        self.check_key(keyspace_id.0, &key.1)?;

        let key_future = lsm_read.get(ts, keyspace_id, &key.1);
        let (maybe_record, maybe_pending_value) = match keyspace_id.pending() {
            Some(pending_keyspace_id) => {
                future::try_join(key_future, lsm_read.get(ts, pending_keyspace_id, &key.1)).await?
            }
            None => (key_future.await?, None),
        };

        if let Some((_, RevisionValue::Regular(bytes))) = maybe_pending_value {
            let pending_mut = PendingMutation::decode(&bytes)?;
            return Err(InternalError::Conflict(pending_mut.txid));
        }

        Ok(match maybe_record {
            Some((ts, value)) => match value {
                RevisionValue::Regular(v) => Some(Record {
                    key: key.clone(),
                    ts: ts,
                    value: v,
                }),
                RevisionValue::Tombstone => None,
            },
            None => None,
        })
    }

    pub(super) async fn get_latest(
        &self,
        key: Key,
    ) -> Result<(Timestamp, Option<Record>), InternalError> {
        let lsm_read = self.lsm.read()?;

        let keyspace_id = key.0;
        self.check_key(keyspace_id.0, &key.1)?;

        let _guard = self.lock_mgr.read_lock(&key.1).await;

        let safe_read_ts = self.sequencer.safe_read_ts();

        let key_future = Self::unsafe_get_latest_record(&lsm_read, keyspace_id, &key.1);

        let (maybe_record, maybe_pending_value) = match keyspace_id.pending() {
            Some(pending_keyspace_id) => {
                future::try_join(
                    key_future,
                    Self::unsafe_get_latest_record(&lsm_read, pending_keyspace_id, &key.1),
                )
                .await?
            }
            None => (key_future.await?, None),
        };

        if let Some((_, RevisionValue::Regular(bytes))) = maybe_pending_value {
            let pending_mut = PendingMutation::decode(&bytes)?;
            return Err(InternalError::Conflict(pending_mut.txid));
        }

        Ok(match maybe_record {
            Some((ts, value)) => match value {
                RevisionValue::Regular(v) => (
                    ts,
                    Some(Record {
                        key: key,
                        ts: ts,
                        value: v,
                    }),
                ),
                RevisionValue::Tombstone => (ts, None),
            },
            None => (safe_read_ts, None),
        })
    }

    pub(super) async fn latest_snapshot(
        &self,
        keys: BTreeSet<Key>,
    ) -> Result<Timestamp, InternalError> {
        // TODO: This doesn't require loading the values, so we could optimize here to do less
        // work.
        let mut result = Timestamp::ZERO;
        for key in keys {
            let (ts, _) = self.get_latest(key).await?;
            result = cmp::max(result, ts);
        }
        Ok(result)
    }

    pub(super) async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<Range<Vec<u8>>>), InternalError> {
        if limit == 0 {
            return Err(anyhow!("scan_page limit=0").into());
        }
        let limit = cmp::min(limit, 1000);

        let scan_range = self.scan_range(keyspace_id.0, range, direction)?;
        self.sequencer.wait_for_safe_read(ts).await?;

        let lsm_read = self.lsm.read()?;

        // range                          |-----------|
        // self.range               |---------|
        // scan_range                     |---|


        // Ask the LSM for the page. Note that the returned continuation is in terms of the
        // constrained range that we asked it for, not the entire range from the request.
        let (page, intersecting_continue_cursor) = lsm_read
            .scan_page(ts, keyspace_id, scan_range.borrow(), direction, limit)
            .await?;
        let scanned_range = match intersecting_continue_cursor {
            Some(ref intersecting_continue_cursor) => match direction {
                Direction::Asc => Range {
                    lower: scan_range.lower,
                    upper: intersecting_continue_cursor.lower.clone(),
                },
                Direction::Desc => Range {
                    lower: intersecting_continue_cursor.upper.clone(),
                    upper: scan_range.upper,
                },
            },
            None => scan_range,
        };
        let continue_cursor = match direction {
            Direction::Asc => Range {
                lower: scanned_range.upper.clone(),
                upper: range.upper.to_vec(),
            },
            Direction::Desc => Range {
                lower: range.lower.to_vec(),
                upper: scanned_range.lower.clone(),
            },
        };

        // If we're looking at a userland keyspace, then we have to look for conflicts too.
        if let Some(pending_keyspace_id) = keyspace_id.pending() {
            let mut maybe_cursor = Some(scanned_range.clone());
            while let Some(cursor) = maybe_cursor {
                let (conflict_page, conflict_continue_cursor) = lsm_read
                    .scan_page(
                        ts,
                        pending_keyspace_id,
                        cursor.borrow(),
                        Direction::Asc,
                        1000,
                    )
                    .await?;

                // TODO: If we have more than x% of a page by the time we see a conflict, might be
                // better just to return it and hope that the conflict gets cleaned up by the time
                // the caller asks for the next page.
                for record in conflict_page {
                    if let RevisionValue::Regular(bytes) = record.value {
                        let pending_mut = PendingMutation::decode(&bytes)?;
                        return Err(InternalError::Conflict(pending_mut.txid));
                    }
                }
                maybe_cursor = conflict_continue_cursor;
            }
        }

        let maybe_continue_cursor = if continue_cursor.is_empty() {
            None
        } else {
            Some(continue_cursor)
        };

        Ok((
            page.into_iter()
                .filter_map(|revision| match revision.value {
                    RevisionValue::Regular(v) => Some(Record {
                        key: revision.key,
                        ts: revision.ts,
                        value: v,
                    }),
                    RevisionValue::Tombstone => None,
                })
                .collect(),
            maybe_continue_cursor,
        ))
    }

    pub(super) async fn history_page(
        &self,
        key: Key,
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Revision>, Option<HistoryRange>), InternalError> {
        let limit = cmp::min(limit, 1000);
        let keyspace_id = key.0;

        let range = match range {
            HistoryRange::Until(max) => {
                self.sequencer.wait_for_safe_read(max).await?;
                range
            }
            HistoryRange::Between(_, max) => {
                self.sequencer.wait_for_safe_read(max).await?;
                range
            }
            HistoryRange::All => {
                let max = self.latest_snapshot(BTreeSet::from([key.clone()])).await?;
                HistoryRange::Until(max)
            }
            HistoryRange::Since(min) => {
                let max = self.latest_snapshot(BTreeSet::from([key.clone()])).await?;
                HistoryRange::Between(min, max)
            }
        };

        let lsm_read = self.lsm.read()?;
        let _guard = self.lock_mgr.read_lock(&key.1).await;

        let (page, continue_cursor) = lsm_read
            .history_page(keyspace_id, &key.1, range, direction, limit)
            .await?;

        if let Some(pending_keyspace_id) = keyspace_id.pending() {
            let maybe_pending =
                Self::unsafe_get_latest_record(&lsm_read, pending_keyspace_id, &key.1).await?;

            if let Some((ts, RevisionValue::Regular(v))) = maybe_pending {
                if range.contains(ts) {
                    // TODO: we can constrain this a lot more - really we only need to surface a
                    // conflict if the page actually could have seen it, and we should be linearizing
                    // an unbounded upper just once on the first page
                    let pending_mut = PendingMutation::decode(&v)?;
                    return Err(InternalError::Conflict(pending_mut.txid));
                }
            }
        }

        Ok((
            page.into_iter()
                .map(|(ts, value)| Revision {
                    key: key.clone(),
                    ts: ts,
                    value: value,
                })
                .collect(),
            continue_cursor,
        ))
    }

    pub(super) async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        let lsm_rw = self.lsm.read_write()?;
        let _guard = self.acquire_write_locks(&preconds, &muts).await?;

        if let Some(conflict_txid) = Self::check_write_conflicts(&lsm_rw, &preconds, &muts).await? {
            return Err(InternalError::Conflict(conflict_txid));
        }

        let ts = self.sequencer.start_write();

        lsm_rw
            .write(*ts, preconds, muts)
            .await
            .map_err(|e| InternalError::Other(e.into()))?;

        Ok(*ts)
    }

    pub(super) async fn prepare(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        let lsm_rw = self.lsm.read_write()?;
        let _guard = self.acquire_write_locks(&preconds, &muts).await?;

        if let Some(conflict_txid) = Self::check_write_conflicts(&lsm_rw, &preconds, &muts).await? {
            return Err(InternalError::Conflict(conflict_txid));
        }

        let ts = self.sequencer.start_write();

        let mut actual_muts = BTreeMap::new();

        for precond in &preconds {
            let keyspace_id = precond.keyspace_id().precond().ok_or_else(|| {
                anyhow::anyhow!(
                    "attempted prepare of non-data keyspace {:?}",
                    precond.keyspace_id()
                )
            })?;
            let value = Self::unsafe_get_latest_record(&lsm_rw, keyspace_id, precond.key())
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
                .prepare_sender
                .send((txid, keyspace_id, key, PrepareType::Mutation))
                .await;
        }

        Ok(*ts)
    }

    pub(super) async fn try_commit(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<TxOutcome> {
        self.try_write_tx_outcome(
            txid,
            TxOutcomeRecord::Committed {
                ts,
                precond_keys,
                mut_keys,
            },
        )
        .await
    }

    pub(super) async fn try_abort(&self, txid: Txid) -> anyhow::Result<TxOutcome> {
        self.try_write_tx_outcome(txid, TxOutcomeRecord::Aborted)
            .await
    }

    pub(super) async fn wait(&self, txid: Txid) -> Result<TxOutcome, InternalError> {
        let tx_outcome_key = txid.encode_fixed();
        self.check_key(KeyspaceId::TX_OUTCOMES.0, &tx_outcome_key[..])?;
        loop {
            let wait = {
                let lsm_read = self.lsm.read()?;
                let _guard = self.lock_mgr.read_lock(&tx_outcome_key[..]).await;

                match Self::unsafe_get_latest_record(
                    &lsm_read,
                    KeyspaceId::TX_OUTCOMES,
                    &tx_outcome_key[..],
                )
                .await?
                {
                    Some((_, RevisionValue::Regular(tx_outcome_bytes))) => {
                        let tx_outcome_record: TxOutcomeRecord =
                            pb::internal::TxOutcomeRecord::decode(&tx_outcome_bytes[..])
                                .map_err(|e| InternalError::Other(e.into()))?
                                .try_into()?;
                        return Ok(tx_outcome_record.tx_outcome());
                    }
                    // Must be done with _guard still active.
                    None => self.waiters.wait(txid),
                    _ => {
                        // TODO: This should only happen when the pending records have already been
                        // cleaned up, so we should return a specific error to tell the caller to
                        // just retry whatever they were trying to do.
                        return Err(InternalError::TxOutcomeMissing);
                    }
                }
            };

            wait.await;
        }
    }

    pub(super) async fn cleanup_committed(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        let tx_outcome = TxOutcome::Committed(ts);
        let lsm_rw = self.lsm.read_write()?;

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

    // TODO: make this take a lockmgr guard that proves the lock is held
    async fn unsafe_get_latest_record<R: LsmRead>(
        lsm_read: &R,
        keyspace_id: KeyspaceId,
        key: &[u8],
    ) -> anyhow::Result<Option<(Timestamp, RevisionValue)>> {
        lsm_read.get(Timestamp(u64::MAX), keyspace_id, key).await
    }

    async fn acquire_write_locks<'a>(
        &'a self,
        preconds: &Vec<Precondition>,
        muts: &BTreeMap<Key, Mutation>,
    ) -> anyhow::Result<Guard<'a>> {
        for precond in preconds {
            self.check_key(precond.keyspace_id().0, precond.key())?;
        }
        for (keyspace_id, key) in muts.keys() {
            self.check_key(keyspace_id.0, &key)?;
        }
        Ok(self
            .lock_mgr
            .lock_all(
                preconds.iter().map(|precond| precond.key()),
                muts.keys().map(|(_, k)| &k[..]),
            )
            .await)
    }

    // TODO: make this take a lockmgr guard that proves the lock is held
    async fn check_write_conflicts<R: LsmRead>(
        lsm_read: &R,
        preconds: &Vec<Precondition>,
        muts: &BTreeMap<Key, Mutation>,
    ) -> anyhow::Result<Option<Txid>> {
        for (keyspace_id, key) in Iterator::chain(
            preconds
                .iter()
                .map(|precond| (precond.keyspace_id(), precond.key())),
            muts.keys()
                .map(|(keyspace_id, key)| (*keyspace_id, &key[..])),
        ) {
            if let Some(pending_keyspace_id) = keyspace_id.pending() {
                if let Some((_, RevisionValue::Regular(value))) =
                    Self::unsafe_get_latest_record(lsm_read, pending_keyspace_id, key).await?
                {
                    let other_txid = Txid::decode(&value[..Txid::ENCODED_LEN])?;
                    return Ok(Some(other_txid));
                }
            }
        }
        Ok(None)
    }

    async fn try_write_tx_outcome(
        &self,
        txid: Txid,
        tx_outcome_record: TxOutcomeRecord,
    ) -> anyhow::Result<TxOutcome> {
        let tx_outcome_key = txid.encode_fixed();
        {
            self.check_key(KeyspaceId::TX_OUTCOMES.0, &tx_outcome_key[..])?;

            let lsm_rw = self.lsm.read_write()?;
            let _guard = self.lock_mgr.write_lock(&tx_outcome_key[..]).await;

            if let Some((_, RevisionValue::Regular(tx_outcome_bytes))) =
                Self::unsafe_get_latest_record(
                    &lsm_rw,
                    KeyspaceId::TX_OUTCOMES,
                    &tx_outcome_key[..],
                )
                .await?
            {
                let existing_tx_outcome_record: TxOutcomeRecord =
                    pb::internal::TxOutcomeRecord::decode(&tx_outcome_bytes[..])?.try_into()?;
                return Ok(existing_tx_outcome_record.tx_outcome());
            }

            let tx_outcome_record_bytes =
                pb::internal::TxOutcomeRecord::from(tx_outcome_record.clone()).encode_to_vec();
            lsm_rw
                .write(
                    Timestamp::ZERO,
                    vec![],
                    BTreeMap::from([(
                        (KeyspaceId::TX_OUTCOMES, tx_outcome_key.to_vec()),
                        Mutation::Put(tx_outcome_record_bytes),
                    )]),
                )
                .await
                .map_err(|e| InternalError::Other(e.into()))?;
        }
        let tx_outcome = tx_outcome_record.tx_outcome();
        if let TxOutcomeRecord::Committed {
            ts,
            precond_keys,
            mut_keys,
        } = tx_outcome_record
        {
            _ = self
                .commit_sender
                .send((txid, ts, precond_keys, mut_keys))
                .await;
        }
        self.waiters.notify(txid);
        Ok(tx_outcome)
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
        let _guard = self.lock_mgr.write_lock(&key[..]).await;

        let (pending_ts, value) =
            match Self::unsafe_get_latest_record(lsm_rw, pending_keyspace_id, &key).await? {
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
        let _guard = self.lock_mgr.write_lock(&key[..]).await;

        let (overwrite_ts, m) = if let Some((prepare_ts, RevisionValue::Regular(bytes))) =
            Self::unsafe_get_latest_record(lsm_rw, precond_keyspace_id, &key).await?
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

    pub(super) async fn cleanup_committed_outcomes(
        &self,
        meta_synced: Arc<MetaSynced>,
        shards: Arc<dyn Shards + Sync + Send>,
        mut r: mpsc::Receiver<(Txid, Timestamp, BTreeSet<Key>, BTreeSet<Key>)>,
    ) {
        while let Some((txid, ts, precond_keys, mut_keys)) = r.recv().await {
            Retry::new()
                .indefinitely(&async || -> anyhow::Result<()> {
                    self.cleanup_one_committed_outcome(
                        &meta_synced,
                        &shards,
                        txid,
                        ts,
                        &precond_keys,
                        &mut_keys,
                    )
                    .await?;
                    Ok::<_, anyhow::Error>(())
                })
                .await;
        }
    }

    async fn cleanup_one_committed_outcome(
        &self,
        meta_synced: &MetaSynced,
        shards: &Arc<dyn Shards + Send + Sync>,
        txid: Txid,
        ts: Timestamp,
        precond_keys: &BTreeSet<Key>,
        mut_keys: &BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        let lsm_rw = self.lsm.read_write()?;

        let mut by_tablet = HashMap::new();

        for (keyspace_id, key) in precond_keys {
            let tablet_id = meta_synced.tablet_id_for_key(keyspace_id.0, &key)?;
            by_tablet
                .entry(tablet_id)
                .or_insert_with(|| (BTreeSet::new(), BTreeSet::new()))
                .0
                .insert((*keyspace_id, key.clone()));
        }
        for (keyspace_id, key) in mut_keys {
            let tablet_id = meta_synced.tablet_id_for_key(keyspace_id.0, &key)?;
            by_tablet
                .entry(tablet_id)
                .or_insert_with(|| (BTreeSet::new(), BTreeSet::new()))
                .1
                .insert((*keyspace_id, key.clone()));
        }

        // Lifetime shenanigans.
        let tablets = by_tablet
            .keys()
            .map(|tablet_id| shards.tablet(*tablet_id).map(|tablet| (*tablet_id, tablet)))
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        let mut futures = Vec::with_capacity(by_tablet.len());
        for (tablet_id, (precond_keys, mut_keys)) in by_tablet {
            let tablet = tablets.get(&tablet_id).unwrap();
            futures.push(tablet.cleanup_committed(txid, ts, precond_keys, mut_keys));
        }
        future::try_join_all(futures).await?;

        // TODO: mutual exclusion
        let tx_outcome_key = txid.encode_fixed();
        let _guard = self.lock_mgr.write_lock(&tx_outcome_key[..]);
        lsm_rw
            .write(
                Timestamp::ZERO.plus_one(),
                vec![],
                BTreeMap::from([(
                    (KeyspaceId::TX_OUTCOMES, tx_outcome_key.to_vec()),
                    Mutation::Delete,
                )]),
            )
            .await
            .map_err(|e| InternalError::Other(e.into()))?;
        Ok(())
    }

    pub(super) async fn resolve_prepared(
        &self,
        shards: &Arc<dyn Shards + Send + Sync>,
        receiver: mpsc::Receiver<(Txid, KeyspaceId, Vec<u8>, PrepareType)>,
    ) {
        crate::util::bounded_unordered_for_each(
            receiver,
            64,
            |(txid, keyspace_id, key, prepare_type)| async move {
                let owner_tablet = shards.tablet(txid.owner()).unwrap();
                let tx_outcome = owner_tablet.wait(txid).await.unwrap();
                // Commits get cleaned up by the owner tablet calling cleanup_committed. Ignore them
                // here to avoid duplicating work.
                // TODO: retry instead of unwrap
                if let TxOutcome::Aborted = tx_outcome {
                    let lsm_rw = self.lsm.wait_read_write().await;
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

    // Scans for pending mutations that exist on disk already and delivers them to `sender`.
    pub(super) async fn scan_for_pending_mutations(
        &self,
        sender: mpsc::Sender<(Txid, KeyspaceId, Vec<u8>, PrepareType)>,
    ) {
        let keyspace_ids = self.lsm.wait_read().await.keyspaces();
        for keyspace_id in keyspace_ids {
            if !keyspace_id.is_pending() {
                continue;
            }

            Retry::new()
                .indefinitely(&async || -> anyhow::Result<()> {
                    let mut s = self
                        .scan_all(
                            self.sequencer.safe_read_ts(),
                            keyspace_id,
                            self.range.clone(),
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
    pub(super) async fn scan_for_precond_locks(
        &self,
        sender: mpsc::Sender<(Txid, KeyspaceId, Vec<u8>, PrepareType)>,
    ) {
        let keyspace_ids = self.lsm.wait_read().await.keyspaces();
        for keyspace_id in keyspace_ids {
            if !keyspace_id.is_precond() {
                continue;
            }

            Retry::new()
                .indefinitely(&async || -> anyhow::Result<()> {
                    let mut s = self
                        .scan_all(
                            self.sequencer.safe_read_ts(),
                            keyspace_id,
                            self.range.clone(),
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

    // Scans for committed outcomes that exist on disk already and delivers them to `sender`.
    pub(super) async fn scan_for_committed_outcomes(
        &self,
        sender: mpsc::Sender<(Txid, Timestamp, BTreeSet<Key>, BTreeSet<Key>)>,
    ) {
        Retry::new()
            .indefinitely(&async || -> anyhow::Result<()> {
                let mut s = self
                    .scan_all(
                        self.sequencer.safe_read_ts(),
                        KeyspaceId::TX_OUTCOMES,
                        Range::prefix(self.tablet_id.encode_fixed().to_vec()),
                        Direction::Asc,
                    )
                    .boxed();
                while let Some(record) = s.try_next().await? {
                    let txid = Txid::decode(&record.key.1[..])?;

                    let tx_outcome_record: TxOutcomeRecord =
                        pb::internal::TxOutcomeRecord::decode(&record.value[..])?.try_into()?;

                    if let TxOutcomeRecord::Committed {
                        ts: commit_ts,
                        precond_keys,
                        mut_keys,
                    } = tx_outcome_record
                    {
                        let _ = sender.send((txid, commit_ts, precond_keys, mut_keys)).await;
                    }
                }
                Ok(())
            })
            .await
    }

    // Scans the entirety of `range` by calling scan_page repeatedly.
    fn scan_all(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<Vec<u8>>,
        direction: Direction,
    ) -> impl Stream<Item = anyhow::Result<Record>> + '_ {
        scan_all(
            move |ts, keyspace_id, range, direction| async move {
                self.scan_page(ts, keyspace_id, range.borrow(), direction, 1000)
                    .await
            },
            ts,
            keyspace_id,
            range,
            direction,
        )
    }

    pub(super) async fn abort_long_waits(&self) {
        loop {
            let (instant, txid) = self.waiters.pop_oldest().await;
            let elapsed = instant.elapsed();
            let remaining = WAIT_ABORT_TIMEOUT.saturating_sub(elapsed);
            tokio::time::sleep(remaining).await;
            Retry::new()
                .indefinitely(&async || self.try_abort(txid).await)
                .await;
        }
    }

    fn scan_range(
        &self,
        colo_group_id: ColoGroupId,
        range: Range<&[u8]>,
        direction: Direction,
    ) -> anyhow::Result<Range<Vec<u8>>> {
        if colo_group_id != self.colo_group_id {
            return Err(anyhow!(
                "misroute: {:?} does not own any of {:?}",
                self.tablet_id,
                colo_group_id
            ));
        }
        let owned_range = self.range.borrow();

        let intersection = owned_range.intersection(&range);
        if intersection.is_empty() {
            return Err(anyhow!(
                "misroute: cannot scan {:?}/{:?}: {:?} owns {:?}",
                colo_group_id,
                range,
                self.tablet_id,
                owned_range,
            ));
        }

        let is_next = match direction {
            Direction::Asc => intersection.lower >= owned_range.lower,
            Direction::Desc => intersection.upper <= owned_range.upper,
        };

        if !is_next {
            return Err(anyhow!(
                "misroute: cannot scan {:?}/{:?}: {:?} owns {:?}, which is not the next range for a {:?} scan",
                colo_group_id,
                range,
                self.tablet_id,
                owned_range,
                direction,
            ));
        }

        Ok(intersection.to_vec())
    }

    fn check_key(&self, colo_group_id: ColoGroupId, key: &[u8]) -> anyhow::Result<()> {
        if self.colo_group_id != colo_group_id || !self.range.contains(&key) {
            return Err(anyhow!("{:?}/{:?} not owned", colo_group_id, key).into());
        }

        Ok(())
    }

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        let lsm_rw = self.lsm.read_write()?;
        lsm_rw.create_keyspace(keyspace_id).await?;
        if let Some(pending_keyspace_id) = keyspace_id.pending() {
            lsm_rw.create_keyspace(pending_keyspace_id).await?;
        }
        if let Some(precond_keyspace_id) = keyspace_id.precond() {
            lsm_rw.create_keyspace(precond_keyspace_id).await?;
        }

        Ok(())
    }

    pub(super) fn manifest(&self) -> Manifest {
        self.lsm.manifest()
    }

    pub(super) async fn sync_meta(
        self: &Arc<Self>,
        storage: &Arc<S>,
        shards: &Arc<dyn Shards + Sync + Send>,
        sync_type: SyncType,
        snapshot: MetaSyncedSnapshot,
    ) {
        Retry::new()
            .indefinitely(&async || -> anyhow::Result<()> {
                let sync_type = sync_type.clone();
                match sync_type {
                    SyncType::Initial => {
                        self.refresh_metadata(&storage, &shards, &snapshot).await?;

                        let tablet_metadata = snapshot.tablet_metadata(self.tablet_id).await?;
                        if matches!(
                            tablet_metadata.state,
                            MetaState::Stable(TabletState::Active),
                        ) {
                            for keyspace_id in snapshot.keyspace_ids().await? {
                                self.create_keyspace(keyspace_id).await?;
                            }
                        }
                    }
                    SyncType::Tx(meta_keys) => {
                        for meta_key in meta_keys {
                            match meta_key {
                                MetaKey::Keyspace(keyspace_id) => {
                                    let tablet_metadata =
                                        snapshot.tablet_metadata(self.tablet_id).await?;

                                    // TODO: There's a race here where we might drop a keyspace
                                    // creation on the floor if it's done during a transition. We
                                    // could make this graceful, but it'd be a lot easier to just
                                    // make keyspace creation wait until there's an active tablet
                                    // for every range.
                                    if matches!(
                                        tablet_metadata.state,
                                        MetaState::Stable(TabletState::Active),
                                    ) {
                                        self.create_keyspace(keyspace_id).await?;
                                    }
                                }
                                MetaKey::Tablet(tablet_id) => {
                                    if tablet_id == self.tablet_id {
                                        self.refresh_metadata(&storage, &shards, &snapshot).await?;
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

    pub(super) async fn wait_mostly_hydrated(&self) -> anyhow::Result<()> {
        let mut state = {
            self.hydration
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

    pub(super) async fn catchup(&self) -> anyhow::Result<()> {
        let mut state = {
            let hydration_lock = self.hydration.lock().unwrap();
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

    async fn refresh_metadata(
        self: &Arc<Self>,
        storage: &Arc<S>,
        shards: &Arc<dyn Shards + Sync + Send>,
        snapshot: &MetaSyncedSnapshot,
    ) -> anyhow::Result<()> {
        let tablet_metadata = snapshot.tablet_metadata(self.tablet_id).await?;

        let apparent_tablet_state = match tablet_metadata.state {
            MetaState::Stable(state) => state,
            MetaState::Transitioning(_, next_state) => next_state,
        };
        self.lsm.set_state(apparent_tablet_state).await;

        if let (Some(transfer_id), TabletState::Hydrating) =
            (tablet_metadata.transfer_id, apparent_tablet_state)
        {
            let transfer_metadata = snapshot.transfer_metadata(transfer_id).await?;

            let mut maybe_hydration = self.hydration.lock().unwrap();
            if matches!(maybe_hydration.deref(), None) {
                log::info!(
                    "{:?} starting hydration for {:?}",
                    self.tablet_id,
                    transfer_id
                );
                let (tx, rx) = tokio::sync::watch::channel(HydrationState::Started);
                *maybe_hydration = Some(Hydration {
                    task: spawn_owned({
                        let self_ = Arc::clone(self);
                        let storage = Arc::clone(storage);
                        let shards = Arc::clone(shards);
                        let srcs = transfer_metadata.srcs.clone();
                        async move {
                            Retry::new()
                                .indefinitely(&async || -> anyhow::Result<()> {
                                    self_.hydrate(&storage, &shards, &srcs[..]).await?;
                                    Ok(())
                                })
                                .await;
                        }
                    }),
                    set_state: tx,
                    state: rx,
                });
            }
        } else {
            let mut maybe_hydration = self.hydration.lock().unwrap();
            *maybe_hydration = None;
        }

        if let TabletState::Frozen = apparent_tablet_state {
            self.lsm.flush().await?;
        }

        Ok(())
    }

    async fn hydrate(
        self: &Arc<Self>,
        storage: &Arc<S>,
        shards: &Arc<dyn Shards + Sync + Send>,
        srcs: &[TabletId],
    ) -> anyhow::Result<()> {
        let mut preloader = Preloader::new(Arc::clone(storage));
        let mut loaded = HashSet::new();

        for i in 0.. {
            let mut run_ids_seen = HashSet::new();

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
                let src = shards.tablet(*src_id)?;

                let manifest = src.manifest().await?;

                for (keyspace_id, keyspace_manifest) in &manifest.keyspaces {
                    preloader.add_keyspace(*keyspace_id, keyspace_manifest.levels.len());
                    for (i, level) in keyspace_manifest.levels.iter().enumerate() {
                        if i == 0 {
                            continue;
                        }

                        for run_manifest in &level.runs {
                            if !self.range.contains_range(&run_manifest.range) {
                                // If the run partially overlaps, compaction at the source will
                                // eventually make it not.
                                continue;
                            }

                            if loaded.contains(&run_manifest.run_id) {
                                continue;
                            }

                            preloader.queue(run_manifest.run_id, i);
                            loaded.insert(run_manifest.run_id);
                            run_ids_seen.insert(run_manifest.run_id);
                        }
                    }
                }
            }

            // Unload the runs that went away because of compaction.
            for run_id in loaded.extract_if(|run_id| !run_ids_seen.contains(run_id)) {
                preloader.unload(run_id);
            }

            if done_after_round {
                break;
            }

            preloader.load().await?;

            if i == 3 {
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
            if i >= 3 {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }

        // TODO: Need to block compactions and only enable after we transition.
        self.lsm.load()?.load(preloader.wait().await?).await?;
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

fn scan_all<F, Fut>(
    f: F,
    ts: Timestamp,
    keyspace_id: KeyspaceId,
    range: Range<Vec<u8>>,
    direction: Direction,
) -> impl Stream<Item = anyhow::Result<Record>>
where
    F: Fn(Timestamp, KeyspaceId, Range<Vec<u8>>, Direction) -> Fut,
    Fut: Future<Output = Result<(Vec<Record>, Option<Range<Vec<u8>>>), InternalError>>,
{
    try_stream! {
        let mut maybe_cursor = Some(range);
        while let Some(cursor) = maybe_cursor {
            let (page, continue_cursor) = f(ts, keyspace_id, cursor, direction).await?;

            for record in page {
                yield record;
            }

            maybe_cursor = continue_cursor;
        }
    }
}

pub(crate) enum PrepareType {
    Precondition,
    Mutation,
}

struct PendingMutation {
    txid: Txid,
    m: Mutation,
}

impl Encode for PendingMutation {
    fn encoded_size_estimate(&self) -> usize {
        Txid::ENCODED_LEN + 1 + self.m.len()
    }

    fn encode(&self, w: &mut Vec<u8>) {
        self.txid.encode(w);
        match &self.m {
            Mutation::Put(v) => {
                w.push(1);
                w.extend_from_slice(&v[..]);
            }
            Mutation::Delete => w.push(0),
        }
    }
}

impl Decode for PendingMutation {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() < Txid::ENCODED_LEN + 1 {
            anyhow::bail!(
                "invalid pending mutation: expected >={}B, got {}B",
                Txid::ENCODED_LEN + 1,
                b.len()
            );
        }

        let txid = Txid::decode(&b[..Txid::ENCODED_LEN])?;

        let m = match b[Txid::ENCODED_LEN] {
            0 => Mutation::Delete,
            1 => Mutation::Put(b[Txid::ENCODED_LEN + 1..].to_vec()),
            _ => anyhow::bail!("invalid pending mutation: type tag not in [0, 1]"),
        };

        Ok(Self { txid, m })
    }
}

struct PrecondLocks {
    txids: BTreeSet<Txid>,
}

impl Encode for PrecondLocks {
    fn encoded_size_estimate(&self) -> usize {
        Txid::ENCODED_LEN * self.txids.len()
    }

    fn encode(&self, w: &mut Vec<u8>) {
        for txid in &self.txids {
            txid.encode(w);
        }
    }
}

impl Decode for PrecondLocks {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() % Txid::ENCODED_LEN != 0 {
            return Err(anyhow!(
                "wrong length for precond value {}: must be a multiple of {}",
                b.len(),
                Txid::ENCODED_LEN
            ));
        }

        let txids = b
            .chunks(Txid::ENCODED_LEN)
            .map(Txid::decode)
            .collect::<anyhow::Result<BTreeSet<_>>>()?;

        Ok(Self { txids })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TxOutcomeRecord {
    Committed {
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    },
    Aborted,
}

impl TxOutcomeRecord {
    fn tx_outcome(&self) -> TxOutcome {
        match self {
            TxOutcomeRecord::Aborted => TxOutcome::Aborted,
            TxOutcomeRecord::Committed { ts, .. } => TxOutcome::Committed(*ts),
        }
    }
}

impl From<TxOutcomeRecord> for pb::internal::TxOutcomeRecord {
    fn from(value: TxOutcomeRecord) -> Self {
        Self {
            outcome_type: match value {
                TxOutcomeRecord::Committed {
                    ts,
                    precond_keys,
                    mut_keys,
                } => Some(pb::internal::tx_outcome_record::OutcomeType::Committed(
                    pb::internal::tx_outcome_record::Committed {
                        ts: ts.as_nanos(),
                        precond_keys: Some(precond_keys.into()),
                        mut_keys: Some(mut_keys.into()),
                    },
                )),
                TxOutcomeRecord::Aborted {} => {
                    Some(pb::internal::tx_outcome_record::OutcomeType::Aborted(()))
                }
            },
        }
    }
}

impl TryFrom<pb::internal::TxOutcomeRecord> for TxOutcomeRecord {
    type Error = anyhow::Error;

    fn try_from(value: pb::internal::TxOutcomeRecord) -> Result<Self, Self::Error> {
        Ok(match value.outcome_type {
            Some(pb::internal::tx_outcome_record::OutcomeType::Committed(
                pb::internal::tx_outcome_record::Committed {
                    ts,
                    precond_keys,
                    mut_keys,
                },
            )) => TxOutcomeRecord::Committed {
                ts: Timestamp::from_nanos(ts),
                mut_keys: BTreeSet::<Key>::try_from(
                    mut_keys.ok_or_else(|| anyhow!("missing mut_keys"))?,
                )?,
                precond_keys: BTreeSet::<Key>::try_from(
                    precond_keys.ok_or_else(|| anyhow!("missing precond_keys"))?,
                )?,
            },
            Some(pb::internal::tx_outcome_record::OutcomeType::Aborted(_)) => {
                TxOutcomeRecord::Aborted
            }
            None => return Err(anyhow!("missing outcome_type")),
        })
    }
}

struct Waiters {
    inner: Mutex<WaitersInner>,
    arrival: tokio::sync::Notify,
}

struct WaitersInner {
    by_txid: HashMap<
        Txid,
        (
            tokio::sync::watch::Sender<()>,
            tokio::sync::watch::Receiver<()>,
        ),
    >,
    by_oldest_waiter: VecDeque<(Instant, Txid)>,
}

impl Waiters {
    fn new() -> Self {
        Self {
            arrival: tokio::sync::Notify::new(),
            inner: Mutex::new(WaitersInner {
                by_txid: HashMap::new(),
                by_oldest_waiter: VecDeque::new(),
            }),
        }
    }

    fn notify(&self, txid: Txid) {
        let mut inner = self.inner.lock().unwrap();
        if let Some((tx, _)) = inner.by_txid.remove(&txid) {
            _ = tx.send(());
        }
    }

    async fn pop_oldest(&self) -> (Instant, Txid) {
        loop {
            {
                let mut inner = self.inner.lock().unwrap();
                if let Some((instant, txid)) = inner.by_oldest_waiter.pop_front() {
                    return (instant, txid);
                }
            }
            self.arrival.notified().await;
        }
    }

    async fn wait(&self, txid: Txid) {
        let mut rx = {
            let mut inner = self.inner.lock().unwrap();
            let new = !inner.by_txid.contains_key(&txid);
            let rx = inner
                .by_txid
                .entry(txid)
                .or_insert_with(|| tokio::sync::watch::channel(()))
                .1
                .clone();
            if new {
                inner.by_oldest_waiter.push_back((Instant::now(), txid));
                self.arrival.notify_one();
            }
            rx
        };
        _ = rx.changed().await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::TxOutcomeRecord;
    use crate::pb;
    use crate::test::assert_roundtrip_pb;
    use crate::types::ColoGroupId;
    use crate::types::KeyspaceId;
    use crate::types::Timestamp;

    #[test]
    fn test_tx_outcome_record_encoding() -> anyhow::Result<()> {
        assert_roundtrip_pb::<_, pb::internal::TxOutcomeRecord>(TxOutcomeRecord::Aborted)?;

        assert_roundtrip_pb::<_, pb::internal::TxOutcomeRecord>(TxOutcomeRecord::Committed {
            ts: Timestamp(5),
            precond_keys: BTreeSet::new(),
            mut_keys: BTreeSet::new(),
        })?;

        assert_roundtrip_pb::<_, pb::internal::TxOutcomeRecord>(TxOutcomeRecord::Committed {
            ts: Timestamp(5),
            precond_keys: BTreeSet::new(),
            mut_keys: BTreeSet::from([(KeyspaceId(ColoGroupId(5), 8), vec![1, 2, 3])]),
        })?;

        assert_roundtrip_pb::<_, pb::internal::TxOutcomeRecord>(TxOutcomeRecord::Committed {
            ts: Timestamp(5),
            precond_keys: BTreeSet::from([(KeyspaceId(ColoGroupId(3), 4), vec![4, 5, 6])]),
            mut_keys: BTreeSet::from([(KeyspaceId(ColoGroupId(5), 8), vec![1, 2, 3])]),
        })?;

        assert_roundtrip_pb::<_, pb::internal::TxOutcomeRecord>(TxOutcomeRecord::Committed {
            ts: Timestamp(5),
            precond_keys: BTreeSet::new(),
            mut_keys: BTreeSet::from([
                (KeyspaceId(ColoGroupId(5), 8), vec![1, 2, 3]),
                (KeyspaceId(ColoGroupId(3), 4), vec![4, 5, 6]),
            ]),
        })?;

        assert_roundtrip_pb::<_, pb::internal::TxOutcomeRecord>(TxOutcomeRecord::Committed {
            ts: Timestamp(5),
            precond_keys: BTreeSet::new(),
            mut_keys: BTreeSet::from([
                (KeyspaceId(ColoGroupId(5), 8), vec![1, 2, 3]),
                (KeyspaceId(ColoGroupId(5), 8), vec![1, 2, 5]),
            ]),
        })?;

        assert_roundtrip_pb::<_, pb::internal::TxOutcomeRecord>(TxOutcomeRecord::Committed {
            ts: Timestamp(5),
            precond_keys: BTreeSet::new(),
            mut_keys: BTreeSet::from([
                (KeyspaceId(ColoGroupId(5), 8), vec![1, 2, 3]),
                (KeyspaceId(ColoGroupId(5), 8), vec![1, 2, 5]),
                (KeyspaceId(ColoGroupId(5), 8), vec![1, 2, 5, 8]),
            ]),
        })?;

        Ok(())
    }
}
