use std::cmp;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use byteorder::BigEndian;
use byteorder::ByteOrder;
use futures::future;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use prost::Message;
use tokio::sync::mpsc;

use crate::lsm::Lsm;
use crate::meta::Meta;
use crate::meta::MetaKey;
use crate::meta::MetaReader;
use crate::meta::MetaSynced;
use crate::meta::MetaState;
use crate::meta::MetaSyncedSnapshot;
use crate::meta::SyncType;
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
use crate::tablet::Tablet;
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
use crate::types::ShardId;
use crate::types::Timestamp;
use crate::util::encode;
use crate::util::Background;
use crate::util::Decode;
use crate::util::Encode;
use crate::util::Retry;

const MAX_PRECOND_VALUE_LEN: usize = 256;
const WAIT_ABORT_TIMEOUT: Duration = Duration::from_millis(1_000);

pub(crate) struct LsmTablet<S: Storage> {
    inner: Arc<LsmTabletInner<S>>,

    bg: Background,
}

#[async_trait]
impl<S: Storage + Send + Sync + 'static> Tablet for LsmTablet<S> {
    async fn get(&self, ts: Timestamp, key: &Key) -> Result<Option<Record>, InternalError> {
        self.inner.get(ts, key).await
    }

    async fn get_latest(&self, key: Key) -> Result<(Timestamp, Option<Record>), InternalError> {
        self.inner.get_latest(key).await
    }

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> Result<Timestamp, InternalError> {
        self.inner.latest_snapshot(keys).await
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<Range<Vec<u8>>>), InternalError> {
        self.inner
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
        self.inner.history_page(key, range, direction, limit).await
    }

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        self.inner.write(preconds, muts).await
    }

    async fn prepare(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        self.inner.prepare(txid, preconds, muts).await
    }

    async fn try_commit(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<TxOutcome> {
        self.inner
            .try_commit(txid, ts, precond_keys, mut_keys)
            .await
    }
    async fn try_abort(&self, txid: Txid) -> anyhow::Result<TxOutcome> {
        self.inner.try_abort(txid).await
    }
    async fn wait(&self, txid: Txid) -> Result<TxOutcome, InternalError> {
        self.inner.wait(txid).await
    }
    async fn cleanup_committed(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        self.inner
            .cleanup_committed(txid, ts, precond_keys, mut_keys)
            .await
    }

    async fn wait_meta_sync(&self, ts: Timestamp) -> anyhow::Result<()> {
        self.inner.meta_synced.wait(ts).await
    }
}

impl<S: Storage + Send + Sync + 'static> LsmTablet<S> {
    pub async fn new(
        tablet_id: TabletId,
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
        lsm: Lsm<S>,
        meta: Box<dyn Meta + Sync + Send + 'static>,
        shards: Box<dyn Shards + Sync + Send>,
    ) -> anyhow::Result<Self> {
        let (prepare_sender, prepare_receiver) = mpsc::channel(1024);
        let (commit_sender, commit_receiver) = mpsc::channel(128);

        lsm.create_keyspace(KeyspaceId::TX_OUTCOMES).await?;

        let meta_synced = Arc::new(MetaSynced::new(meta));

        let inner = Arc::new(LsmTabletInner::new(
            tablet_id,
            colo_group_id,
            range,
            lsm,
            meta_synced,
            shards,
            prepare_sender.clone(),
            commit_sender.clone(),
        ));

        let bg = Background::new();

        bg.spawn({
            let inner = inner.clone();
            async move {
                inner.cleanup_committed_outcomes(commit_receiver).await;
            }
        });

        bg.spawn({
            let inner = inner.clone();
            async move {
                inner.resolve_prepared(prepare_receiver).await;
            }
        });

        bg.spawn({
            let inner = inner.clone();
            let prepare_sender = prepare_sender.clone();
            async move {
                inner.scan_for_pending_mutations(prepare_sender).await;
            }
        });

        bg.spawn({
            let inner = inner.clone();
            async move {
                inner.scan_for_precond_locks(prepare_sender).await;
            }
        });

        bg.spawn({
            let inner = inner.clone();
            async move {
                inner.scan_for_committed_outcomes(commit_sender).await;
            }
        });

        bg.spawn({
            let inner = inner.clone();
            async move {
                inner.abort_long_waits().await;
            }
        });

        bg.spawn({
            let inner = inner.clone();
            async move {
                inner
                    .meta_synced
                    .subscribe(async |sync_type, snapshot: MetaSyncedSnapshot| {
                        inner.sync_meta(sync_type, snapshot).await;
                    })
                    .await;
            }
        });

        Ok(Self { inner, bg })
    }
}

struct LsmTabletInner<S: Storage> {
    tablet_id: TabletId,
    colo_group_id: ColoGroupId,
    range: Range<Vec<u8>>,

    lsm: ProtectedLsm<S>,
    meta_synced: Arc<MetaSynced>,
    shards: Box<dyn Shards + Sync + Send>,
    sequencer: Sequencer,
    lock_mgr: LockMgr,

    prepare_sender: mpsc::Sender<(Txid, KeyspaceId, Vec<u8>, PrepareType)>,
    commit_sender: mpsc::Sender<(Txid, Timestamp, BTreeSet<Key>, BTreeSet<Key>)>,
    waiters: Waiters,
}

impl<S: Storage + Send + Sync + 'static> LsmTabletInner<S> {
    fn new(
        tablet_id: TabletId,
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
        lsm: Lsm<S>,
        meta_synced: Arc<MetaSynced>,
        shards: Box<dyn Shards + Sync + Send>,
        prepare_sender: mpsc::Sender<(Txid, KeyspaceId, Vec<u8>, PrepareType)>,
        commit_sender: mpsc::Sender<(Txid, Timestamp, BTreeSet<Key>, BTreeSet<Key>)>,
    ) -> Self {
        Self {
            tablet_id,
            colo_group_id,
            range,
            lsm: ProtectedLsm::new(lsm),
            meta_synced,
            shards,
            prepare_sender,
            commit_sender,
            sequencer: Sequencer::new(),
            lock_mgr: LockMgr::new(16384),
            waiters: Waiters::new(),
        }
    }

    async fn get(&self, ts: Timestamp, key: &Key) -> Result<Option<Record>, InternalError> {
        let lsm_read = self.lsm.read()?;

        let keyspace_id = key.0;

        self.check_key(keyspace_id.0, &key.1)?;
        self.sequencer.wait_for_safe_read(ts).await?;

        let (maybe_record, maybe_pending_value) = future::try_join(
            lsm_read.get(ts, keyspace_id, &key.1),
            lsm_read.get(
                ts,
                keyspace_id
                    .pending()
                    .ok_or_else(|| anyhow::anyhow!("not a userland keyspace"))?,
                &key.1,
            ),
        )
        .await?;

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

    async fn get_latest(&self, key: Key) -> Result<(Timestamp, Option<Record>), InternalError> {
        let lsm_read = self.lsm.read()?;

        let keyspace_id = key.0;
        self.check_key(keyspace_id.0, &key.1)?;

        let _guard = self.lock_mgr.read_lock(&key.1).await;

        let safe_read_ts = self.sequencer.safe_read_ts();

        let (maybe_record, maybe_pending_value) = future::try_join(
            Self::unsafe_get_latest_record(&lsm_read, keyspace_id, &key.1),
            Self::unsafe_get_latest_record(
                &lsm_read,
                keyspace_id
                    .pending()
                    .ok_or_else(|| anyhow::anyhow!("not a userland keyspace"))?,
                &key.1,
            ),
        )
        .await?;

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

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> Result<Timestamp, InternalError> {
        // TODO: This doesn't require loading the values, so we could optimize here to do less
        // work.
        let mut result = Timestamp::ZERO;
        for key in keys {
            let (ts, _) = self.get_latest(key).await?;
            result = cmp::max(result, ts);
        }
        Ok(result)
    }

    async fn scan_page(
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

        let intersecting_range = self.range.borrow().intersection(&range).clone();

        if intersecting_range.is_empty() {
            return Err(InternalError::Other(anyhow!(
                "misroute: {:?}'s owned range {:?}/{:?} does not intersect with scan range {:?}",
                self.tablet_id,
                keyspace_id.0,
                self.range,
                range,
            )));
        }

        let lsm_read = self.lsm.read()?;

        // range                          |-----------|
        // self.range               |---------|
        // intersecting_range             |---|

        self.sequencer.wait_for_safe_read(ts).await?;

        // Ask the LSM for the page. Note that the returned continuation is in terms of the
        // constrained range that we asked it for, not the entire range from the request.
        let (page, intersecting_continue_cursor) = lsm_read
            .scan_page(ts, keyspace_id, intersecting_range, direction, limit)
            .await?;
        let scanned_range = match intersecting_continue_cursor {
            Some(ref intersecting_continue_cursor) => match direction {
                Direction::Asc => Range {
                    lower: intersecting_range.lower.to_vec(),
                    upper: intersecting_continue_cursor.lower.clone(),
                },
                Direction::Desc => Range {
                    lower: intersecting_continue_cursor.upper.clone(),
                    upper: intersecting_range.upper.to_vec(),
                },
            },
            None => intersecting_range.to_vec(),
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

    async fn history_page(
        &self,
        key: Key,
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Revision>, Option<HistoryRange>), InternalError> {
        let limit = cmp::min(limit, 1000);
        let keyspace_id = key.0;

        let _guard = self.lock_mgr.read_lock(&key.1).await;
        let lsm_read = self.lsm.read()?;

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

        let (page, continue_cursor) = lsm_read
            .history_page(keyspace_id, &key.1, range, direction, limit)
            .await?;

        let maybe_pending = Self::unsafe_get_latest_record(
            &lsm_read,
            keyspace_id
                .pending()
                .ok_or_else(|| anyhow::anyhow!("not a userland keyspace"))?,
            &key.1,
        )
        .await?;

        if let Some((ts, RevisionValue::Regular(v))) = maybe_pending {
            if range.contains(ts) {
                // TODO: we can constrain this a lot more - really we only need to surface a
                // conflict if the page actually could have seen it, and we should be linearizing
                // an unbounded upper just once on the first page
                let pending_mut = PendingMutation::decode(&v)?;
                return Err(InternalError::Conflict(pending_mut.txid));
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

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        let _guard = self.acquire_write_locks(&preconds, &muts).await?;

        let lsm_rw = self.lsm.read_write()?;

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

    async fn prepare(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        let _guard = self.acquire_write_locks(&preconds, &muts).await?;

        let lsm_rw = self.lsm.read_write()?;

        if let Some(conflict_txid) = Self::check_write_conflicts(&lsm_rw, &preconds, &muts).await? {
            return Err(InternalError::Conflict(conflict_txid));
        }

        let ts = self.sequencer.start_write();

        let mut actual_muts = BTreeMap::new();

        for precond in &preconds {
            let keyspace_id = precond
                .keyspace_id()
                .precond()
                .ok_or_else(|| anyhow::anyhow!("non-userland keyspace"))?;
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
                    keyspace_id
                        .pending()
                        .ok_or_else(|| anyhow::anyhow!("non-userland keyspace"))?,
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

    async fn try_commit(
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

    async fn try_abort(&self, txid: Txid) -> anyhow::Result<TxOutcome> {
        self.try_write_tx_outcome(txid, TxOutcomeRecord::Aborted)
            .await
    }

    async fn wait(&self, txid: Txid) -> Result<TxOutcome, InternalError> {
        // A small bummer to need read permissions in order to do this.
        let lsm_read = self.lsm.read()?;
        loop {
            let tx_outcome_key = txid.encode_fixed();
            let wait = {
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

    async fn cleanup_committed(
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
            if let Some((_, RevisionValue::Regular(value))) = Self::unsafe_get_latest_record(
                lsm_read,
                keyspace_id
                    .pending()
                    .ok_or_else(|| anyhow::anyhow!("non-userland keyspace"))?,
                key,
            )
            .await?
            {
                let other_txid = Txid::decode(&value[..Txid::ENCODED_LEN])?;
                return Ok(Some(other_txid));
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
            let _guard = self.lock_mgr.write_lock(&tx_outcome_key[..]).await;
            let lsm_rw = self.lsm.read_write()?;

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
        let pending_keyspace_id = keyspace_id
            .pending()
            .ok_or_else(|| anyhow::anyhow!("not a userland keyspace"))?;

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
        let precond_keyspace_id = keyspace_id
            .precond()
            .ok_or_else(|| anyhow::anyhow!("not a userland keyspace"))?;

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

    async fn cleanup_committed_outcomes(
        &self,
        mut r: mpsc::Receiver<(Txid, Timestamp, BTreeSet<Key>, BTreeSet<Key>)>,
    ) {
        while let Some((txid, ts, precond_keys, mut_keys)) = r.recv().await {
            Retry::new()
                .indefinitely(&async || -> anyhow::Result<()> {
                    self.cleanup_one_committed_outcome(txid, ts, &precond_keys, &mut_keys)
                        .await?;
                    Ok::<_, anyhow::Error>(())
                })
                .await;
        }
    }

    async fn cleanup_one_committed_outcome(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: &BTreeSet<Key>,
        mut_keys: &BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        let lsm_rw = self.lsm.read_write()?;

        let mut by_tablet = HashMap::new();

        for (keyspace_id, key) in precond_keys {
            let tablet_id = self.meta_synced.tablet_id_for_key(keyspace_id.0, &key)?;
            by_tablet
                .entry(tablet_id)
                .or_insert_with(|| (BTreeSet::new(), BTreeSet::new()))
                .0
                .insert((*keyspace_id, key.clone()));
        }
        for (keyspace_id, key) in mut_keys {
            let tablet_id = self.meta_synced.tablet_id_for_key(keyspace_id.0, &key)?;
            by_tablet
                .entry(tablet_id)
                .or_insert_with(|| (BTreeSet::new(), BTreeSet::new()))
                .1
                .insert((*keyspace_id, key.clone()));
        }

        // Lifetime shenanigans.
        let tablets = by_tablet
            .keys()
            .map(|tablet_id| {
                self.shards
                    .tablet(*tablet_id)
                    .map(|tablet| (*tablet_id, tablet))
            })
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

    async fn resolve_prepared(
        &self,
        receiver: mpsc::Receiver<(Txid, KeyspaceId, Vec<u8>, PrepareType)>,
    ) {
        crate::util::bounded_unordered_map(
            receiver,
            64,
            |(txid, keyspace_id, key, prepare_type)| async move {
                let owner_tablet = self.shards.tablet(txid.owner()).unwrap();
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
    async fn scan_for_pending_mutations(
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
    async fn scan_for_precond_locks(
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
    async fn scan_for_committed_outcomes(
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

    async fn abort_long_waits(&self) {
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

    fn check_key(&self, colo_group_id: ColoGroupId, key: &[u8]) -> anyhow::Result<()> {
        if colo_group_id == ColoGroupId::META && self.tablet_id == TabletId::META {
            return Ok(());
        }

        if colo_group_id == ColoGroupId::TABLET_META {
            if key.len() < 12 {
                return Err(anyhow!(
                    "key {:?} too short for ColoGroupId::TABLET_META",
                    key
                ));
            }
            let tablet_id = TabletId(
                ShardId(BigEndian::read_u32(&key[0..4])),
                BigEndian::read_u64(&key[4..12]),
            );
            if self.tablet_id == tablet_id {
                return Ok(());
            }
        }

        if self.colo_group_id != colo_group_id || !self.range.contains(&key) {
            return Err(anyhow!("{:?}/{:?} not owned", colo_group_id, key).into());
        }

        Ok(())
    }

    // TODO: remove?
    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        let lsm_rw = self.lsm.read_write()?;
        lsm_rw.create_keyspace(keyspace_id).await?;
        if keyspace_id.is_userland() {
            lsm_rw
                .create_keyspace(keyspace_id.pending().unwrap())
                .await?;
            lsm_rw
                .create_keyspace(keyspace_id.precond().unwrap())
                .await?;
        }

        Ok(())
    }

    async fn sync_meta(&self, sync_type: SyncType, snapshot: MetaSyncedSnapshot) {
        Retry::new()
            .indefinitely(&async || -> anyhow::Result<()> {
                let sync_type = sync_type.clone();
                match sync_type {
                    SyncType::Initial => {
                        for keyspace_id in snapshot.keyspace_ids().await? {
                            self.create_keyspace(keyspace_id).await?;
                        }
                        let tablet_metadata = snapshot.tablet_metadata(self.tablet_id).await?;
                        self.lsm.set_state(match tablet_metadata.state {
                            MetaState::Stable(state) => state,
                            MetaState::Transitioning(_, next_state) => next_state,
                        }).await;
                    }
                    SyncType::Tx(meta_keys) => {
                        for meta_key in meta_keys {
                            match meta_key {
                                MetaKey::Keyspace(keyspace_id) => {
                                    self.create_keyspace(keyspace_id).await?;
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

    use crate::pb;
    use crate::tablet::tablet::TxOutcomeRecord;
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
