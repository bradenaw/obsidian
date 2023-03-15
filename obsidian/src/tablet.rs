use std::cmp;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::Cursor;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::anyhow;
use async_trait::async_trait;
use byteorder::BigEndian;
use byteorder::ByteOrder;
use byteorder::LittleEndian;
use futures::future;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::lock_mgr::Guard;
use crate::lock_mgr::LockMgr;
use crate::lsm::Lsm;
use crate::obsidian::InternalError;
use crate::obsidian::Router;
use crate::obsidian::TabletId;
use crate::obsidian::Tablets;
use crate::obsidian::TxOutcome;
use crate::obsidian::Txid;
use crate::range::Range;
use crate::range::RangeSet;
use crate::sequencer::Sequencer;
use crate::types::ColoGroupId;
use crate::types::Direction;
use crate::types::HistoryRange;
use crate::types::KeyspaceId;
use crate::types::Mutation;
use crate::types::Precondition;
use crate::types::ShardId;
use crate::types::Timestamp;
use crate::types::Value;
use crate::util::longest_shared_prefix_len;
use crate::util::read_varint_from;
use crate::util::write_varint_to;

const MAX_PRECOND_VALUE_LEN: usize = 256;

#[async_trait]
pub(crate) trait Tablet {
    async fn get(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, InternalError>;

    async fn get_latest(
        &self,
        keyspace_id: KeyspaceId,
        key: &[u8],
    ) -> Result<(Timestamp, Option<Vec<u8>>), InternalError>;

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<(Vec<u8>, Timestamp, Vec<u8>)>, Option<Range<Vec<u8>>>), InternalError>;

    async fn history_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        key: &[u8],
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<(Timestamp, Value)>, Option<HistoryRange>)>;

    async fn write(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<(KeyspaceId, Vec<u8>), Mutation>,
    ) -> Result<Timestamp, InternalError>;

    async fn prepare(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<(KeyspaceId, Vec<u8>), Mutation>,
    ) -> Result<Timestamp, InternalError>;

    async fn try_commit(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
        mut_keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
    ) -> anyhow::Result<TxOutcome>;
    async fn try_abort(&self, txid: Txid) -> anyhow::Result<TxOutcome>;
    async fn wait(&self, txid: Txid) -> anyhow::Result<TxOutcome>;
    async fn cleanup_committed(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
        mut_keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
    ) -> anyhow::Result<()>;
}

pub(crate) struct LsmTablet {
    inner: Arc<LsmTabletInner>,

    bg_cleanup_committed_outcomes: JoinHandle<()>,
    bg_resolve_pending: JoinHandle<()>,
}

#[async_trait]
impl Tablet for LsmTablet {
    async fn get(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, InternalError> {
        self.inner.get(ts, keyspace_id, key).await
    }

    async fn get_latest(
        &self,
        keyspace_id: KeyspaceId,
        key: &[u8],
    ) -> Result<(Timestamp, Option<Vec<u8>>), InternalError> {
        self.inner.get_latest(keyspace_id, key).await
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<(Vec<u8>, Timestamp, Vec<u8>)>, Option<Range<Vec<u8>>>), InternalError> {
        self.inner
            .scan_page(ts, keyspace_id, range, direction, limit)
            .await
    }

    async fn history_page(
        &self,
        _ts: Timestamp,
        _keyspace_id: KeyspaceId,
        _key: &[u8],
        _range: HistoryRange,
        _direction: Direction,
        _limit: usize,
    ) -> anyhow::Result<(Vec<(Timestamp, Value)>, Option<HistoryRange>)> {
        todo!();
    }

    async fn write(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<(KeyspaceId, Vec<u8>), Mutation>,
    ) -> Result<Timestamp, InternalError> {
        self.inner.write(txid, preconds, muts).await
    }

    async fn prepare(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<(KeyspaceId, Vec<u8>), Mutation>,
    ) -> Result<Timestamp, InternalError> {
        self.inner.prepare(txid, preconds, muts).await
    }

    async fn try_commit(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
        mut_keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
    ) -> anyhow::Result<TxOutcome> {
        self.inner
            .try_commit(txid, ts, precond_keys, mut_keys)
            .await
    }
    async fn try_abort(&self, txid: Txid) -> anyhow::Result<TxOutcome> {
        self.inner.try_abort(txid).await
    }
    async fn wait(&self, txid: Txid) -> anyhow::Result<TxOutcome> {
        self.inner.wait(txid).await
    }
    async fn cleanup_committed(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
        mut_keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
    ) -> anyhow::Result<()> {
        self.inner
            .cleanup_committed(txid, ts, precond_keys, mut_keys)
            .await
    }
}

impl LsmTablet {
    pub async fn new(
        tablet_id: TabletId,
        lsm: Lsm,
        owned_ranges: HashMap<ColoGroupId, RangeSet<Vec<u8>>>,
        tablets: Box<dyn Tablets + Sync + Send>,
        router: Box<dyn Router + Sync + Send>,
    ) -> anyhow::Result<Self> {
        let (prepare_sender, prepare_receiver) = mpsc::channel(1024);
        let (commit_sender, commit_receiver) = mpsc::channel(128);

        lsm.create_keyspace(KeyspaceId::TX_OUTCOMES).await?;

        let inner = Arc::new(LsmTabletInner::new(
            tablet_id,
            lsm,
            owned_ranges,
            tablets,
            router,
            prepare_sender,
            commit_sender,
        ));

        // TODO: also read already-committed outcomes into commit_sender

        let inner_ = inner.clone();
        let bg_cleanup_committed_outcomes = tokio::spawn(async move {
            inner_
                .clone()
                .cleanup_committed_outcomes(commit_receiver)
                .await;
        });

        let inner_ = inner.clone();
        let bg_resolve_pending = tokio::spawn(async move {
            inner_.clone().resolve_prepared(prepare_receiver).await;
        });

        Ok(Self {
            inner,
            bg_cleanup_committed_outcomes,
            bg_resolve_pending,
        })
    }

    pub async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        self.inner.lsm.create_keyspace(keyspace_id).await?;
        if keyspace_id.is_userland() {
            self.inner
                .lsm
                .create_keyspace(keyspace_id.pending().unwrap())
                .await?;
            self.inner
                .lsm
                .create_keyspace(keyspace_id.precond().unwrap())
                .await?;
        }

        Ok(())
    }
}

impl Drop for LsmTablet {
    fn drop(&mut self) {
        self.bg_cleanup_committed_outcomes.abort();
        self.bg_resolve_pending.abort();
    }
}

struct LsmTabletInner {
    tablet_id: TabletId,
    lsm: Lsm,
    owned_ranges: HashMap<ColoGroupId, RangeSet<Vec<u8>>>,
    tablets: Box<dyn Tablets + Sync + Send>,
    router: Box<dyn Router + Sync + Send>,
    sequencer: Sequencer,
    lock_mgr: LockMgr,

    prepare_sender: mpsc::Sender<(Txid, KeyspaceId, Vec<u8>, PrepareType)>,
    commit_sender: mpsc::Sender<(
        Txid,
        Timestamp,
        BTreeSet<(KeyspaceId, Vec<u8>)>,
        BTreeSet<(KeyspaceId, Vec<u8>)>,
    )>,
    waiters: Mutex<
        HashMap<
            Txid,
            (
                tokio::sync::watch::Sender<()>,
                tokio::sync::watch::Receiver<()>,
            ),
        >,
    >,
}

impl LsmTabletInner {
    fn new(
        tablet_id: TabletId,
        lsm: Lsm,
        owned_ranges: HashMap<ColoGroupId, RangeSet<Vec<u8>>>,
        tablets: Box<dyn Tablets + Sync + Send>,
        router: Box<dyn Router + Sync + Send>,
        prepare_sender: mpsc::Sender<(Txid, KeyspaceId, Vec<u8>, PrepareType)>,
        commit_sender: mpsc::Sender<(
            Txid,
            Timestamp,
            BTreeSet<(KeyspaceId, Vec<u8>)>,
            BTreeSet<(KeyspaceId, Vec<u8>)>,
        )>,
    ) -> Self {
        Self {
            tablet_id,
            lsm,
            owned_ranges,
            tablets,
            router,
            prepare_sender,
            commit_sender,
            sequencer: Sequencer::new(),
            lock_mgr: LockMgr::new(16384),
            waiters: Mutex::new(HashMap::new()),
        }
    }

    async fn get(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, InternalError> {
        self.check_key(keyspace_id.0, &key)?;
        self.sequencer.wait_for_safe_read(ts).await?;

        let (maybe_record, maybe_pending_value) = future::try_join(
            self.lsm.get(ts, keyspace_id, &key),
            self.lsm.get(
                ts,
                keyspace_id
                    .pending()
                    .ok_or_else(|| anyhow::anyhow!("not a userland keyspace"))?,
                &key,
            ),
        )
        .await?;

        if let Some((_, Value::Regular(bytes))) = maybe_pending_value {
            let pending_mut = PendingMutation::decode(&bytes)?;
            return Err(InternalError::Conflict(pending_mut.txid));
        }

        Ok(match maybe_record {
            Some((_, value)) => match value {
                Value::Regular(v) => Some(v),
                Value::Tombstone => None,
            },
            None => None,
        })
    }

    async fn get_latest(
        &self,
        keyspace_id: KeyspaceId,
        key: &[u8],
    ) -> Result<(Timestamp, Option<Vec<u8>>), InternalError> {
        self.check_key(keyspace_id.0, &key)?;

        let _guard = self.lock_mgr.read_lock(key).await;

        let safe_read_ts = self.sequencer.safe_read_ts();

        let (maybe_record, maybe_pending_value) = future::try_join(
            self.unsafe_get_latest_record(keyspace_id, &key),
            self.unsafe_get_latest_record(
                keyspace_id
                    .pending()
                    .ok_or_else(|| anyhow::anyhow!("not a userland keyspace"))?,
                &key,
            ),
        )
        .await?;

        if let Some((_, Value::Regular(bytes))) = maybe_pending_value {
            let pending_mut = PendingMutation::decode(&bytes)?;
            return Err(InternalError::Conflict(pending_mut.txid));
        }

        Ok(match maybe_record {
            Some((ts, value)) => match value {
                Value::Regular(v) => (ts, Some(v)),
                Value::Tombstone => (ts, None),
            },
            None => (safe_read_ts, None),
        })
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<(Vec<u8>, Timestamp, Vec<u8>)>, Option<Range<Vec<u8>>>), InternalError> {
        if limit == 0 {
            return Err(anyhow!("scan_page limit=0").into());
        }
        let limit = cmp::min(limit, 1000);

        let owned_range_set = self
            .owned_ranges
            .get(&keyspace_id.0)
            .ok_or_else(|| anyhow!("no ranges owned from {:?}", keyspace_id))?;

        let intersecting_range_set = owned_range_set
            .intersection(&RangeSet::from(range.to_vec()))
            .clone();

        let scan_range = match direction {
            Direction::Asc => intersecting_range_set.first(),
            Direction::Desc => intersecting_range_set.last(),
        }
        .ok_or_else(|| {
            anyhow!(
                "misroute: {:?} owns no ranges overlapping with {:?}",
                self.tablet_id,
                range
            )
        })?;

        // range                          |-----------|
        // owned_range_set          |---------|    |--------|
        // intersecting_range_set         |---|    |--|
        // scan_range                     |---|

        // Make sure scan_range is actually the next range to look at for `range`.
        let ok = match direction {
            Direction::Asc => scan_range.lower.borrow() == range.lower,
            Direction::Desc => scan_range.upper.borrow() == range.upper,
        };
        if !ok {
            return Err(anyhow!(
                "misroute: {:?} not the next tablet for {:?}",
                self.tablet_id,
                range
            )
            .into());
        }

        self.sequencer.wait_for_safe_read(ts).await?;

        // Ask the LSM for the page. Note that the returned continuation is in terms of the
        // constrained range that we asked it for, not the entire range from the request.
        let (page, intersecting_continue_cursor) = self
            .lsm
            .scan_page(ts, keyspace_id, scan_range.borrow(), direction, limit)
            .await?;
        let scanned_range = match intersecting_continue_cursor {
            Some(ref intersecting_continue_cursor) => match direction {
                Direction::Asc => Range {
                    lower: scan_range.lower.clone(),
                    upper: intersecting_continue_cursor.lower.clone(),
                },
                Direction::Desc => Range {
                    lower: intersecting_continue_cursor.upper.clone(),
                    upper: scan_range.upper.clone(),
                },
            },
            None => scan_range.clone(),
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
                let (conflict_page, conflict_continue_cursor) = self
                    .lsm
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
                    if let Value::Regular(bytes) = record.value {
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
                .filter_map(|record| match record.value {
                    Value::Regular(v) => Some((record.key, record.ts, v)),
                    Value::Tombstone => None,
                })
                .collect(),
            maybe_continue_cursor,
        ))
    }

    async fn write(
        &self,
        _txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<(KeyspaceId, Vec<u8>), Mutation>,
    ) -> Result<Timestamp, InternalError> {
        let _guard = self.acquire_write_locks(&preconds, &muts).await?;

        if let Some(conflict_txid) = self.check_write_conflicts(&preconds, &muts).await? {
            return Err(InternalError::Conflict(conflict_txid));
        }

        let ts = self.sequencer.start_write();

        self.lsm
            .write(*ts, preconds, muts)
            .await
            .map_err(|e| InternalError::Other(e.into()))?;

        Ok(*ts)
    }

    async fn prepare(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<(KeyspaceId, Vec<u8>), Mutation>,
    ) -> Result<Timestamp, InternalError> {
        let _guard = self.acquire_write_locks(&preconds, &muts).await?;

        if let Some(conflict_txid) = self.check_write_conflicts(&preconds, &muts).await? {
            return Err(InternalError::Conflict(conflict_txid));
        }

        let ts = self.sequencer.start_write();

        let mut actual_muts = BTreeMap::new();

        for precond in &preconds {
            let keyspace_id = precond
                .keyspace_id()
                .precond()
                .ok_or_else(|| anyhow::anyhow!("non-userland keyspace"))?;
            let mut value = self
                .unsafe_get_latest_record(keyspace_id, precond.key())
                .await
                .map_err(|e| InternalError::Other(e.into()))?
                .map(|(_, v)| match v {
                    Value::Regular(v) => v,
                    Value::Tombstone => vec![],
                })
                .unwrap_or(vec![]);
            value.extend_from_slice(&txid.to_bytes()[..]);

            if value.len() > MAX_PRECOND_VALUE_LEN {
                return Err(InternalError::Other(anyhow::anyhow!("too much contention")));
            }

            actual_muts.insert((keyspace_id, precond.key().to_vec()), Mutation::Put(value));
        }
        for ((keyspace_id, key), m) in &muts {
            let value = PendingMutation { txid, m: m.clone() }.encode();

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

        self.lsm
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
        precond_keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
        mut_keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
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

    async fn wait(&self, txid: Txid) -> anyhow::Result<TxOutcome> {
        loop {
            let mut rx = {
                let tx_outcome_key = txid.to_bytes();
                let _guard = self.lock_mgr.read_lock(&tx_outcome_key[..]).await;

                if let Some((_, Value::Regular(tx_outcome_bytes))) = self
                    .unsafe_get_latest_record(KeyspaceId::TX_OUTCOMES, &tx_outcome_key[..])
                    .await?
                {
                    let tx_outcome_record = TxOutcomeRecord::decode(&tx_outcome_bytes[..])?;
                    return Ok(tx_outcome_record.tx_outcome());
                }

                let mut waiters = self.waiters.lock().unwrap();
                let (_, rx) = waiters
                    .entry(txid)
                    .or_insert_with(|| tokio::sync::watch::channel(()));
                rx.clone()
            };
            _ = rx.changed().await;
        }
    }

    async fn cleanup_committed(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
        mut_keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
    ) -> anyhow::Result<()> {
        let tx_outcome = TxOutcome::Committed(ts);

        for (keyspace_id, key) in precond_keys {
            self.cleanup_precond_key(txid, keyspace_id, key).await?;
        }
        for (keyspace_id, key) in mut_keys {
            self.cleanup_pending_key(txid, tx_outcome, keyspace_id, key)
                .await?;
        }

        Ok(())
    }

    // TODO: make this take a lockmgr guard that proves the lock is held
    async fn unsafe_get_latest_record(
        &self,
        keyspace_id: KeyspaceId,
        key: &[u8],
    ) -> anyhow::Result<Option<(Timestamp, Value)>> {
        self.lsm.get(Timestamp(u64::MAX), keyspace_id, key).await
    }

    async fn acquire_write_locks<'a>(
        &'a self,
        preconds: &Vec<Precondition>,
        muts: &BTreeMap<(KeyspaceId, Vec<u8>), Mutation>,
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
    async fn check_write_conflicts(
        &self,
        preconds: &Vec<Precondition>,
        muts: &BTreeMap<(KeyspaceId, Vec<u8>), Mutation>,
    ) -> anyhow::Result<Option<Txid>> {
        for (keyspace_id, key) in Iterator::chain(
            preconds
                .iter()
                .map(|precond| (precond.keyspace_id(), precond.key())),
            muts.keys()
                .map(|(keyspace_id, key)| (*keyspace_id, &key[..])),
        ) {
            if let Some((_, Value::Regular(value))) = self
                .unsafe_get_latest_record(
                    keyspace_id
                        .pending()
                        .ok_or_else(|| anyhow::anyhow!("non-userland keyspace"))?,
                    key,
                )
                .await?
            {
                let other_txid = Txid::try_from(&value[..Txid::ENCODED_LEN])?;
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
        let tx_outcome_key = txid.to_bytes();
        {
            let _guard = self.lock_mgr.write_lock(&tx_outcome_key[..]).await;
            if let Some((_, Value::Regular(tx_outcome_bytes))) = self
                .unsafe_get_latest_record(KeyspaceId::TX_OUTCOMES, &tx_outcome_key[..])
                .await?
            {
                let existing_tx_outcome_record = TxOutcomeRecord::decode(&tx_outcome_bytes[..])?;
                return Ok(existing_tx_outcome_record.tx_outcome());
            }
            self.lsm
                .write(
                    Timestamp::ZERO,
                    vec![],
                    BTreeMap::from([(
                        (KeyspaceId::TX_OUTCOMES, tx_outcome_key.to_vec()),
                        Mutation::Put(tx_outcome_record.encode()),
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
        let mut waiters = self.waiters.lock().unwrap();
        if let Some((tx, _)) = waiters.remove(&txid) {
            _ = tx.send(());
        }
        Ok(tx_outcome)
    }

    async fn cleanup_pending_key(
        &self,
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

        let (pending_ts, value) = match self
            .unsafe_get_latest_record(pending_keyspace_id, &key)
            .await?
        {
            Some((pending_ts, value)) => (pending_ts, value),
            None => return Ok(()),
        };
        let m = match value {
            Value::Regular(v) => {
                let pending_m = PendingMutation::decode(&v)?;
                if pending_m.txid != txid {
                    return Ok(());
                }
                pending_m.m
            }
            Value::Tombstone => return Ok(()),
        };
        let resolve_ts = match tx_outcome {
            TxOutcome::Committed(commit_ts) => commit_ts,
            TxOutcome::Aborted => Timestamp(pending_ts.0 + 1),
        };
        muts.insert((pending_keyspace_id, key.clone()), Mutation::Delete);
        if let TxOutcome::Committed(_) = tx_outcome {
            muts.insert((keyspace_id, key.clone()), m);
        }
        self.lsm.write(resolve_ts, vec![], muts).await?;
        Ok(())
    }

    async fn cleanup_precond_key(
        &self,
        txid: Txid,
        keyspace_id: KeyspaceId,
        key: Vec<u8>,
    ) -> anyhow::Result<()> {
        let precond_keyspace_id = keyspace_id
            .precond()
            .ok_or_else(|| anyhow::anyhow!("not a userland keyspace"))?;

        let mut muts = BTreeMap::new();
        let _guard = self.lock_mgr.write_lock(&key[..]).await;

        let (overwrite_ts, m) = if let Some((prepare_ts, Value::Regular(bytes))) = self
            .unsafe_get_latest_record(precond_keyspace_id, &key)
            .await?
        {
            let n = bytes.len();
            let new_value_bytes = bytes
                .chunks(Txid::ENCODED_LEN)
                .filter_map(|txid_bytes| match Txid::try_from(txid_bytes) {
                    Ok(other_txid) => {
                        if other_txid == txid {
                            None
                        } else {
                            Some(Ok(txid_bytes))
                        }
                    }
                    Err(e) => Some(Err(e)),
                })
                .try_fold(Vec::with_capacity(n), |mut acc, elem| {
                    acc.extend_from_slice(elem?);
                    Ok::<Vec<u8>, anyhow::Error>(acc)
                })?;

            let m = if new_value_bytes.is_empty() {
                Mutation::Delete
            } else {
                Mutation::Put(new_value_bytes)
            };

            (prepare_ts.plus_one(), m)
        } else {
            return Ok(());
        };
        muts.insert((precond_keyspace_id, key.clone()), m);
        self.lsm.write(overwrite_ts, vec![], muts).await?;
        Ok(())
    }

    async fn cleanup_committed_outcomes(
        &self,
        mut r: mpsc::Receiver<(
            Txid,
            Timestamp,
            BTreeSet<(KeyspaceId, Vec<u8>)>,
            BTreeSet<(KeyspaceId, Vec<u8>)>,
        )>,
    ) {
        while let Some((txid, ts, precond_keys, mut_keys)) = r.recv().await {
            self.cleanup_one_committed_outcome(txid, ts, precond_keys, mut_keys)
                .await
                .unwrap();
        }
    }

    async fn cleanup_one_committed_outcome(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
        mut_keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
    ) -> anyhow::Result<()> {
        let mut by_tablet = HashMap::new();

        for (keyspace_id, key) in precond_keys {
            let tablet_id = self.router.tablet_id_for_key(keyspace_id.0, &key)?;
            by_tablet
                .entry(tablet_id)
                .or_insert_with(|| (BTreeSet::new(), BTreeSet::new()))
                .0
                .insert((keyspace_id, key));
        }
        for (keyspace_id, key) in mut_keys {
            let tablet_id = self.router.tablet_id_for_key(keyspace_id.0, &key)?;
            by_tablet
                .entry(tablet_id)
                .or_insert_with(|| (BTreeSet::new(), BTreeSet::new()))
                .1
                .insert((keyspace_id, key));
        }

        // Lifetime shenanigans.
        let tablets = by_tablet
            .keys()
            .map(|tablet_id| {
                self.tablets
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
        let tx_outcome_key = txid.to_bytes();
        self.lsm
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
                let owner_tablet = self.tablets.tablet(txid.owner()).unwrap();
                let tx_outcome = owner_tablet.wait(txid).await.unwrap();
                // Commits get cleaned up by the owner tablet calling cleanup_committed. Ignore them
                // here to avoid duplicating work.
                // TODO: retry instead of unwrap
                if let TxOutcome::Aborted = tx_outcome {
                    match prepare_type {
                        PrepareType::Precondition => self
                            .cleanup_precond_key(txid, keyspace_id, key)
                            .await
                            .unwrap(),
                        PrepareType::Mutation => self
                            .cleanup_pending_key(txid, tx_outcome, keyspace_id, key)
                            .await
                            .unwrap(),
                    }
                }
            },
        )
        .await;
    }

    fn check_key(&self, colo_group_id: ColoGroupId, key: &[u8]) -> anyhow::Result<()> {
        if colo_group_id == ColoGroupId::META {
            if key.len() < 12 {
                return Err(anyhow!("key {:?} too short for ColoGroupId::META", key));
            }
            let tablet_id = TabletId(
                ShardId(BigEndian::read_u32(&key[0..4])),
                BigEndian::read_u64(&key[4..12]),
            );
            if self.tablet_id == tablet_id {
                return Ok(());
            }
        }
        if self
            .owned_ranges
            .get(&colo_group_id)
            .ok_or_else(|| anyhow!("no ranges owned from {:?}", colo_group_id))?
            .contains(&key.to_vec())
        {
            return Ok(());
        }
        Err(anyhow!("{:?}/{:?} not owned", colo_group_id, key).into())
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

impl PendingMutation {
    fn encode(&self) -> Vec<u8> {
        let txid_bytes = self.txid.to_bytes();

        let mut value = Vec::with_capacity(txid_bytes.len() + 1 + self.m.len());

        value.extend_from_slice(&txid_bytes[..]);
        match &self.m {
            Mutation::Put(v) => {
                value.push(1);
                value.extend_from_slice(&v[..]);
            }
            Mutation::Delete => value.push(0),
        }

        value
    }

    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() < Txid::ENCODED_LEN + 1 {
            anyhow::bail!("invalid pending mutation: too short");
        }

        let txid = Txid::try_from(&b[..Txid::ENCODED_LEN])?;

        let m = match b[Txid::ENCODED_LEN] {
            0 => Mutation::Delete,
            1 => Mutation::Put(b[Txid::ENCODED_LEN + 1..].to_vec()),
            _ => anyhow::bail!("invalid pending mutation: type tag not in [0, 1]"),
        };

        Ok(Self { txid, m })
    }
}

#[derive(Debug, Eq, PartialEq)]
enum TxOutcomeRecord {
    Committed {
        ts: Timestamp,
        precond_keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
        mut_keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
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

    fn encode(&self) -> Vec<u8> {
        match self {
            TxOutcomeRecord::Aborted => vec![0],
            TxOutcomeRecord::Committed {
                ts,
                precond_keys,
                mut_keys,
            } => {
                let mut m = BTreeMap::new();
                for (keyspace_id, key) in precond_keys {
                    m.entry(key)
                        .or_insert_with(BTreeMap::new)
                        .entry(keyspace_id)
                        .or_insert(0x01);
                }
                for (keyspace_id, key) in mut_keys {
                    let entry = m
                        .entry(key)
                        .or_insert_with(BTreeMap::new)
                        .entry(keyspace_id)
                        .or_insert(0);
                    *entry = *entry | 0x02;
                }

                let mut maybe_prev_key = None;

                let mut out = vec![0; 9];
                out[0] = 1;
                LittleEndian::write_u64(&mut out[1..], ts.as_nanos());
                let mut out = Cursor::new(out);
                out.seek(SeekFrom::End(0)).unwrap();
                write_varint_to(&mut out, m.len() as u64).unwrap();
                for (key, keyspace_ids) in m {
                    let n_shared = match maybe_prev_key {
                        Some(prev_key) => longest_shared_prefix_len(prev_key, key),
                        None => 0,
                    };

                    write_varint_to(&mut out, n_shared as u64).unwrap();
                    let n_more = key.len() - n_shared;
                    write_varint_to(&mut out, n_more as u64).unwrap();
                    out.write(&key[n_shared..]).unwrap();
                    write_varint_to(&mut out, keyspace_ids.len() as u64).unwrap();

                    for (keyspace_id, bits) in keyspace_ids {
                        write_varint_to(&mut out, keyspace_id.0 .0 as u64).unwrap();
                        write_varint_to(&mut out, (keyspace_id.1 as u64) << 2 | bits).unwrap();
                    }

                    maybe_prev_key = Some(key);
                }
                out.into_inner()
            }
        }
    }

    pub fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() == 0 {
            anyhow::bail!("invalid tx outcome: empty");
        }
        match b[0] {
            0 => {
                if b.len() != 1 {
                    anyhow::bail!("invalid tx outcome: extra bytes");
                }
                Ok(TxOutcomeRecord::Aborted)
            }
            1 => {
                if b.len() < 9 {
                    anyhow::bail!("invalid tx outcome: wrong length");
                }
                let ts = Timestamp::from_nanos(LittleEndian::read_u64(&b[1..9]));

                let mut precond_keys = BTreeSet::new();
                let mut mut_keys = BTreeSet::new();

                let mut c = Cursor::new(&b[9..]);

                let n_keys = read_varint_from(&mut c)?.0;
                let mut prev_key = vec![];
                for _ in 0..n_keys {
                    let n_shared = read_varint_from(&mut c)?.0 as usize;
                    if n_shared > prev_key.len() {
                        anyhow::bail!("invalid tx outcome: shared prefix longer than prev key");
                    }
                    let n_more = read_varint_from(&mut c)?.0 as usize;
                    let mut key = vec![0u8; n_shared + n_more];
                    (key[..n_shared]).copy_from_slice(&prev_key[..n_shared]);
                    c.read_exact(&mut key[n_shared..])?;
                    let n_keyspace_ids = read_varint_from(&mut c)?.0 as usize;
                    for _ in 0..n_keyspace_ids {
                        let colo_group_id = read_varint_from(&mut c)?.0 as u32;
                        let keyspace_id_and_tag = read_varint_from(&mut c)?.0;
                        let keyspace_id = KeyspaceId(
                            ColoGroupId(colo_group_id),
                            (keyspace_id_and_tag >> 2) as u32,
                        );
                        if keyspace_id_and_tag & 0x01 != 0 {
                            precond_keys.insert((keyspace_id, key.clone()));
                        }
                        if keyspace_id_and_tag & 0x02 != 0 {
                            mut_keys.insert((keyspace_id, key.clone()));
                        }
                    }
                    prev_key = key;
                }

                Ok(TxOutcomeRecord::Committed {
                    ts,
                    precond_keys,
                    mut_keys,
                })
            }
            _ => anyhow::bail!("invalid tx outcome: tag not 0 or 1"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crate::tablet::TxOutcomeRecord;
    use crate::types::ColoGroupId;
    use crate::types::KeyspaceId;
    use crate::types::Timestamp;

    #[test]
    fn test_tx_outcome_record_encoding() -> anyhow::Result<()> {
        fn check(record: TxOutcomeRecord) -> anyhow::Result<()> {
            let encoded = record.encode();
            let decoded = TxOutcomeRecord::decode(&encoded)?;
            assert_eq!(record, decoded);
            Ok(())
        }

        check(TxOutcomeRecord::Aborted)?;

        check(TxOutcomeRecord::Committed {
            ts: Timestamp(5),
            precond_keys: BTreeSet::new(),
            mut_keys: BTreeSet::new(),
        })?;

        check(TxOutcomeRecord::Committed {
            ts: Timestamp(5),
            precond_keys: BTreeSet::new(),
            mut_keys: BTreeSet::from([(KeyspaceId(ColoGroupId(5), 8), vec![1, 2, 3])]),
        })?;

        check(TxOutcomeRecord::Committed {
            ts: Timestamp(5),
            precond_keys: BTreeSet::from([(KeyspaceId(ColoGroupId(3), 4), vec![4, 5, 6])]),
            mut_keys: BTreeSet::from([(KeyspaceId(ColoGroupId(5), 8), vec![1, 2, 3])]),
        })?;

        check(TxOutcomeRecord::Committed {
            ts: Timestamp(5),
            precond_keys: BTreeSet::new(),
            mut_keys: BTreeSet::from([
                (KeyspaceId(ColoGroupId(5), 8), vec![1, 2, 3]),
                (KeyspaceId(ColoGroupId(3), 4), vec![4, 5, 6]),
            ]),
        })?;

        check(TxOutcomeRecord::Committed {
            ts: Timestamp(5),
            precond_keys: BTreeSet::new(),
            mut_keys: BTreeSet::from([
                (KeyspaceId(ColoGroupId(5), 8), vec![1, 2, 3]),
                (KeyspaceId(ColoGroupId(5), 8), vec![1, 2, 5]),
            ]),
        })?;

        check(TxOutcomeRecord::Committed {
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
