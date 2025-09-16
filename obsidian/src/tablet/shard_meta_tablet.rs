use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::future;
use futures::StreamExt;
use futures::TryStreamExt;
use prost::Message;
use tokio::sync::mpsc;

use crate::lsm::Lsm;
use crate::lsm::Manifest;
use crate::meta::MetaSynced;
use crate::meta::TabletState;
use crate::obsidian::InternalError;
use crate::obsidian::Router;
use crate::obsidian::Shards;
use crate::obsidian::TxOutcome;
use crate::obsidian::Txid;
use crate::pb;
use crate::storage::Storage;
use crate::tablet::protected::LsmReadWrite;
use crate::tablet::protected::ProtectedLsm;
use crate::tablet::tablet_inner::TabletInner;
use crate::tablet::Tablet;
use crate::tablet::TabletId;
use crate::util::Decode;
use crate::util::Retry;
use crate::util::WithBackground;
use crate::Bound;
use crate::ColoGroupId;
use crate::Direction;
use crate::HistoryRange;
use crate::Key;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Precondition;
use crate::Range;
use crate::Record;
use crate::Revision;
use crate::RevisionValue;
use crate::ShardId;
use crate::Timestamp;

const WAIT_ABORT_TIMEOUT: Duration = Duration::from_millis(1_000);

/// ShardMetaTablets are owned by a single shard and own a range of ColoGroupId::SHARD_META that
/// begins with their own shard ID.
///
/// They are distinct from other kinds of tablets:
///
/// 1. They always have TabletState::Active. Their range cannot be moved to another tablet.
/// 2. They only host the TX_OUTCOMES keyspace so they refuse regular writes but do accept
///    try_commit/try_abort.
pub(crate) struct ShardMetaTablet<S>(WithBackground<ShardMetaTabletInner<S>>)
where
    S: Storage;

struct ShardMetaTabletInner<S>
where
    S: Storage,
{
    inner: TabletInner<S>,
    meta_synced: Arc<MetaSynced>,
    shards: Arc<dyn Shards>,
    waiters: Waiters,

    commit_sender: mpsc::Sender<(Txid, Timestamp, BTreeSet<Key>, BTreeSet<Key>)>,
}

impl<S> ShardMetaTablet<S>
where
    S: Storage,
{
    pub(crate) async fn new(
        shard_id: ShardId,
        lsm: Lsm<S>,
        meta_synced: Arc<MetaSynced>,
        shards: Arc<dyn Shards>,
    ) -> anyhow::Result<Self> {
        lsm.create_keyspace(KeyspaceId::TX_OUTCOMES).await?;

        let (commit_sender, commit_receiver) = mpsc::channel(128);

        let tablet_id = TabletId::shard_meta(shard_id);

        let inner = Arc::new(ShardMetaTabletInner {
            inner: TabletInner::new(
                tablet_id,
                ColoGroupId::SHARD_META,
                TabletId::shard_meta_owned_range(shard_id),
                ProtectedLsm::new(tablet_id, lsm, TabletState::Active),
            ),
            commit_sender: commit_sender.clone(),
            meta_synced,
            shards,
            waiters: Waiters::new(),
        });

        let tablet = ShardMetaTablet(WithBackground::new(inner));

        tablet.0.spawn(async |inner| {
            inner.scan_for_committed_outcomes(commit_sender).await;
        });

        tablet.0.spawn(async |inner| {
            inner.cleanup_committed_outcomes(commit_receiver).await;
        });

        Ok(tablet)
    }
}

#[async_trait]
impl<S> Tablet for ShardMetaTablet<S>
where
    S: Storage,
{
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
        _preconds: Vec<Precondition>,
        _muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        Err(anyhow!("ShardMetaTablet::write not allowed").into())
    }

    async fn prepare(
        &self,
        _txid: Txid,
        _preconds: Vec<Precondition>,
        _muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        Err(anyhow!("ShardMetaTablet::prepare not allowed").into())
    }

    async fn try_commit(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<TxOutcome> {
        self.0
            .try_write_tx_outcome(
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
        self.0.try_abort(txid).await
    }

    async fn wait(&self, txid: Txid) -> Result<TxOutcome, InternalError> {
        let tx_outcome_key = txid.encode_fixed();
        self.0
            .inner
            .check_key(KeyspaceId::TX_OUTCOMES.0, &tx_outcome_key[..])?;
        loop {
            let wait = {
                let lsm_read = self.0.inner.lsm.read()?;
                let _guard = self.0.inner.lock_mgr.read_lock(&tx_outcome_key[..]).await;

                match TabletInner::<S>::unsafe_get_latest_record(
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
                    None => self.0.waiters.wait(txid),
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
        _txid: Txid,
        _ts: Timestamp,
        _precond_keys: BTreeSet<Key>,
        _mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        Err(anyhow!("ShardMetaTablet::cleanup_committed not allowed").into())
    }

    async fn wait_meta_sync(&self, _ts: Timestamp) -> anyhow::Result<()> {
        Err(anyhow!("ShardMetaTablet::wait_meta_sync not allowed").into())
    }

    async fn manifest(&self) -> anyhow::Result<Manifest> {
        Ok(self.0.inner.manifest())
    }

    async fn wait_mostly_hydrated(&self) -> anyhow::Result<()> {
        Err(anyhow!("ShardMetaTablet::wait_mostly_hydrated not allowed").into())
    }

    async fn catchup(&self) -> anyhow::Result<()> {
        Err(anyhow!("ShardMetaTablet::catchup not allowed").into())
    }

    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>> {
        Err(anyhow!("ShardMetaTablet::find_split not allowed").into())
    }
}

impl<S> ShardMetaTabletInner<S>
where
    S: Storage,
{
    async fn try_abort(&self, txid: Txid) -> anyhow::Result<TxOutcome> {
        self.try_write_tx_outcome(txid, TxOutcomeRecord::Aborted)
            .await
    }

    async fn try_write_tx_outcome(
        &self,
        txid: Txid,
        tx_outcome_record: TxOutcomeRecord,
    ) -> anyhow::Result<TxOutcome> {
        let tx_outcome_key = txid.encode_fixed();
        {
            self.inner
                .check_key(KeyspaceId::TX_OUTCOMES.0, &tx_outcome_key[..])?;

            let lsm_rw = self.inner.lsm.read_write()?;
            let _guard = self.inner.lock_mgr.write_lock(&tx_outcome_key[..]).await;

            if let Some((_, RevisionValue::Regular(tx_outcome_bytes))) =
                TabletInner::<S>::unsafe_get_latest_record(
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

    // Scans for committed outcomes that exist on disk already and delivers them to `sender`.
    async fn scan_for_committed_outcomes(
        &self,
        sender: mpsc::Sender<(Txid, Timestamp, BTreeSet<Key>, BTreeSet<Key>)>,
    ) {
        Retry::new()
            .indefinitely(&async || -> anyhow::Result<()> {
                let mut s = self
                    .inner
                    .scan_all(
                        self.inner.sequencer.safe_read_ts(),
                        KeyspaceId::TX_OUTCOMES,
                        Range::prefix(self.inner.tablet_id.encode_fixed().to_vec()),
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
        let lsm_rw = self.inner.lsm.read_write()?;

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
        let _guard = self.inner.lock_mgr.write_lock(&tx_outcome_key[..]);
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
    use crate::ColoGroupId;
    use crate::KeyspaceId;
    use crate::Timestamp;

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
