use std::cmp;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::sync::RwLock;
use std::time::Duration;
use std::time::SystemTime;

use async_trait::async_trait;
use byteorder::BigEndian;
use byteorder::ByteOrder;
use futures::future;
use rand::Rng;
use thiserror::Error;

use crate::lock_mgr::LockMgr;
use crate::lsm::Lsm;
use crate::sequencer::Sequencer;
use crate::types::KeyspaceId;
use crate::types::Mutation;
use crate::types::Precondition;
use crate::types::Timestamp;
use crate::types::Value;
use crate::types::WriteError;
use crate::util::hexlify;

struct Obsidian {
    router: Box<dyn Router>,
    tablets: Box<dyn Tablets>,
}

const MAX_CONFLICT_RETRIES: u32 = 10;

impl Obsidian {
    pub async fn get(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        key: Vec<u8>,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let tablet_id = self.router.tablet_id_for_key(keyspace_id, &key)?;
        let tablet = self.tablets.tablet(tablet_id)?;
        let txid = TxId::new(tablet_id);
        let mut already_seen_conflicts = HashSet::new();
        for i in 0..MAX_CONFLICT_RETRIES {
            if i != 0 {
                let delay = std::cmp::max(
                    Duration::from_millis(10).saturating_mul(2u32.saturating_pow(i - 1)),
                    Duration::from_millis(5000),
                );
                tokio::time::sleep(rand::thread_rng().gen_range(delay / 2..delay * 3 / 2)).await;
            }

            match tablet.get(ts, keyspace_id, key.clone()).await {
                Ok(v) => return Ok(v),
                Err(ReadError::Conflict(other_txid)) => {
                    if already_seen_conflicts.contains(&other_txid) {
                        continue;
                    }
                    let other_txid_owner_tablet = self.tablets.tablet(other_txid.owner)?;
                    if txid.can_preempt(&other_txid) {
                        other_txid_owner_tablet.try_abort(other_txid).await?;
                    } else {
                        other_txid_owner_tablet.wait(other_txid).await?;
                    }
                    already_seen_conflicts.insert(other_txid);
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
        anyhow::bail!("too many conflicts");
    }

    pub async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<(KeyspaceId, Vec<u8>), Mutation>,
    ) -> Result<Timestamp, WriteError> {
        let write_by_tablet = self.split_write(preconds, muts)?;

        let owner_tablet_id = write_by_tablet
            .keys()
            .skip(rand::thread_rng().gen_range(0..write_by_tablet.len()))
            .next()
            .unwrap();
        let mut txid = TxId::new(*owner_tablet_id);

        // TODO: move into loop, since need to resolve conflicts
        if write_by_tablet.len() == 1 {
            let (tablet_id, (preconds, muts)) = write_by_tablet.into_iter().next().unwrap();

            return self
                .tablets
                .tablet(tablet_id)?
                .write(txid, preconds, muts)
                .await;
        }

        for i in 0..MAX_CONFLICT_RETRIES {
            if i != 0 {
                let delay = std::cmp::max(
                    Duration::from_millis(10).saturating_mul(2u32.saturating_pow(i - 1)),
                    Duration::from_millis(5000),
                );
                tokio::time::sleep(rand::thread_rng().gen_range(delay / 2..delay * 3 / 2)).await;
            }
            let mut pending_tablets: BTreeSet<_> = write_by_tablet.keys().collect();
            let mut max_prepare_ts = Timestamp::ZERO;
            let tablets = write_by_tablet
                .keys()
                .map(|tablet_id| {
                    self.tablets
                        .tablet(*tablet_id)
                        .map(|tablet| (*tablet_id, tablet))
                })
                .collect::<anyhow::Result<BTreeMap<_, _>>>()?;

            while !pending_tablets.is_empty() {
                let mut prepare_futures = Vec::with_capacity(write_by_tablet.len());
                for tablet_id in &pending_tablets {
                    let (tablet_preconds, tablet_muts) = write_by_tablet.get(tablet_id).unwrap();
                    let tablet_id = *tablet_id;
                    let tablet = tablets.get(&tablet_id).unwrap();
                    prepare_futures.push(async move {
                        (
                            tablet_id,
                            tablet
                                .prepare(txid, tablet_preconds.to_vec(), tablet_muts.clone())
                                .await,
                        )
                    });
                }
                let prepare_results = future::join_all(prepare_futures).await;
                let mut preempt_conflicts = BTreeSet::new();
                let mut wait_conflicts = BTreeSet::new();
                for (tablet_id, prepare_result) in prepare_results {
                    match prepare_result {
                        Ok(prepare_ts) => {
                            pending_tablets.remove(&tablet_id);
                            max_prepare_ts = cmp::max(max_prepare_ts, prepare_ts);
                        }
                        Err(PrepareError::Conflict(other_txid)) => {
                            if txid.can_preempt(&other_txid) {
                                preempt_conflicts.insert(other_txid);
                            } else {
                                wait_conflicts.insert(other_txid);
                            }
                        }
                        Err(e) => return Err(WriteError::Other(e.into())),
                    }
                }
                if !wait_conflicts.is_empty() {
                    future::try_join_all(wait_conflicts.iter().cloned().map(
                        |other_txid| async move {
                            let tablet = self.tablets.tablet(other_txid.owner)?;
                            tablet.wait(other_txid).await
                        },
                    ))
                    .await
                    .map_err(|e| WriteError::Other(e.into()))?;
                }
                if !preempt_conflicts.is_empty() {
                    future::try_join_all(preempt_conflicts.iter().cloned().map(
                        |other_txid| async move {
                            let tablet = self.tablets.tablet(other_txid.owner)?;
                            tablet.try_abort(other_txid).await
                        },
                    ))
                    .await
                    .map_err(|e| WriteError::Other(e.into()))?;
                }
            }
            let commit_ts = max_prepare_ts;

            match tablets
                .get(&owner_tablet_id)
                .unwrap()
                .try_commit(txid, commit_ts)
                .await?
            {
                TxOutcome::Committed(commit_ts) => return Ok(commit_ts),
                TxOutcome::Aborted => {
                    txid = txid.next();
                    continue;
                }
            }
        }
        Err(WriteError::Other(anyhow::anyhow!("retries exhausted")))
    }

    fn split_write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<(KeyspaceId, Vec<u8>), Mutation>,
    ) -> anyhow::Result<
        BTreeMap<TabletId, (Vec<Precondition>, BTreeMap<(KeyspaceId, Vec<u8>), Mutation>)>,
    > {
        let mut result = BTreeMap::new();

        for precond in preconds {
            let tablet_id = self
                .router
                .tablet_id_for_key(precond.keyspace_id(), precond.key())?;

            result
                .entry(tablet_id)
                .or_insert_with(|| (vec![], BTreeMap::new()))
                .0
                .push(precond);
        }
        for ((keyspace_id, key), m) in muts {
            let tablet_id = self.router.tablet_id_for_key(keyspace_id, &key)?;
            result
                .entry(tablet_id)
                .or_insert_with(|| (vec![], BTreeMap::new()))
                .1
                .insert((keyspace_id, key), m);
        }

        Ok(result)
    }
}

#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
struct TabletId(u64);

trait Router {
    fn tablet_id_for_key(&self, keyspace_id: KeyspaceId, key: &[u8]) -> anyhow::Result<TabletId>;
}

trait Tablets {
    fn tablet(&self, tablet_id: TabletId) -> anyhow::Result<Box<dyn Tablet + Send>>;
}

#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
struct TxId {
    ts: u64,
    rand: [u8; 16],
    owner: TabletId,
}

impl TxId {
    fn new(owner: TabletId) -> Self {
        TxId {
            ts: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64,
            rand: rand::random(),
            owner,
        }
    }

    fn next(mut self) -> Self {
        self.rand = rand::random();
        self.ts -= 1;
        return self;
    }

    fn can_preempt(&self, other: &TxId) -> bool {
        self < other
    }

    fn to_bytes(&self) -> [u8; 32] {
        let mut out = [0u8; 32];
        BigEndian::write_u64(&mut out[0..8], self.ts);
        out[8..24].copy_from_slice(&self.rand[..]);
        BigEndian::write_u64(&mut out[24..32], self.owner.0);
        out
    }
}

impl TryFrom<&[u8]> for TxId {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != 32 {
            anyhow::bail!("txid not 32 bytes");
        }
        let ts = BigEndian::read_u64(&value[0..8]);
        let mut rand = [0u8; 16];
        rand.copy_from_slice(&value[8..24]);
        let owner = TabletId(BigEndian::read_u64(&value[24..32]));

        Ok(Self { ts, rand, owner })
    }
}

enum TxOutcome {
    Committed(Timestamp),
    Aborted,
}

impl TxOutcome {
    fn to_bytes(&self) -> Vec<u8> {
        todo!()
    }
}

impl Debug for TxId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}/{}", self.ts, hexlify(&self.rand), self.owner.0)
    }
}

impl TryFrom<&[u8]> for TxOutcome {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        todo!()
    }
}

#[derive(Error, Debug)]
enum CommitError {
    #[error("already committed")]
    AlreadyCommitted,
    #[error("already aborted")]
    AlreadyAborted,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Error, Debug)]
enum PrepareError {
    #[error("precondition failed")]
    PreconditionFailed,
    #[error("conflict")]
    Conflict(TxId),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Error, Debug)]
enum ReadError {
    #[error("conflict")]
    Conflict(TxId),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[async_trait]
trait Tablet {
    async fn get(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ReadError>;

    async fn write(
        &self,
        txid: TxId,
        preconds: Vec<Precondition>,
        muts: BTreeMap<(KeyspaceId, Vec<u8>), Mutation>,
    ) -> Result<Timestamp, WriteError>;

    async fn prepare(
        &self,
        txid: TxId,
        preconds: Vec<Precondition>,
        muts: BTreeMap<(KeyspaceId, Vec<u8>), Mutation>,
    ) -> Result<Timestamp, PrepareError>;

    async fn try_commit(&self, txid: TxId, ts: Timestamp) -> anyhow::Result<TxOutcome>;
    async fn try_abort(&self, txid: TxId) -> anyhow::Result<TxOutcome>;
    async fn wait(&self, txid: TxId) -> anyhow::Result<TxOutcome>;
}

struct LsmTablet {
    lsm: Lsm,
    tablets: Box<dyn Tablets + Sync + Send>,
    router: Box<dyn Router + Sync + Send>,
    sequencer: Sequencer,
    lock_mgr: LockMgr,

    waiters: RwLock<
        HashMap<
            TxId,
            (
                tokio::sync::watch::Sender<()>,
                tokio::sync::watch::Receiver<()>,
            ),
        >,
    >,
}

#[async_trait]
impl Tablet for LsmTablet {
    async fn get(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ReadError> {
        self.sequencer.wait_for_safe_read(ts).await?;

        let (maybe_value, maybe_pending_value) = future::try_join(
            self.lsm.get(ts, keyspace_id, &key),
            self.lsm.get_latest(
                keyspace_id
                    .pending()
                    .ok_or_else(|| anyhow::anyhow!("not a userland keyspace"))?,
                &key,
            ),
        )
        .await?;

        if let Some(pending_value) = maybe_pending_value {
            let other_txid = TxId::try_from(&pending_value[..])?;
            return Err(ReadError::Conflict(other_txid));
        }

        Ok(maybe_value)
    }

    async fn write(
        &self,
        txid: TxId,
        preconds: Vec<Precondition>,
        muts: BTreeMap<(KeyspaceId, Vec<u8>), Mutation>,
    ) -> Result<Timestamp, WriteError> {
        todo!()
    }

    async fn prepare(
        &self,
        txid: TxId,
        preconds: Vec<Precondition>,
        muts: BTreeMap<(KeyspaceId, Vec<u8>), Mutation>,
    ) -> Result<Timestamp, PrepareError> {
        todo!()
    }

    async fn try_commit(&self, txid: TxId, ts: Timestamp) -> anyhow::Result<TxOutcome> {
        self.try_write_tx_outcome(txid, TxOutcome::Committed(ts))
            .await
    }

    async fn try_abort(&self, txid: TxId) -> anyhow::Result<TxOutcome> {
        self.try_write_tx_outcome(txid, TxOutcome::Aborted).await
    }

    async fn wait(&self, txid: TxId) -> anyhow::Result<TxOutcome> {
        loop {
            let mut rx = {
                let tx_outcome_key = txid.to_bytes();
                let _guard = self
                    .lock_mgr
                    .lock(std::iter::once(&tx_outcome_key[..]), std::iter::empty())
                    .await;

                if let Some(tx_outcome) = self
                    .lsm
                    .get_latest(KeyspaceId::TX_OUTCOMES, &tx_outcome_key[..])
                    .await?
                    .map(|bytes| TxOutcome::try_from(&bytes[..]))
                    .transpose()?
                {
                    return Ok(tx_outcome);
                }

                let mut waiters = self.waiters.write().unwrap();
                let (_, rx) = waiters
                    .entry(txid)
                    .or_insert_with(|| tokio::sync::watch::channel(()));
                rx.clone()
            };
            _ = rx.changed().await;
        }
    }
}

impl LsmTablet {
    fn new(
        lsm: Lsm,
        tablets: Box<dyn Tablets + Sync + Send>,
        router: Box<dyn Router + Sync + Send>,
    ) -> Self {
        Self {
            lsm,
            tablets,
            router,
            sequencer: Sequencer::new(),
            lock_mgr: LockMgr::new(16384),
            waiters: RwLock::new(HashMap::new()),
        }
    }

    async fn try_write_tx_outcome(
        &self,
        txid: TxId,
        tx_outcome: TxOutcome,
    ) -> anyhow::Result<TxOutcome> {
        let tx_outcome_key = txid.to_bytes();
        let _guard = self
            .lock_mgr
            .lock(std::iter::empty(), std::iter::once(&tx_outcome_key[..]))
            .await;
        let maybe_tx_outcome = self
            .lsm
            .get_latest(KeyspaceId::TX_OUTCOMES, &tx_outcome_key[..])
            .await?
            .map(|bytes| TxOutcome::try_from(&bytes[..]))
            .transpose()?;
        if let Some(existing_tx_outcome) = maybe_tx_outcome {
            return Ok(existing_tx_outcome);
        }
        self.lsm
            .write(
                Timestamp::ZERO,
                vec![],
                BTreeMap::from([(
                    (KeyspaceId::TX_OUTCOMES, tx_outcome_key.to_vec()),
                    Mutation::Put(tx_outcome.to_bytes()),
                )]),
            )
            .await
            .map_err(|e| CommitError::Other(e.into()))?;
        let waiters = self.waiters.write().unwrap();
        if let Some((tx, _)) = waiters.get(&txid) {
            _ = tx.send(());
        }
        Ok(tx_outcome)
    }

    async fn resolve(&self, txid: TxId) -> anyhow::Result<()> {
        let owner_tablet = self.tablets.tablet(txid.owner)?;
        let tx_outcome = owner_tablet.wait(txid).await?;
        let mut muts = BTreeMap::new();
        let (pending_ts, pending_keys) = self.pending_keys(txid);
        let _guard = self
            .lock_mgr
            .lock(
                std::iter::empty(),
                pending_keys.iter().map(|(_, key, _)| &key[..]),
            )
            .await;

        let resolve_ts = match tx_outcome {
            TxOutcome::Committed(commit_ts) => commit_ts,
            TxOutcome::Aborted => Timestamp(pending_ts.0 + 1),
        };
        for (keyspace_id, key, value) in pending_keys {
            let m = match value {
                Value::Regular(v) => Mutation::Put(v),
                Value::Tombstone => Mutation::Delete,
            };
            if let TxOutcome::Committed(_) = tx_outcome {
                muts.insert((keyspace_id, key.clone()), m);
            }
            muts.insert(
                (
                    keyspace_id
                        .pending()
                        .ok_or_else(|| anyhow::anyhow!("non userland keyspace"))?,
                    key,
                ),
                Mutation::Delete,
            );
        }
        self.lsm.write(resolve_ts, vec![], muts).await?;
        Ok(())
    }

    fn pending_keys(&self, txid: TxId) -> (Timestamp, Vec<(KeyspaceId, Vec<u8>, Value)>) {
        todo!();
    }
}
