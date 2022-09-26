use std::cmp;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::time::Duration;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::future;
use rand::Rng;
use thiserror::Error;

use crate::types::Mutation;
use crate::types::Precondition;
use crate::types::Timestamp;
use crate::types::WriteError;
use crate::util::hexlify;

struct Obsidian {
    router: Router,
    tablets: Tablets,
}

impl Obsidian {
    pub async fn get(&self, ts: Timestamp, key: Vec<u8>) -> anyhow::Result<Option<Vec<u8>>> {
        let tablet_id = self.router.tablet_for_key(&key)?;
        let txid = TxId::new(tablet_id);
        let tablet = self.tablets.tablet(tablet_id)?;
        tablet.get(txid, ts, key).await
    }

    pub async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Vec<u8>, Mutation>,
    ) -> Result<Timestamp, WriteError> {
        let write_by_tablet = self.split_write(preconds, muts)?;

        if write_by_tablet.len() == 1 {
            let (tablet_id, (preconds, muts)) = write_by_tablet.into_iter().next().unwrap();

            return self.tablets.tablet(tablet_id)?.write(preconds, muts).await;
        }

        let owner_tablet_id = write_by_tablet
            .keys()
            .skip(rand::thread_rng().gen_range(0..write_by_tablet.len()))
            .next()
            .unwrap();
        let mut txid = TxId::new(*owner_tablet_id);

        for i in 0..10 {
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
                            tablet.abort_or_resolve(other_txid).await
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
                .commit(txid, commit_ts)
                .await
            {
                Ok(_) => return Ok(commit_ts),
                Err(CommitError::AlreadyAborted) => {
                    txid = txid.next();
                    continue;
                }
                Err(e) => return Err(WriteError::Other(e.into())),
            }
        }
        Err(WriteError::Other(anyhow::anyhow!("retries exhausted")))
    }

    fn split_write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Vec<u8>, Mutation>,
    ) -> anyhow::Result<BTreeMap<TabletId, (Vec<Precondition>, BTreeMap<Vec<u8>, Mutation>)>> {
        let mut result = BTreeMap::new();

        for precond in preconds {
            let tablet_id = self.router.tablet_for_key(precond.key())?;

            result
                .entry(tablet_id)
                .or_insert_with(|| (vec![], BTreeMap::new()))
                .0
                .push(precond);
        }
        for (key, m) in muts {
            let tablet_id = self.router.tablet_for_key(&key)?;
            result
                .entry(tablet_id)
                .or_insert_with(|| (vec![], BTreeMap::new()))
                .1
                .insert(key, m);
        }

        Ok(result)
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
struct TabletId(u32);

struct Router {}

impl Router {
    fn tablet_for_key(&self, key: &[u8]) -> anyhow::Result<TabletId> {
        todo!()
    }
}

struct Tablets {}

impl Tablets {
    fn tablet(&self, tablet_id: TabletId) -> anyhow::Result<Box<dyn Tablet>> {
        todo!();
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
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
}

enum TxOutcome {
    Committed(Timestamp),
    Aborted,
}

impl Debug for TxId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}/{}", self.ts, hexlify(&self.rand), self.owner.0)
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

#[async_trait]
trait Tablet {
    async fn get(&self, txid: TxId, ts: Timestamp, key: Vec<u8>)
        -> anyhow::Result<Option<Vec<u8>>>;

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Vec<u8>, Mutation>,
    ) -> Result<Timestamp, WriteError>;

    async fn prepare(
        &self,
        txid: TxId,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Vec<u8>, Mutation>,
    ) -> Result<Timestamp, PrepareError>;

    async fn commit(&self, txid: TxId, ts: Timestamp) -> Result<(), CommitError>;
    async fn abort_or_resolve(&self, txid: TxId) -> anyhow::Result<()>;
    async fn wait(&self, txid: TxId) -> anyhow::Result<TxOutcome>;
    async fn cleanup(&self, txid: TxId, ts: Timestamp) -> anyhow::Result<()>;
}
