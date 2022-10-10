use std::cmp;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::time::Duration;
use std::time::SystemTime;

use byteorder::BigEndian;
use byteorder::ByteOrder;
use byteorder::LittleEndian;
use futures::future;
use rand::Rng;
use thiserror::Error;

use crate::tablet::Tablet;
use crate::types::KeyspaceId;
use crate::types::Mutation;
use crate::types::Precondition;
use crate::types::Timestamp;
use crate::types::WriteError;
use crate::util::hexlify;
use crate::util::sleep_for_retry;

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
                sleep_for_retry(
                    i as usize,
                    Duration::from_millis(10),
                    Duration::from_millis(5000),
                )
                .await;
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
        anyhow::bail!("too much contention");
    }

    pub async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<(KeyspaceId, Vec<u8>), Mutation>,
    ) -> Result<Timestamp, WriteError> {
        let write_by_tablet = self.split_write(preconds.clone(), muts.clone())?;

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
                .await
                .map_err(|e| match e {
                    InternalWriteError::PreconditionFailed => WriteError::PreconditionFailed,
                    e => WriteError::Other(e.into()),
                });
        }

        let mut already_seen_conflicts = HashSet::new();
        for i in 0..MAX_CONFLICT_RETRIES {
            if i != 0 {
                sleep_for_retry(
                    i as usize,
                    Duration::from_millis(10),
                    Duration::from_millis(5000),
                )
                .await;
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

            let mut j = 0;
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
                let mut saw_an_already_seen = false;
                for (tablet_id, prepare_result) in prepare_results {
                    match prepare_result {
                        Ok(prepare_ts) => {
                            pending_tablets.remove(&tablet_id);
                            max_prepare_ts = cmp::max(max_prepare_ts, prepare_ts);
                        }
                        Err(InternalWriteError::Conflict(other_txid)) => {
                            if already_seen_conflicts.contains(&other_txid) {
                                saw_an_already_seen = true;
                            } else if txid.can_preempt(&other_txid) {
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

                    for other_txid in wait_conflicts {
                        already_seen_conflicts.insert(other_txid);
                    }
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
                    for other_txid in preempt_conflicts {
                        already_seen_conflicts.insert(other_txid);
                    }
                }
                if saw_an_already_seen {
                    sleep_for_retry(j, Duration::from_millis(10), Duration::from_millis(5000))
                        .await;
                }
                j += 1
            }
            let commit_ts = max_prepare_ts;

            match tablets
                .get(&owner_tablet_id)
                .unwrap()
                .try_commit(
                    txid,
                    commit_ts,
                    preconds
                        .iter()
                        .map(|precond| (precond.keyspace_id(), precond.key().to_vec()))
                        .collect(),
                    muts.keys().cloned().collect(),
                )
                .await?
            {
                TxOutcome::Committed(commit_ts) => return Ok(commit_ts),
                TxOutcome::Aborted => {
                    txid = txid.next();
                    continue;
                }
            }
        }
        Err(WriteError::Other(anyhow::anyhow!("too much contention")))
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
pub(crate) struct TabletId(u64);

pub(crate) trait Router {
    fn tablet_id_for_key(&self, keyspace_id: KeyspaceId, key: &[u8]) -> anyhow::Result<TabletId>;
}

pub(crate) trait Tablets {
    fn tablet(&self, tablet_id: TabletId) -> anyhow::Result<Box<dyn Tablet + Send + Sync>>;
}

#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct TxId {
    ts: u64,
    rand: [u8; 16],
    owner: TabletId,
}

impl TxId {
    pub const ENCODED_LEN: usize = 32;

    pub fn new(owner: TabletId) -> Self {
        TxId {
            ts: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64,
            rand: rand::random(),
            owner,
        }
    }

    pub fn next(mut self) -> Self {
        self.rand = rand::random();
        self.ts -= 1;
        return self;
    }

    pub fn can_preempt(&self, other: &TxId) -> bool {
        self < other
    }

    pub fn owner(&self) -> TabletId {
        self.owner
    }

    pub fn to_bytes(&self) -> [u8; 32] {
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

#[derive(Clone, Copy, Debug)]
pub(crate) enum TxOutcome {
    Committed(Timestamp),
    Aborted,
}

impl TxOutcome {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            TxOutcome::Aborted => {
                vec![0]
            }
            TxOutcome::Committed(ts) => {
                let mut out = vec![0; 9];
                out[1] = 1;
                LittleEndian::write_u64(&mut out[1..], ts.as_nanos());
                out
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
                Ok(TxOutcome::Aborted)
            }
            1 => {
                if b.len() != 9 {
                    anyhow::bail!("invalid tx outcome: wrong length");
                }
                let ts = Timestamp::from_nanos(BigEndian::read_u64(&b[1..]));
                Ok(TxOutcome::Committed(ts))
            }
            _ => anyhow::bail!("invalid tx outcome: tag not 0 or 1"),
        }
    }
}
impl Debug for TxId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}/{}", self.ts, hexlify(&self.rand), self.owner.0)
    }
}

#[derive(Error, Debug)]
pub(crate) enum CommitError {
    #[error("already committed")]
    AlreadyCommitted,
    #[error("already aborted")]
    AlreadyAborted,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Error, Debug)]
pub(crate) enum InternalWriteError {
    #[error("precondition failed")]
    PreconditionFailed,
    #[error("conflict")]
    Conflict(TxId),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Error, Debug)]
pub(crate) enum ReadError {
    #[error("conflict")]
    Conflict(TxId),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
