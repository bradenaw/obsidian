use std::cell::RefCell;
use std::cmp;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::Context;
use byteorder::BigEndian;
use byteorder::ByteOrder;
use futures::future;
use rand::Rng;
use thiserror::Error;

use crate::range::Bound;
use crate::tablet::Tablet;
use crate::types::ColoGroupId;
use crate::types::Direction;
use crate::types::KeyspaceId;
use crate::types::Mutation;
use crate::types::Precondition;
use crate::types::ShardId;
use crate::types::Timestamp;
use crate::types::WriteError;
use crate::util::hexlify;
use crate::util::sleep_for_retry;
use crate::util::Retry;

struct Obsidian {
    router: Box<dyn Router>,
    tablets: Box<dyn Tablets>,
}

const MAX_CONFLICT_RETRIES: usize = 10;

impl Obsidian {
    fn new(router: Box<dyn Router>, tablets: Box<dyn Tablets>) -> Self {
        Self { router, tablets }
    }

    pub async fn get(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        key: Vec<u8>,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let tablet_id = self.router.tablet_id_for_key(keyspace_id.0, &key)?;
        let tablet = self.tablets.tablet(tablet_id)?;
        let txid = Txid::new(tablet_id);
        let already_seen_conflicts = RefCell::new(HashSet::new());

        Retry::new()
            .n_attempts(MAX_CONFLICT_RETRIES + 1)
            .with_retry(|| async {
                let mut already_seen_conflicts = already_seen_conflicts.borrow_mut();
                match tablet.get(ts, keyspace_id, key.clone()).await {
                    Ok(v) => return Ok(v),
                    Err(InternalError::Conflict(other_txid)) => {
                        if already_seen_conflicts.contains(&other_txid) {
                            return Err(InternalError::Conflict(other_txid));
                        }
                        let other_txid_owner_tablet = self.tablets.tablet(other_txid.owner)?;
                        if txid.can_preempt(&other_txid) {
                            other_txid_owner_tablet.try_abort(other_txid).await?;
                        } else {
                            other_txid_owner_tablet.wait(other_txid).await?;
                        }
                        already_seen_conflicts.insert(other_txid);
                        Err(InternalError::Conflict(other_txid))
                    }
                    Err(e) => return Err(e.into()),
                }
            })
            .await
            .context("too much contention")
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
        let mut txid = Txid::new(*owner_tablet_id);

        // TODO: move into loop, since need to resolve conflicts
        if write_by_tablet.len() == 1 {
            let (tablet_id, (preconds, muts)) = write_by_tablet.into_iter().next().unwrap();

            return self
                .tablets
                .tablet(tablet_id)?
                .write(txid, preconds, muts)
                .await
                .map_err(|e| match e {
                    InternalError::PreconditionFailed => WriteError::PreconditionFailed,
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
                        Err(InternalError::Conflict(other_txid)) => {
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
                .tablet_id_for_key(precond.keyspace_id().0, precond.key())?;

            result
                .entry(tablet_id)
                .or_insert_with(|| (vec![], BTreeMap::new()))
                .0
                .push(precond);
        }
        for ((keyspace_id, key), m) in muts {
            let tablet_id = self.router.tablet_id_for_key(keyspace_id.0, &key)?;
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
pub(crate) struct TabletId(pub ShardId, pub u64);

impl TabletId {
    pub(crate) const META: Self = TabletId(ShardId(1), 1);
}

impl std::fmt::Display for TabletId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}/{}", self.0 .0, self.1)
    }
}

impl std::fmt::Debug for TabletId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        f.write_str("tablet:")?;
        std::fmt::Display::fmt(self, f)
    }
}

pub(crate) trait Router {
    fn tablet_id_for_key(&self, colo_group_id: ColoGroupId, key: &[u8])
        -> anyhow::Result<TabletId>;

    fn tablet_id_for_bound(
        &self,
        colo_group_id: ColoGroupId,
        bound: Bound<&[u8]>,
        direction: Direction,
    ) -> anyhow::Result<TabletId>;
}

pub(crate) trait Tablets {
    fn tablet(&self, tablet_id: TabletId) -> anyhow::Result<Box<dyn Tablet + Send + Sync>>;
}

#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct Txid {
    ts: u64,
    rand: [u8; 16],
    owner: TabletId,
}

impl Txid {
    pub const ENCODED_LEN: usize = 36;

    pub fn new(owner: TabletId) -> Self {
        Txid {
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

    pub fn can_preempt(&self, other: &Txid) -> bool {
        self < other
    }

    pub fn owner(&self) -> TabletId {
        self.owner
    }

    pub fn to_bytes(&self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0u8; Self::ENCODED_LEN];
        BigEndian::write_u64(&mut out[0..8], self.ts);
        out[8..24].copy_from_slice(&self.rand[..]);
        BigEndian::write_u32(&mut out[24..28], self.owner.0 .0);
        BigEndian::write_u64(&mut out[28..36], self.owner.1);
        out
    }
}

impl TryFrom<&[u8]> for Txid {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != Txid::ENCODED_LEN {
            anyhow::bail!("txid not {} bytes", Txid::ENCODED_LEN);
        }
        let ts = BigEndian::read_u64(&value[0..8]);
        let mut rand = [0u8; 16];
        rand.copy_from_slice(&value[8..24]);
        let owner = TabletId(
            ShardId(BigEndian::read_u32(&value[24..28])),
            BigEndian::read_u64(&value[28..36]),
        );

        Ok(Self { ts, rand, owner })
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum TxOutcome {
    Committed(Timestamp),
    Aborted,
}

impl Debug for Txid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{}/{}",
            self.ts,
            hexlify(&self.rand),
            self.owner.0 .0,
            self.owner.1
        )
    }
}

#[derive(Error, Debug)]
pub(crate) enum InternalError {
    #[error("conflict")]
    Conflict(Txid),
    #[error("already committed")]
    AlreadyCommitted,
    #[error("already aborted")]
    AlreadyAborted,
    #[error("precondition failed")]
    PreconditionFailed,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use crate::lsm::LsmBuilder;
    use crate::range::Bound;
    use crate::range::Range;
    use crate::range::RangeSet;
    use crate::router::StaticRouter;
    use crate::storage::MemStorage;
    use crate::tablet::LsmTablet;
    use crate::tablet::Tablet;
    use crate::types::ColoGroupId;
    use crate::types::Direction;
    use crate::types::HistoryRange;
    use crate::types::KeyspaceId;
    use crate::types::Mutation;
    use crate::types::Precondition;
    use crate::types::ShardId;
    use crate::types::Timestamp;
    use crate::types::Value;

    use super::InternalError;
    use super::Obsidian;
    use super::Router;
    use super::TabletId;
    use super::Tablets;
    use super::TxOutcome;
    use super::Txid;

    impl<T: Router> Router for Arc<T> {
        fn tablet_id_for_key(
            &self,
            colo_group_id: ColoGroupId,
            key: &[u8],
        ) -> anyhow::Result<TabletId> {
            T::tablet_id_for_key(&self, colo_group_id, key)
        }

        fn tablet_id_for_bound(
            &self,
            colo_group_id: ColoGroupId,
            bound: Bound<&[u8]>,
            direction: Direction,
        ) -> anyhow::Result<TabletId> {
            T::tablet_id_for_bound(&self, colo_group_id, bound, direction)
        }
    }

    #[async_trait]
    impl<T: Tablet + Send + Sync> Tablet for Arc<T> {
        async fn get(
            &self,
            ts: Timestamp,
            keyspace_id: KeyspaceId,
            key: Vec<u8>,
        ) -> Result<Option<Vec<u8>>, InternalError> {
            T::get(self, ts, keyspace_id, key).await
        }

        async fn get_latest(
            &self,
            keyspace_id: KeyspaceId,
            key: &[u8],
        ) -> Result<(Timestamp, Option<Vec<u8>>), InternalError> {
            T::get_latest(self, keyspace_id, key).await
        }

        async fn scan_page(
            &self,
            ts: Timestamp,
            keyspace_id: KeyspaceId,
            range: Range<&[u8]>,
            direction: Direction,
            limit: usize,
        ) -> anyhow::Result<(Vec<(Vec<u8>, Timestamp, Vec<u8>)>, Option<Range<Vec<u8>>>)> {
            T::scan_page(self, ts, keyspace_id, range, direction, limit).await
        }

        async fn history_page(
            &self,
            ts: Timestamp,
            keyspace_id: KeyspaceId,
            key: &[u8],
            range: HistoryRange,
            direction: Direction,
            limit: usize,
        ) -> anyhow::Result<(Vec<(Timestamp, Value)>, Option<HistoryRange>)> {
            T::history_page(self, ts, keyspace_id, key, range, direction, limit).await
        }

        async fn write(
            &self,
            txid: Txid,
            preconds: Vec<Precondition>,
            muts: BTreeMap<(KeyspaceId, Vec<u8>), Mutation>,
        ) -> Result<Timestamp, InternalError> {
            T::write(self, txid, preconds, muts).await
        }

        async fn prepare(
            &self,
            txid: Txid,
            preconds: Vec<Precondition>,
            muts: BTreeMap<(KeyspaceId, Vec<u8>), Mutation>,
        ) -> Result<Timestamp, InternalError> {
            T::prepare(self, txid, preconds, muts).await
        }

        async fn try_commit(
            &self,
            txid: Txid,
            ts: Timestamp,
            precond_keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
            mut_keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
        ) -> anyhow::Result<TxOutcome> {
            T::try_commit(self, txid, ts, precond_keys, mut_keys).await
        }

        async fn try_abort(&self, txid: Txid) -> anyhow::Result<TxOutcome> {
            T::try_abort(self, txid).await
        }

        async fn wait(&self, txid: Txid) -> anyhow::Result<TxOutcome> {
            T::wait(self, txid).await
        }

        async fn cleanup_committed(
            &self,
            txid: Txid,
            ts: Timestamp,
            precond_keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
            mut_keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
        ) -> anyhow::Result<()> {
            T::cleanup_committed(self, txid, ts, precond_keys, mut_keys).await
        }
    }

    struct StaticTablets {
        m: Mutex<HashMap<TabletId, Arc<LsmTablet>>>,
    }

    impl Tablets for Arc<StaticTablets> {
        fn tablet(&self, tablet_id: TabletId) -> anyhow::Result<Box<dyn Tablet + Send + Sync>> {
            Ok(Box::new(
                self.m
                    .lock()
                    .unwrap()
                    .get(&tablet_id)
                    .ok_or_else(|| anyhow::anyhow!("no tablet for {}", tablet_id))?
                    .clone(),
            ))
        }
    }

    #[tokio::test]
    async fn test_2pc() {
        async fn inner() -> anyhow::Result<()> {
            let tablets = Arc::new(StaticTablets {
                m: Mutex::new(HashMap::new()),
            });

            let colo_group_id = ColoGroupId(1);
            let keyspace_id = KeyspaceId(colo_group_id, 1);

            let tablet_id1 = TabletId(ShardId(1), 1);
            let tablet_id2 = TabletId(ShardId(2), 1);

            let router = Arc::new(StaticRouter::new(
                vec![(
                    colo_group_id,
                    (vec![Bound::Before(vec![1])], vec![tablet_id1, tablet_id2]),
                )]
                .into_iter()
                .collect(),
            ));

            let storage = Arc::new(MemStorage::new());
            let tablet1 = LsmTablet::new(
                tablet_id1,
                LsmBuilder::new().storage(storage.clone()).build().await?,
                vec![(
                    colo_group_id,
                    RangeSet::from(Range {
                        lower: Bound::BeforeAll,
                        upper: Bound::Before(vec![1]),
                    }),
                )]
                .into_iter()
                .collect(),
                Box::new(tablets.clone()),
                Box::new(router.clone()),
            )
            .await?;
            tablet1.create_keyspace(keyspace_id).await?;
            let tablet2 = LsmTablet::new(
                tablet_id2,
                LsmBuilder::new().storage(storage.clone()).build().await?,
                vec![(
                    colo_group_id,
                    RangeSet::from(Range {
                        lower: Bound::Before(vec![1]),
                        upper: Bound::AfterAll,
                    }),
                )]
                .into_iter()
                .collect(),
                Box::new(tablets.clone()),
                Box::new(router.clone()),
            )
            .await?;
            tablet2.create_keyspace(keyspace_id).await?;

            {
                let mut m = tablets.m.lock().unwrap();
                m.insert(TabletId(ShardId(1), 1), Arc::new(tablet1));
                m.insert(TabletId(ShardId(2), 1), Arc::new(tablet2));
            }

            let obs = Obsidian::new(Box::new(router.clone()), Box::new(tablets));

            let key1 = vec![0];
            let key2 = vec![1];

            let write_ts = obs
                .write(
                    vec![],
                    BTreeMap::from([
                        ((keyspace_id, key1.clone()), Mutation::Put(vec![1, 2, 3])),
                        ((keyspace_id, key2.clone()), Mutation::Put(vec![4, 5, 6])),
                    ]),
                )
                .await?;

            assert_eq!(
                obs.get(write_ts, keyspace_id, key1).await?,
                Some(vec![1, 2, 3])
            );
            assert_eq!(
                obs.get(write_ts, keyspace_id, key2).await?,
                Some(vec![4, 5, 6])
            );

            Ok(())
        }

        if let Err(e) = inner().await {
            println!("{:?}", e);
            assert!(false);
        }
    }
}
