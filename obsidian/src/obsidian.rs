use std::cell::RefCell;
use std::cmp;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::fmt::Debug;
use std::future::Future;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::anyhow;
use anyhow::Context;
use byteorder::BigEndian;
use byteorder::ByteOrder;
use futures::future;
use futures::stream::FuturesUnordered;
use futures::TryStreamExt;
use rand::Rng;
use thiserror::Error;

use crate::range::Bound;
use crate::range::Range;
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
use crate::util::Decode;
use crate::util::Encode;
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
        self.with_resolve_conflicts(|| {
            let key = key.clone();
            async move {
                let tablet_id = self.router.tablet_id_for_key(keyspace_id.0, &key)?;
                let tablet = self.tablets.tablet(tablet_id)?;
                tablet.get(ts, keyspace_id, key.clone()).await
            }
        })
        .await
    }

    pub async fn latest_snapshot(
        &self,
        keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
    ) -> anyhow::Result<Timestamp> {
        let mut by_tablet = BTreeMap::new();
        for (keyspace_id, key) in &keys {
            let tablet_id = self.router.tablet_id_for_key(keyspace_id.0, &key)?;
            by_tablet
                .entry(tablet_id)
                .or_insert_with(BTreeSet::new)
                .insert((*keyspace_id, &key[..]));
        }
        let mut futures = FuturesUnordered::new();
        for (tablet_id, keys) in by_tablet.into_iter() {
            // TODO: with a little more information, we could get away with at most *one* round of
            // conflict resolution.
            futures.push(self.with_resolve_conflicts(move || {
                let tablet = self.tablets.tablet(tablet_id);
                let keys = keys.clone();
                async move { tablet?.latest_snapshot(keys).await }
            }));
        }
        let mut result = Timestamp::ZERO;
        while let Some(ts) = futures.try_next().await? {
            result = cmp::max(ts, result);
        }

        Ok(result)
    }

    pub async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<(Vec<u8>, Timestamp, Vec<u8>)>, Option<Range<Vec<u8>>>)> {
        self.with_resolve_conflicts(|| async move {
            let start_bound = match direction {
                Direction::Asc => range.lower,
                Direction::Desc => range.upper,
            };
            let tablet_id =
                self.router
                    .tablet_id_for_bound(keyspace_id.0, start_bound, direction)?;

            let tablet = self.tablets.tablet(tablet_id)?;
            tablet
                .scan_page(ts, keyspace_id, range, direction, limit)
                .await
        })
        .await
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

        if write_by_tablet.len() == 1 {
            let (tablet_id, (preconds, muts)) = write_by_tablet.into_iter().next().unwrap();
            return self
                .with_resolve_conflicts(|| {
                    let preconds = preconds.clone();
                    let muts = muts.clone();
                    async move {
                        self.tablets
                            .tablet(tablet_id)?
                            .write(txid, preconds, muts)
                            .await
                    }
                })
                .await
                .map_err(|e| {
                    match e.downcast_ref::<InternalError>() {
                        Some(InternalError::PreconditionFailed) => {
                            return WriteError::PreconditionFailed;
                        }
                        _ => {}
                    }
                    e.into()
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

    async fn with_resolve_conflicts<F, Fut, T>(&self, f: F) -> anyhow::Result<T>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, InternalError>>,
    {
        // We can use an 'arbitrary' txid here even with a nonexistent tablet because this
        // never surfaces anywhere else, we just use it to decide if we can preempt other
        // transactions.
        let txid = Txid::new(TabletId(ShardId(0), 0));

        let already_seen_conflicts = RefCell::new(HashSet::new());

        Retry::new()
            .n_attempts(MAX_CONFLICT_RETRIES + 1)
            .with_retry(|| async {
                let mut already_seen_conflicts = already_seen_conflicts.borrow_mut();

                match f().await {
                    Ok(v) => return Ok(v),
                    Err(InternalError::Conflict(other_txid)) => {
                        // If we've already seen this txid as a conflict that means we already
                        // wait/aborted it and we're still just waiting for it to get cleaned up,
                        // so just take another turn around the retry loop.
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
}

#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct TabletId(pub ShardId, pub u64);

impl TabletId {
    pub(crate) const ENCODED_LEN: usize = 12;
    pub(crate) const META: Self = TabletId(ShardId(1), 1);
}

impl TabletId {
    pub(crate) fn encode_fixed(&self) -> [u8; 12] {
        let mut out = [0u8; 12];
        BigEndian::write_u32(&mut out[..4], self.0 .0);
        BigEndian::write_u64(&mut out[4..], self.1);
        out
    }
}

impl Encode for TabletId {
    fn encoded_size_estimate(&self) -> usize {
        Self::ENCODED_LEN
    }

    fn encode(&self, w: &mut Vec<u8>) {
        w.extend_from_slice(&self.encode_fixed()[..]);
    }
}

impl Decode for TabletId {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() != 12 {
            return Err(anyhow!(
                "tablet ID must be 12 bytes, got {}: {}",
                b.len(),
                hexlify(b)
            ));
        }
        return Ok(TabletId(
            ShardId(BigEndian::read_u32(&b[0..4])),
            BigEndian::read_u64(&b[4..12]),
        ));
    }
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

    pub(crate) fn encode_fixed(&self) -> [u8; Self::ENCODED_LEN] {
        // Encode with tablet ID first so that they're routed properly as a part of TABLET_META
        // when used as a key.
        let mut out = [0u8; Self::ENCODED_LEN];
        BigEndian::write_u32(&mut out[0..4], self.owner.0 .0);
        BigEndian::write_u64(&mut out[4..12], self.owner.1);
        BigEndian::write_u64(&mut out[12..20], self.ts);
        out[20..36].copy_from_slice(&self.rand[..]);
        out
    }
}

impl Encode for Txid {
    fn encoded_size_estimate(&self) -> usize {
        Self::ENCODED_LEN
    }

    fn encode(&self, w: &mut Vec<u8>) {
        w.extend_from_slice(&self.encode_fixed()[..]);
    }
}

impl Decode for Txid {
    fn decode(value: &[u8]) -> anyhow::Result<Self> {
        if value.len() != Txid::ENCODED_LEN {
            anyhow::bail!("txid not {} bytes", Txid::ENCODED_LEN);
        }
        let owner = TabletId(
            ShardId(BigEndian::read_u32(&value[0..4])),
            BigEndian::read_u64(&value[4..12]),
        );
        let ts = BigEndian::read_u64(&value[12..20]);
        let mut rand = [0u8; 16];
        rand.copy_from_slice(&value[20..36]);

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

        async fn latest_snapshot(
            &self,
            keys: BTreeSet<(KeyspaceId, &[u8])>,
        ) -> Result<Timestamp, InternalError> {
            T::latest_snapshot(self, keys).await
        }

        async fn scan_page(
            &self,
            ts: Timestamp,
            keyspace_id: KeyspaceId,
            range: Range<&[u8]>,
            direction: Direction,
            limit: usize,
        ) -> Result<(Vec<(Vec<u8>, Timestamp, Vec<u8>)>, Option<Range<Vec<u8>>>), InternalError>
        {
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

    async fn new_with_single_byte_routing(n_tablets: usize) -> anyhow::Result<Obsidian> {
        let tablets = Arc::new(StaticTablets {
            m: Mutex::new(HashMap::new()),
        });

        let colo_group_id = ColoGroupId(1);
        let keyspace_id = KeyspaceId(colo_group_id, 1);

        // META gets everything up to the first split.
        let mut tablet_ids = vec![TabletId::META];
        let mut splits = vec![];
        for i in 0..n_tablets {
            let shard_id = ShardId(((i % 2) + 1) as u32);
            tablet_ids.push(TabletId(shard_id, (i + 2) as u64));
            splits.push(Bound::Before(vec![(i + 1) as u8]));
        }

        let router = Arc::new(StaticRouter::new(
            vec![(colo_group_id, (splits.clone(), tablet_ids.clone()))]
                .into_iter()
                .collect(),
        ));

        let storage = Arc::new(MemStorage::new());

        for (i, tablet_id) in tablet_ids.iter().enumerate() {
            let range = Range {
                lower: if i == 0 {
                    Bound::BeforeAll
                } else {
                    splits[i - 1].clone()
                },
                upper: if i == tablet_ids.len() - 1 {
                    Bound::AfterAll
                } else {
                    splits[i].clone()
                },
            };
            println!("{:?} owns {:?}", tablet_id, range);
            let tablet = LsmTablet::new(
                *tablet_id,
                LsmBuilder::new().storage(storage.clone()).build().await?,
                vec![(colo_group_id, RangeSet::from(range))]
                    .into_iter()
                    .collect(),
                Box::new(tablets.clone()),
                Box::new(router.clone()),
            )
            .await?;
            tablet.create_keyspace(keyspace_id).await?;
            let mut m = tablets.m.lock().unwrap();
            m.insert(*tablet_id, Arc::new(tablet));
        }

        Ok(Obsidian::new(Box::new(router.clone()), Box::new(tablets)))
    }

    #[tokio::test]
    async fn test_2pc() -> anyhow::Result<()> {
        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
        let obs = new_with_single_byte_routing(2).await?;

        let key1 = vec![1];
        let key2 = vec![2];

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

    #[tokio::test]
    async fn test_scan_page() {
        async fn inner() -> anyhow::Result<()> {
            let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
            let obs = new_with_single_byte_routing(3).await?;

            let writes: [(Vec<u8>, _); 12] = [
                //          ts=0123456789
                (vec![1, 0], b" o  o    o"),
                (vec![1, 1], b"   o     o"),
                (vec![1, 2], b"   o x    "),
                (vec![1, 3], b"   oxo    "),
                (vec![2, 0], b"    o   o "),
                (vec![2, 1], b"     o  o "),
                (vec![2, 2], b" o x  o  o"),
                (vec![3, 0], b"  o oxo  o"),
                (vec![3, 1], b"  o  oo o "),
                (vec![3, 2], b" xoxoxoxox"),
                (vec![3, 3], b"        o "),
                (vec![3, 4], b" ooooooooo"),
            ];

            let mut timestamps = vec![Timestamp(0)];
            for ts_idx in 1..writes[0].1.len() {
                let mut mutations = BTreeMap::new();
                for (key, versions) in &writes {
                    let mutation = match versions[ts_idx] {
                        b'o' => Mutation::Put(format!("{:?} {}", key, ts_idx).into()),
                        b'x' => Mutation::Delete,
                        _ => continue,
                    };

                    mutations.insert((keyspace_id, key.clone()), mutation);
                }

                if mutations.is_empty() {
                    timestamps.push(timestamps.last().cloned().unwrap_or(Timestamp(0)));
                    continue;
                }

                let ts = obs.write(vec![], mutations).await?;
                timestamps.push(ts);
            }

            async fn check(
                obs: &Obsidian,
                timestamps: &[Timestamp],
                ts_idx: usize,
                range: Range<&[u8]>,
                expected: Vec<(Vec<u8>, usize)>,
            ) -> anyhow::Result<()> {
                let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
                for direction in [Direction::Asc, Direction::Desc] {
                    for page_size in 1..=expected.len() {
                        let mut maybe_cursor = Some(range.to_vec());
                        let mut results = vec![];
                        while let Some(cursor) = maybe_cursor {
                            let (page, continue_cursor) = obs
                                .scan_page(
                                    timestamps[ts_idx],
                                    keyspace_id,
                                    cursor.borrow(),
                                    direction,
                                    page_size,
                                )
                                .await?;

                            assert!(page.len() <= page_size);
                            results.extend(page);
                            assert_ne!(continue_cursor, Some(cursor));
                            maybe_cursor = continue_cursor;
                        }

                        if direction == Direction::Desc {
                            results.reverse();
                        }

                        assert_eq!(
                            results,
                            expected
                                .clone()
                                .into_iter()
                                .map(|(key, ts_idx)| (
                                    key.clone(),
                                    timestamps[ts_idx],
                                    format!("{:?} {}", key, ts_idx).into(),
                                ))
                                .collect::<Vec<_>>(),
                            "scan_page(ts={:?}, /*keyspace_id*/, /*cursor*/, direction={:?}, page_size={})",
                            timestamps[ts_idx],
                            direction,
                            page_size,
                        );
                    }
                }

                Ok(())
            }

            check(
                &obs,
                &timestamps,
                5,
                Range {
                    lower: Bound::Before(&[1, 1]),
                    upper: Bound::After(&[2, 0]),
                },
                vec![(vec![1, 1], 3), (vec![1, 3], 5), (vec![2, 0], 4)],
            )
            .await?;

            check(
                &obs,
                &timestamps,
                4,
                Range::all(),
                vec![
                    (vec![1, 0], 4),
                    (vec![1, 1], 3),
                    (vec![1, 2], 3),
                    // [1,3] got deleted at 4
                    (vec![2, 0], 4),
                    // [2,1] doesn't exist yet
                    // [2,2] got deleted at 3
                    (vec![3, 0], 4),
                    (vec![3, 1], 2),
                    (vec![3, 2], 4),
                    // [3,3] doesn't exist yet
                    (vec![3, 4], 4),
                ],
            )
            .await?;

            Ok(())
        }

        if let Err(e) = inner().await {
            panic!("{:?}", e);
        }
    }
}
