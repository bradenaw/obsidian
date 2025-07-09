use std::cmp;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use byteorder::BigEndian;
use byteorder::ByteOrder;
use futures::future;
use futures::stream::FuturesUnordered;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use rand::seq::SliceRandom;
use rand::Rng;
use thiserror::Error;

use crate::meta::Meta;
use crate::meta::MetaSynced;
use crate::pb;
use crate::range::Bound;
use crate::range::Range;
use crate::tablet::Tablet;
use crate::types::ColoGroupId;
use crate::types::Direction;
use crate::types::Key;
use crate::types::KeyspaceId;
use crate::types::Mutation;
use crate::types::Precondition;
use crate::types::Record;
use crate::types::ShardId;
use crate::types::Timestamp;
use crate::types::WriteError;
use crate::util::hexlify;
use crate::util::sleep_for_retry;
use crate::util::Decode;
use crate::util::Encode;
use crate::util::Retry;
use crate::util::RetryResult;

#[async_trait]
pub trait Obsidian {
    async fn get(&self, ts: Timestamp, key: &Key) -> anyhow::Result<Option<Record>>;

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<Record>, Option<Range<Vec<u8>>>)>;

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> anyhow::Result<Timestamp>;

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, WriteError>;

    async fn create_colo_group(
        &self,
        colo_group_id: ColoGroupId,
        initial_splits: Vec<Bound<Vec<u8>>>,
    ) -> anyhow::Result<()>;

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()>;
}

pub trait ObsidianExt {
    fn scan(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<Vec<u8>>,
        direction: Direction,
    ) -> Box<dyn Stream<Item = anyhow::Result<Record>> + Send + '_>;
}

impl<T: Obsidian + Sync> ObsidianExt for T {
    // TODO: This needs to give access to the underlying cursor in case it gets interrupted between
    // results (e.g. timing out between yielding two results).
    fn scan(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<Vec<u8>>,
        direction: Direction,
    ) -> Box<dyn Stream<Item = anyhow::Result<Record>> + Send + '_> {
        Box::new(try_stream! {
            let mut maybe_cursor = Some(range);
            while let Some(cursor) = maybe_cursor {
                let (page, continue_cursor) = self.scan_page(
                    ts,
                    keyspace_id,
                    cursor.borrow(),
                    direction,
                    1000, // page_size
                ).await?;

                for record in page {
                    yield record;
                }

                maybe_cursor = continue_cursor;
            }
        })
    }
}

pub(crate) struct Frontend {
    meta: Box<dyn Meta + Send + Sync>,
    meta_synced: MetaSynced,
    shards: Box<dyn Shards + Send + Sync>,
}

const MAX_CONFLICT_RETRIES: usize = 10;

#[async_trait]
impl Obsidian for Frontend {
    async fn get(&self, ts: Timestamp, key: &Key) -> anyhow::Result<Option<Record>> {
        let keyspace_id = key.0;
        self.with_resolve_conflicts(|| async move {
            let tablet_id = self.meta_synced.tablet_id_for_key(keyspace_id.0, &key.1)?;
            let tablet = self.shards.tablet(tablet_id)?;
            tablet.get(ts, key).await
        })
        .await
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<Record>, Option<Range<Vec<u8>>>)> {
        self.with_resolve_conflicts(|| async move {
            let start_bound = match direction {
                Direction::Asc => range.lower,
                Direction::Desc => range.upper,
            };
            let tablet_id =
                self.meta_synced
                    .tablet_id_for_bound(keyspace_id.0, start_bound, direction)?;

            let tablet = self.shards.tablet(tablet_id)?;
            tablet
                .scan_page(ts, keyspace_id, range, direction, limit)
                .await
        })
        .await
    }

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> anyhow::Result<Timestamp> {
        let mut by_tablet = BTreeMap::new();
        for (keyspace_id, key) in &keys {
            let tablet_id = self.meta_synced.tablet_id_for_key(keyspace_id.0, &key)?;
            by_tablet
                .entry(tablet_id)
                .or_insert_with(BTreeSet::new)
                .insert((*keyspace_id, key.clone()));
        }
        let mut futures = FuturesUnordered::new();
        for (tablet_id, keys) in by_tablet.into_iter() {
            // TODO: with a little more information, we could get away with at most *one* round of
            // conflict resolution.
            futures.push(self.with_resolve_conflicts(move || {
                let tablet = self.shards.tablet(tablet_id);
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

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
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
                    async move { self.shards.tablet(tablet_id)?.write(preconds, muts).await }
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
                    self.shards
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
                            let tablet = self.shards.tablet(other_txid.owner)?;
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
                            let tablet = self.shards.tablet(other_txid.owner)?;
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
            // We have to commit at a _higher_ timestamp so that the resolution of the pending
            // records is at a higher timestamp than the pending records themselves.
            let commit_ts = max_prepare_ts.plus_one();

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

    async fn create_colo_group(
        &self,
        colo_group_id: ColoGroupId,
        initial_splits: Vec<Bound<Vec<u8>>>,
    ) -> anyhow::Result<()> {
        self.meta
            .create_colo_group(colo_group_id, initial_splits)
            .await?;

        // TODO: if the colo group already exists, we want to still sync_meta here, so that the
        // caller doesn't get an "already exists" and then try to use it and immediately get back a
        // "doesn't exist" because that node hasn't learned about it yet.

        self.sync_meta().await?;

        Ok(())
    }

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        self.meta.create_keyspace(keyspace_id).await?;

        self.sync_meta().await?;

        Ok(())
    }
}

impl Frontend {
    pub(crate) fn new(
        meta: Box<dyn Meta + Send + Sync>,
        meta_synced: MetaSynced,
        shards: Box<dyn Shards + Send + Sync>,
    ) -> Self {
        Self {
            meta,
            meta_synced,
            shards,
        }
    }

    fn split_write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> anyhow::Result<BTreeMap<TabletId, (Vec<Precondition>, BTreeMap<Key, Mutation>)>> {
        let mut result = BTreeMap::new();

        for precond in preconds {
            let tablet_id = self
                .meta_synced
                .tablet_id_for_key(precond.keyspace_id().0, precond.key())?;

            result
                .entry(tablet_id)
                .or_insert_with(|| (vec![], BTreeMap::new()))
                .0
                .push(precond);
        }
        for (key, m) in muts {
            let tablet_id = self.meta_synced.tablet_id_for_key(key.0 .0, &key.1)?;
            result
                .entry(tablet_id)
                .or_insert_with(|| (vec![], BTreeMap::new()))
                .1
                .insert(key, m);
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

        let already_seen_conflicts = Mutex::new(HashSet::new());

        Retry::new()
            .n_attempts(MAX_CONFLICT_RETRIES + 1)
            .with_retry(|| async {
                match f().await {
                    Ok(v) => return RetryResult::Ok(v),
                    Err(InternalError::Conflict(other_txid)) => {
                        // If we've already seen this txid as a conflict that means we already
                        // wait/aborted it and we're still just waiting for it to get cleaned up,
                        // so just take another turn around the retry loop.
                        if already_seen_conflicts.lock().unwrap().contains(&other_txid) {
                            return RetryResult::Retry(InternalError::Conflict(other_txid));
                        }

                        let other_txid_owner_tablet = match self.shards.tablet(other_txid.owner) {
                            Ok(tablet_id) => tablet_id,
                            Err(e) => {
                                return RetryResult::Err(InternalError::Other(e));
                            }
                        };
                        if txid.can_preempt(&other_txid) {
                            log::debug!("{:?} preempting {:?}", txid, other_txid);
                            if let Err(e) = other_txid_owner_tablet.try_abort(other_txid).await {
                                return RetryResult::Err(InternalError::Other(e));
                            }
                        } else {
                            log::debug!("{:?} waiting for {:?}", txid, other_txid);
                            match other_txid_owner_tablet.wait(other_txid).await {
                                // TxOutcomeMissing happens if we raced with the cleanup that
                                // removed it, but that means the pending/preconds are gone so
                                // retrying will work.
                                Ok(_) | Err(InternalError::TxOutcomeMissing) => {}
                                Err(e) => {
                                    return RetryResult::Err(e.into());
                                }
                            }
                        }

                        already_seen_conflicts.lock().unwrap().insert(other_txid);
                        RetryResult::Retry(InternalError::Conflict(other_txid))
                    }
                    Err(e) => {
                        log::info!("error returned by inner: {:?}", e);
                        return RetryResult::Err(e.into());
                    }
                }
            })
            .await
    }

    async fn sync_meta(&self) -> anyhow::Result<()> {
        let ts = self.meta.latest_snapshot().await?;

        self.meta_synced.wait(ts).await?;

        let tablet_ids = {
            let mut tablet_ids = self.meta.tablet_ids(ts).await?;
            tablet_ids.shuffle(&mut rand::thread_rng());
            tablet_ids
        };

        log::info!("sync_meta() to {:?} for {:?} tablets", ts, tablet_ids.len());

        futures::stream::iter(tablet_ids.into_iter())
            .map(|tablet_id| async move {
                log::info!("wait_meta_sync({:?}) for {:?}", ts, tablet_id);
                self.shards.tablet(tablet_id)?.wait_meta_sync(ts).await?;
                log::info!("wait_meta_sync({:?}) for {:?} -> done", ts, tablet_id);
                Ok::<_, anyhow::Error>(())
            })
            .buffer_unordered(64)
            .try_collect::<Vec<_>>()
            .await?;

        Ok(())
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

impl From<TabletId> for pb::internal::TabletId {
    fn from(value: TabletId) -> Self {
        Self {
            shard_id: value.0 .0,
            id: value.1,
        }
    }
}

impl TryFrom<pb::internal::TabletId> for TabletId {
    type Error = anyhow::Error;

    fn try_from(value: pb::internal::TabletId) -> Result<Self, Self::Error> {
        Ok(Self(ShardId(value.shard_id), value.id))
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

pub(crate) trait Shards {
    fn shard(&self, shard_id: ShardId) -> anyhow::Result<Box<dyn Shard + Sync + Send>>;

    fn shards(&self) -> Vec<Box<dyn Shard + Sync + Send>>;

    fn tablet(&self, tablet_id: TabletId) -> anyhow::Result<Box<dyn Tablet + Send + Sync>> {
        self.shard(tablet_id.0)?.tablet(tablet_id)
    }
}

#[async_trait]
pub(crate) trait Shard {
    fn id(&self) -> ShardId;

    async fn create_tablet(
        &self,
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
    ) -> anyhow::Result<TabletId>;

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

impl Debug for Txid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tx:{}/{}/{}/{}",
            self.ts,
            hexlify(&self.rand),
            self.owner.0 .0,
            self.owner.1
        )
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum TxOutcome {
    Committed(Timestamp),
    Aborted,
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
    // Can happen on an attempt at a wait() if Tablet::cleanup_committed_outcomes already
    // cleaned everything up and removed the TxOutcome.
    #[error("TxOutcome missing")]
    TxOutcomeMissing,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[cfg(test)]
mod test {
    use crate::test::obsidian_test_suite;

    obsidian_test_suite!(crate::test::new_for_test);
}
