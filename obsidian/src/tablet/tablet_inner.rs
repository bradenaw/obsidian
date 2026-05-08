use std::cmp;
use std::cmp::max;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;

use anyhow::anyhow;
use async_stream::try_stream;
use futures::future;
use futures::Stream;
use obsidian_util::Decode;
use obsidian_util::Encode;

use crate::tablet::journaled_lsm::LsmWrite;
use crate::tablet::lock_mgr::Guard;
use crate::tablet::lock_mgr::LockMgr;
use crate::tablet::read_only_lsm::LsmRead;
use crate::tablet::scan_locks::ScanLocks;
use crate::tablet::sequencer::Sequencer;
use crate::ColoGroupId;
use crate::Direction;
use crate::HistoryRange;
use crate::InternalError;
use crate::Key;
use crate::KeyspaceId;
use crate::Manifest;
use crate::Mutation;
use crate::Precondition;
use crate::Range;
use crate::Record;
use crate::Revision;
use crate::RevisionValue;
use crate::TabletId;
use crate::Timestamp;
use crate::Txid;

pub(super) struct TabletInner<L> {
    pub tablet_id: TabletId,
    pub colo_group_id: ColoGroupId,
    pub range: Range<Vec<u8>>,

    pub lsm: L,
    pub sequencer: Sequencer,
    pub lock_mgr: LockMgr,
    pub scan_locks: ScanLocks,
}

impl<L> TabletInner<L>
where
    L: LsmRead,
{
    pub(super) fn new(
        tablet_id: TabletId,
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
        lsm: L,
    ) -> Self {
        Self {
            tablet_id,
            colo_group_id,
            range,
            lsm,
            sequencer: Sequencer::new(),
            lock_mgr: LockMgr::new(1 << 16 /*buckets*/),
            scan_locks: ScanLocks::new(),
        }
    }

    pub(super) async fn get(
        &self,
        ts: Timestamp,
        key: &Key,
    ) -> Result<Option<Record>, InternalError> {
        Ok(self
            .get_multi(ts, BTreeSet::from([key.clone()]))
            .await?
            .remove(key))
    }

    pub(super) async fn get_multi(
        &self,
        ts: Timestamp,
        keys: BTreeSet<Key>,
    ) -> Result<BTreeMap<Key, Record>, InternalError> {
        self.sequencer.wait_for_safe_read(ts).await?;

        // This lock is only necessary to protect against cleanups doing upgrades (pending->real),
        // because they're non-transactional. For everything else, the wait_for_safe_read above is
        // sufficient.
        let _guard = self
            .lock_mgr
            .read_lock_all(keys.iter().map(|(_, key_bytes)| &key_bytes[..]))
            .await;

        let mut results = BTreeMap::new();
        for key in keys {
            let keyspace_id = key.0;
            self.check_key(keyspace_id.0, &key.1)?;

            let key_future = self.lsm.get(ts, keyspace_id, &key.1);
            let (maybe_record, maybe_pending_value) = match keyspace_id.pending() {
                Some(pending_keyspace_id) => {
                    future::try_join(key_future, self.lsm.get(ts, pending_keyspace_id, &key.1))
                        .await?
                }
                None => (key_future.await?, None),
            };

            if let Some((_, RevisionValue::Regular(bytes))) = maybe_pending_value {
                let pending_mut = PendingMutation::decode(&bytes)?;
                return Err(InternalError::Conflict(pending_mut.txid));
            }

            if let Some((ts, RevisionValue::Regular(value))) = maybe_record {
                results.insert(key.clone(), Record { key, ts, value });
            }

            // TODO: InternalError::PartialGet if too big
        }

        Ok(results)
    }

    pub(super) async fn get_latest_multi(
        &self,
        keys: BTreeSet<Key>,
    ) -> Result<(Timestamp, BTreeMap<Key, Record>), InternalError> {
        for key in &keys {
            self.check_key(key.0 .0, &key.1)?;
        }

        let _guard = self
            .lock_mgr
            .read_lock_all(keys.iter().map(|(_, key_bytes)| &key_bytes[..]))
            .await;
        let safe_read_ts = self.sequencer.safe_read_ts();
        let mut snapshot_ts = Timestamp::ZERO;

        let mut results = BTreeMap::new();

        for key in keys {
            let keyspace_id = key.0;

            let key_future = self.unsafe_get_latest_record(keyspace_id, &key.1);

            let (maybe_record, maybe_pending_value) = match keyspace_id.pending() {
                Some(pending_keyspace_id) => {
                    future::try_join(
                        key_future,
                        self.unsafe_get_latest_record(pending_keyspace_id, &key.1),
                    )
                    .await?
                }
                None => (key_future.await?, None),
            };

            if let Some((_, RevisionValue::Regular(bytes))) = maybe_pending_value {
                let pending_mut = PendingMutation::decode(&bytes)?;
                return Err(InternalError::Conflict(pending_mut.txid));
            }

            match maybe_record {
                Some((ts, revision_value)) => {
                    snapshot_ts = max(snapshot_ts, ts);
                    if let RevisionValue::Regular(value) = revision_value {
                        results.insert(key.clone(), Record { key, ts, value });
                    }
                }
                None => {
                    snapshot_ts = safe_read_ts;
                }
            }
        }

        Ok((snapshot_ts, results))
    }

    pub(super) async fn latest_snapshot(
        &self,
        keys: BTreeSet<Key>,
    ) -> Result<Timestamp, InternalError> {
        // TODO: This doesn't require loading the values, so we could optimize here to do less
        // work.
        let (ts, _) = self.get_latest_multi(keys).await?;
        Ok(ts)
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

        let _guard = self.scan_locks.scan();

        // range                          |-----------|
        // self.range               |---------|
        // scan_range                     |---|

        // Ask the LSM for the page. Note that the returned continuation is in terms of the
        // constrained range that we asked it for, not the entire range from the request.
        let (page, intersecting_continue_cursor) = self
            .lsm
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

        let _guard = self.lock_mgr.read_lock(&key.1).await;

        let (page, continue_cursor) = self
            .lsm
            .history_page(keyspace_id, &key.1, range, direction, limit)
            .await?;

        if let Some(pending_keyspace_id) = keyspace_id.pending() {
            let maybe_pending = self
                .unsafe_get_latest_record(pending_keyspace_id, &key.1)
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
        }

        Ok((
            page.into_iter()
                .map(|(ts, value)| Revision {
                    key: key.clone(),
                    ts,
                    value,
                })
                .collect(),
            continue_cursor,
        ))
    }

    // TODO: make this take a lockmgr guard that proves the lock is held
    pub(super) async fn unsafe_get_latest_record(
        &self,
        keyspace_id: KeyspaceId,
        key: &[u8],
    ) -> anyhow::Result<Option<(Timestamp, RevisionValue)>> {
        self.lsm.get(Timestamp(u64::MAX), keyspace_id, key).await
    }

    // Scans the entirety of `range` by calling scan_page repeatedly.
    pub(super) fn scan_all(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<Vec<u8>>,
        direction: Direction,
    ) -> impl Stream<Item = Result<Record, InternalError>> + '_ {
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

    pub(super) fn check_key(&self, colo_group_id: ColoGroupId, key: &[u8]) -> anyhow::Result<()> {
        if self.colo_group_id != colo_group_id || !self.range.contains(&key) {
            return Err(anyhow!("{:?}/{:?} not owned", colo_group_id, key));
        }

        Ok(())
    }

    pub(super) fn manifest(&self) -> Manifest {
        self.lsm.manifest()
    }
}

impl<L> TabletInner<L>
where
    L: LsmRead + LsmWrite,
{
    pub(super) async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        // XXX: This is not tolerant of timeouts, since that might cause us to release the locks
        // before the write completes.

        let _guard = self.acquire_write_locks(&preconds, &muts).await?;

        if let Some(conflict_txid) = self.check_write_conflicts(&preconds, &muts).await? {
            return Err(InternalError::Conflict(conflict_txid));
        }

        self.check_preconds(&preconds).await?;

        let ts = self.sequencer.start_write();

        self.lsm.write(*ts, muts).await?;

        Ok(*ts)
    }

    pub(super) async fn acquire_write_locks<'a>(
        &'a self,
        preconds: &Vec<Precondition>,
        muts: &BTreeMap<Key, Mutation>,
    ) -> anyhow::Result<Guard<'a>> {
        for precond in preconds {
            self.check_key(precond.keyspace_id().0, precond.key())?;
        }
        for (keyspace_id, key) in muts.keys() {
            self.check_key(keyspace_id.0, key)?;
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
    pub(super) async fn check_write_conflicts(
        &self,
        preconds: &[Precondition],
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
                if let Some((_, RevisionValue::Regular(value))) = self
                    .unsafe_get_latest_record(pending_keyspace_id, key)
                    .await?
                {
                    let other_txid = Txid::decode(&value[..Txid::ENCODED_LEN])?;
                    return Ok(Some(other_txid));
                }
            }
        }
        Ok(None)
    }

    pub(super) async fn check_preconds(
        &self,
        preconds: &[Precondition],
    ) -> Result<(), InternalError> {
        for precond in preconds {
            let res = self
                .unsafe_get_latest_record(precond.keyspace_id(), precond.key())
                .await?;
            match precond {
                Precondition::NotChangedSince(_, _, ts) => {
                    if let Some((last_write_ts, _)) = res {
                        if last_write_ts > *ts {
                            return Err(InternalError::PreconditionFailed);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        if keyspace_id.0 != self.colo_group_id {
            return Err(anyhow!(
                "cannot create {:?} in tablet for {:?}",
                keyspace_id,
                self.colo_group_id
            ));
        }
        self.lsm.create_keyspace(keyspace_id).await?;
        if let Some(pending_keyspace_id) = keyspace_id.pending() {
            self.lsm.create_keyspace(pending_keyspace_id).await?;
        }
        if let Some(precond_keyspace_id) = keyspace_id.precond() {
            self.lsm.create_keyspace(precond_keyspace_id).await?;
        }

        Ok(())
    }
}

fn scan_all<F, Fut>(
    f: F,
    ts: Timestamp,
    keyspace_id: KeyspaceId,
    range: Range<Vec<u8>>,
    direction: Direction,
) -> impl Stream<Item = Result<Record, InternalError>>
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

pub(super) struct PendingMutation {
    pub txid: Txid,
    pub m: Mutation,
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

pub(super) struct PrecondLocks {
    pub txids: BTreeSet<Txid>,
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
        if !b.len().is_multiple_of(Txid::ENCODED_LEN) {
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

#[cfg(test)]
mod tests {
    use std::cmp::max;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::StreamExt;
    use obsidian_external::mem::MemStorage;
    use obsidian_lsm::Lsm;
    use obsidian_lsm::LsmOptions;
    use obsidian_util::encode;

    use crate::tablet::journaled_lsm::JournaledLsm;
    use crate::tablet::tablet_inner::PendingMutation;
    use crate::tablet::tablet_inner::TabletInner;
    use crate::tablet::tablet_journal_writer::TabletJournalWriter;
    use crate::Bound;
    use crate::ColoGroupId;
    use crate::Direction;
    use crate::InternalError;
    use crate::KeyspaceId;
    use crate::Mutation;
    use crate::Precondition;
    use crate::Range;
    use crate::Record;
    use crate::ShardId;
    use crate::TabletId;
    use crate::TabletJournalEntry;
    use crate::Timestamp;
    use crate::Txid;

    struct NoopJournalWriter {}

    #[async_trait]
    impl TabletJournalWriter for NoopJournalWriter {
        async fn append(&self, _entry: TabletJournalEntry) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_write_preconds() -> anyhow::Result<()> {
        let lsm = Lsm::empty(LsmOptions::default(), Arc::new(MemStorage::new()));

        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
        let ka = b"a";
        let kb = b"b";

        lsm.create_keyspace(keyspace_id);
        lsm.create_keyspace(keyspace_id.pending().unwrap());

        let tablet_id = TabletId(ShardId(1), 1);
        let tablet_inner = TabletInner::new(
            tablet_id,
            keyspace_id.0,
            Range::all(),
            JournaledLsm::new(lsm, Arc::new(NoopJournalWriter {})),
        );

        let write_0_ts = tablet_inner
            .write(
                vec![],
                BTreeMap::from([
                    ((keyspace_id, ka.to_vec()), Mutation::Put(b"a0".to_vec())),
                    ((keyspace_id, kb.to_vec()), Mutation::Put(b"b0".to_vec())),
                ]),
            )
            .await?;

        let before_write_0_ts = Timestamp(write_0_ts.0 - 1);

        assert!(matches!(
            tablet_inner
                .write(
                    vec![Precondition::NotChangedSince(
                        keyspace_id,
                        ka.to_vec(),
                        before_write_0_ts,
                    )],
                    BTreeMap::from([((keyspace_id, ka.to_vec()), Mutation::Put(b"a1".to_vec()))]),
                )
                .await,
            Err(InternalError::PreconditionFailed),
        ));

        let write_1_ts = tablet_inner
            .write(
                vec![Precondition::NotChangedSince(
                    keyspace_id,
                    ka.to_vec(),
                    write_0_ts,
                )],
                BTreeMap::from([
                    ((keyspace_id, ka.to_vec()), Mutation::Put(b"a1".to_vec())),
                    ((keyspace_id, kb.to_vec()), Mutation::Delete),
                ]),
            )
            .await?;

        let before_write_1_ts = Timestamp(write_1_ts.0 - 1);

        assert_eq!(
            tablet_inner
                .get(before_write_0_ts, &(keyspace_id, ka.to_vec()))
                .await?,
            None
        );
        assert_eq!(
            tablet_inner
                .get(before_write_0_ts, &(keyspace_id, kb.to_vec()))
                .await?,
            None
        );
        assert_eq!(
            tablet_inner
                .get(before_write_1_ts, &(keyspace_id, ka.to_vec()))
                .await?,
            Some(Record {
                key: (keyspace_id, ka.to_vec()),
                ts: write_0_ts,
                value: b"a0".to_vec(),
            })
        );

        assert_eq!(
            tablet_inner
                .get(before_write_1_ts, &(keyspace_id, kb.to_vec()))
                .await?,
            Some(Record {
                key: (keyspace_id, kb.to_vec()),
                ts: write_0_ts,
                value: b"b0".to_vec()
            })
        );
        assert_eq!(
            tablet_inner
                .get(write_1_ts, &(keyspace_id, ka.to_vec()))
                .await?,
            Some(Record {
                key: (keyspace_id, ka.to_vec()),
                ts: write_1_ts,
                value: b"a1".to_vec()
            }),
        );
        assert_eq!(
            tablet_inner
                .get(write_1_ts, &(keyspace_id, kb.to_vec()))
                .await?,
            None,
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_conflict() -> anyhow::Result<()> {
        let _ = pretty_env_logger::try_init();
        let lsm = Lsm::empty(LsmOptions::default(), Arc::new(MemStorage::new()));

        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);

        lsm.create_keyspace(keyspace_id);
        lsm.create_keyspace(keyspace_id.pending().unwrap());

        let tablet_id = TabletId(ShardId(1), 1);
        let tablet_inner = TabletInner::new(
            tablet_id,
            keyspace_id.0,
            Range::all(),
            JournaledLsm::new(lsm, Arc::new(NoopJournalWriter {})),
        );

        let mut ts = Timestamp::ZERO;

        for i in 0..20 {
            ts = max(
                ts,
                tablet_inner
                    .write(
                        vec![],
                        BTreeMap::from([((keyspace_id, vec![i as u8]), Mutation::Put(vec![]))]),
                    )
                    .await?,
            );
        }

        let other_txid = Txid::new(ShardId(0));

        ts = max(
            ts,
            tablet_inner
                .write(
                    vec![],
                    BTreeMap::from([(
                        (keyspace_id.pending().unwrap(), vec![20u8]),
                        Mutation::Put(encode(&PendingMutation {
                            txid: other_txid,
                            m: Mutation::Put(vec![]),
                        })),
                    )]),
                )
                .await?,
        );

        let scan = tablet_inner.scan_all(
            ts,
            keyspace_id,
            Range {
                lower: Bound::Before(vec![0]),
                upper: Bound::After(vec![20]),
            },
            Direction::Asc,
        );

        assert_eq!(
            scan.any(async |result| {
                if let Err(InternalError::Conflict(conflict_txid)) = result {
                    return conflict_txid == other_txid;
                }
                false
            })
            .await,
            true,
        );

        Ok(())
    }
}
