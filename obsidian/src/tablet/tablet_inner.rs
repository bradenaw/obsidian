use std::cmp;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;

use anyhow::anyhow;
use async_stream::try_stream;
use futures::future;
use futures::Stream;

use crate::lsm::Manifest;
use crate::tablet::lock_mgr::Guard;
use crate::tablet::lock_mgr::LockMgr;
use crate::tablet::protected::LsmRead;
use crate::tablet::protected::LsmReadWrite;
use crate::tablet::protected::ProtectedLsm;
use crate::tablet::sequencer::Sequencer;
use crate::tablet::tablet_journal_writer::TabletJournalWriter;
use crate::util::Decode;
use crate::util::Encode;
use crate::ColoGroupId;
use crate::Direction;
use crate::HistoryRange;
use crate::InternalError;
use crate::Key;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Precondition;
use crate::Range;
use crate::Record;
use crate::Revision;
use crate::RevisionValue;
use crate::TabletId;
use crate::TabletJournalEntry;
use crate::Timestamp;
use crate::Txid;

pub(super) struct TabletInner {
    pub tablet_id: TabletId,
    pub colo_group_id: ColoGroupId,
    pub range: Range<Vec<u8>>,

    pub lsm: ProtectedLsm,
    pub journal: Arc<dyn TabletJournalWriter>,
    pub sequencer: Sequencer,
    pub lock_mgr: LockMgr,
}

impl TabletInner {
    pub(super) fn new(
        tablet_id: TabletId,
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
        lsm: ProtectedLsm,
        journal: Arc<dyn TabletJournalWriter>,
    ) -> Self {
        Self {
            tablet_id,
            colo_group_id,
            range,
            lsm,
            journal,
            sequencer: Sequencer::new(),
            lock_mgr: LockMgr::new(1),
        }
    }

    pub(super) async fn get(
        &self,
        ts: Timestamp,
        key: &Key,
    ) -> Result<Option<Record>, InternalError> {
        self.sequencer.wait_for_safe_read(ts).await?;

        let lsm_read = self.lsm.read()?;

        let keyspace_id = key.0;
        self.check_key(keyspace_id.0, &key.1)?;

        let key_future = lsm_read.get(ts, keyspace_id, &key.1);
        let (maybe_record, maybe_pending_value) = match keyspace_id.pending() {
            Some(pending_keyspace_id) => {
                future::try_join(key_future, lsm_read.get(ts, pending_keyspace_id, &key.1)).await?
            }
            None => (key_future.await?, None),
        };

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

    pub(super) async fn get_latest(
        &self,
        key: Key,
    ) -> Result<(Timestamp, Option<Record>), InternalError> {
        let lsm_read = self.lsm.read()?;

        let keyspace_id = key.0;
        self.check_key(keyspace_id.0, &key.1)?;

        let _guard = self.lock_mgr.read_lock(&key.1).await;

        let safe_read_ts = self.sequencer.safe_read_ts();

        let key_future = Self::unsafe_get_latest_record(&lsm_read, keyspace_id, &key.1);

        let (maybe_record, maybe_pending_value) = match keyspace_id.pending() {
            Some(pending_keyspace_id) => {
                future::try_join(
                    key_future,
                    Self::unsafe_get_latest_record(&lsm_read, pending_keyspace_id, &key.1),
                )
                .await?
            }
            None => (key_future.await?, None),
        };

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

    pub(super) async fn latest_snapshot(
        &self,
        keys: BTreeSet<Key>,
    ) -> Result<Timestamp, InternalError> {
        // TODO: This doesn't require loading the values, so we could optimize here to do less
        // work.
        let mut result = Timestamp::ZERO;
        for key in keys {
            let (ts, _) = self.get_latest(key).await?;
            result = cmp::max(result, ts);
        }
        Ok(result)
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

        let lsm_read = self.lsm.read()?;

        // range                          |-----------|
        // self.range               |---------|
        // scan_range                     |---|

        // Ask the LSM for the page. Note that the returned continuation is in terms of the
        // constrained range that we asked it for, not the entire range from the request.
        let (page, intersecting_continue_cursor) = lsm_read
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

        let lsm_read = self.lsm.read()?;
        let _guard = self.lock_mgr.read_lock(&key.1).await;

        let (page, continue_cursor) = lsm_read
            .history_page(keyspace_id, &key.1, range, direction, limit)
            .await?;

        if let Some(pending_keyspace_id) = keyspace_id.pending() {
            let maybe_pending =
                Self::unsafe_get_latest_record(&lsm_read, pending_keyspace_id, &key.1).await?;

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
                    ts: ts,
                    value: value,
                })
                .collect(),
            continue_cursor,
        ))
    }

    pub(super) async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        // XXX: This is not tolerant of timeouts, since that might cause us to release the locks
        // before the write completes.

        let lsm_rw = self.lsm.read_write()?;
        let _guard = self.acquire_write_locks(&preconds, &muts).await?;

        if let Some(conflict_txid) = Self::check_write_conflicts(&lsm_rw, &preconds, &muts).await? {
            return Err(InternalError::Conflict(conflict_txid));
        }

        self.check_preconds(&lsm_rw, &preconds).await?;

        let ts = self.sequencer.start_write();

        self.journal
            .append(TabletJournalEntry::Write(
                *ts,
                muts.iter()
                    .map(|((keyspace_id, key), mutation)| {
                        let value = match mutation {
                            Mutation::Put(value) => RevisionValue::Regular(value.clone()),
                            Mutation::Delete => RevisionValue::Tombstone,
                        };
                        (*keyspace_id, key.clone(), value)
                    })
                    .collect(),
            ))
            .await?;

        lsm_rw.write(*ts, muts);

        Ok(*ts)
    }

    // TODO: make this take a lockmgr guard that proves the lock is held
    pub(super) async fn unsafe_get_latest_record<R: LsmRead>(
        lsm_read: &R,
        keyspace_id: KeyspaceId,
        key: &[u8],
    ) -> anyhow::Result<Option<(Timestamp, RevisionValue)>> {
        lsm_read.get(Timestamp(u64::MAX), keyspace_id, key).await
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
    pub(super) async fn check_write_conflicts<R: LsmRead>(
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
            if let Some(pending_keyspace_id) = keyspace_id.pending() {
                if let Some((_, RevisionValue::Regular(value))) =
                    Self::unsafe_get_latest_record(lsm_read, pending_keyspace_id, key).await?
                {
                    let other_txid = Txid::decode(&value[..Txid::ENCODED_LEN])?;
                    return Ok(Some(other_txid));
                }
            }
        }
        Ok(None)
    }

    // Scans the entirety of `range` by calling scan_page repeatedly.
    pub(super) fn scan_all(
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

    pub(super) async fn check_preconds<R: LsmRead>(
        &self,
        lsm_read: &R,
        preconds: &[Precondition],
    ) -> Result<(), InternalError> {
        for precond in preconds {
            let res =
                Self::unsafe_get_latest_record(lsm_read, precond.keyspace_id(), &precond.key())
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

    pub(super) fn check_key(&self, colo_group_id: ColoGroupId, key: &[u8]) -> anyhow::Result<()> {
        if self.colo_group_id != colo_group_id || !self.range.contains(&key) {
            return Err(anyhow!("{:?}/{:?} not owned", colo_group_id, key).into());
        }

        Ok(())
    }

    pub(super) async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        let lsm_rw = self.lsm.read_write()?;
        lsm_rw.create_keyspace(keyspace_id);
        if let Some(pending_keyspace_id) = keyspace_id.pending() {
            lsm_rw.create_keyspace(pending_keyspace_id);
        }
        if let Some(precond_keyspace_id) = keyspace_id.precond() {
            lsm_rw.create_keyspace(precond_keyspace_id);
        }

        Ok(())
    }

    pub(super) fn manifest(&self) -> Manifest {
        self.lsm.manifest()
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use async_trait::async_trait;

    use crate::lsm::Lsm;
    use crate::lsm::LsmOptions;
    use crate::meta::TabletState;
    use crate::tablet::protected::ProtectedLsm;
    use crate::tablet::tablet_inner::TabletInner;
    use crate::tablet::tablet_journal_writer::TabletJournalWriter;
    use crate::test::MemStorage;
    use crate::ColoGroupId;
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

    struct NoopJournalWriter {}

    #[async_trait]
    impl TabletJournalWriter for NoopJournalWriter {
        async fn append(&self, _entry: TabletJournalEntry) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_write_preconds() -> anyhow::Result<()> {
        let lsm = Lsm::empty(LsmOptions::default(), Arc::new(MemStorage::new())).await?;

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
            ProtectedLsm::new(tablet_id, lsm, TabletState::Active),
            Arc::new(NoopJournalWriter {}),
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
}
