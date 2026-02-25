use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::anyhow;
use futures::stream::StreamExt;
use futures::TryStreamExt;

use crate::lsm::compactor::Compactor;
use crate::lsm::index::Index;
use crate::lsm::index::IndexSnapshot;
use crate::lsm::index::Keyspace;
use crate::lsm::preload::Preloaded;
use crate::lsm::LsmRevision;
use crate::lsm::Manifest;
use crate::runtime::Storage;
use crate::runtime::Wal;
use crate::util::hexlify;
use crate::util::merge_sorted_streams;
use crate::util::shortest_between;
use crate::util::Background;
use crate::util::IteratorEither;
use crate::util::OrdEqByFirst;
use crate::Bound;
use crate::Direction;
use crate::HistoryRange;
use crate::Key;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Range;
use crate::Revision;
use crate::RevisionValue;
use crate::Timestamp;
use crate::WalEntry;
use crate::WalSeq;
use crate::WriteError;

#[derive(Clone)]
pub(crate) struct LsmOptions {
    pub l0_max_size: u64,
    pub run_size_target: u64,
    pub block_size_target: u64,
}

impl Default for LsmOptions {
    fn default() -> Self {
        LsmOptions {
            l0_max_size: 8_000_000,
            run_size_target: 64_000_000,
            block_size_target: 32768,
        }
    }
}

pub(crate) struct Lsm {
    options: LsmOptions,

    wal: Arc<dyn Wal>,
    index: Arc<Index>,
    compactor: Compactor,
    storage: Arc<dyn Storage>,

    bg: Background,
    wal_processed: tokio::sync::watch::Receiver<WalSeq>,
}

impl Lsm {
    pub async fn new(
        options: LsmOptions,
        wal: Arc<dyn Wal>,
        storage: Arc<dyn Storage>,
    ) -> anyhow::Result<Self> {
        let (index, newest_seqno) =
            Self::recovery(options.l0_max_size, &wal, storage.deref()).await?;

        let index_arc = Arc::new(index);

        let bg = Background::new();
        let (wal_processed_send, wal_processed_recv) = tokio::sync::watch::channel(WalSeq(0));
        bg.spawn(Self::process_wal(
            Arc::clone(&index_arc),
            Arc::clone(&wal),
            newest_seqno.unwrap_or(WalSeq(1)),
            wal_processed_send,
            options.l0_max_size,
        ));

        let compactor = Compactor::new(
            Arc::clone(&storage),
            Arc::clone(&index_arc),
            1, // parallelism
            options.run_size_target,
            options.block_size_target,
        );

        Ok(Self {
            options,

            compactor,
            index: index_arc,
            wal,
            storage,
            bg,
            wal_processed: wal_processed_recv,
        })
    }

    pub async fn get(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        key: &[u8],
    ) -> anyhow::Result<Option<(Timestamp, RevisionValue)>> {
        let index_snapshot = self.index.snapshot();
        Self::keyspace(&index_snapshot, keyspace_id)?
            .get(ts, key)
            .await
    }

    pub async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<Revision>, Option<Range<Vec<u8>>>)> {
        let index_snapshot = self.index.snapshot();
        let (page, continue_cursor) = Self::keyspace(&index_snapshot, keyspace_id)?
            .scan_page(ts, range, direction, limit)
            .await?;

        let page = page
            .into_iter()
            .map(|lsm_revision| Revision {
                key: (keyspace_id, lsm_revision.key),
                ts: lsm_revision.ts,
                value: lsm_revision.value,
            })
            .collect();

        Ok((page, continue_cursor))
    }

    pub async fn history_page(
        &self,
        keyspace_id: KeyspaceId,
        key: &[u8],
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<(Timestamp, RevisionValue)>, Option<HistoryRange>)> {
        let index_snapshot = self.index.snapshot();
        Self::keyspace(&index_snapshot, keyspace_id)?
            .history_page(key, range, direction, limit)
            .await
    }

    pub async fn write(
        &self,
        ts: Timestamp,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<(), WriteError> {
        let index_snapshot = self.index.snapshot();

        for key in muts.keys() {
            Self::keyspace(&index_snapshot, key.0)?;
        }

        let seqno = self
            .wal
            .append(WalEntry::Write(
                ts,
                muts.into_iter()
                    .map(|((keyspace_id, key), m)| {
                        let value = match m {
                            Mutation::Put(raw_value) => RevisionValue::Regular(raw_value),
                            Mutation::Delete => RevisionValue::Tombstone,
                        };
                        (keyspace_id, key, value)
                    })
                    .collect(),
            ))
            .await?;

        self.wait_processed(seqno).await?;

        Ok(())
    }

    pub async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        self.create_keyspace_with_depth(keyspace_id, 7 /*depth*/)
            .await
    }

    pub(super) async fn create_keyspace_with_depth(
        &self,
        keyspace_id: KeyspaceId,
        depth: usize,
    ) -> anyhow::Result<()> {
        self.index.create_keyspace(keyspace_id, depth)
    }

    pub async fn pending_compactions(&self) {
        loop {
            let (index_snapshot, changed) = self.index.snapshot_subscribe();
            // TODO: Don't actually need to wait for empty, just for the ones we saw at the
            // beginning to be gone.
            if index_snapshot
                .keyspaces
                .iter()
                .all(|(_, keyspace)| keyspace.l0_active.is_empty() || keyspace.l0_sealed.is_empty())
            {
                break;
            }
            changed.await;
        }
    }

    /// Flush ensures that all writes that have already completed are in runs committed to storage
    /// (i.e. not in L0). Snapshots manifests into the WAL to speed recovery.
    pub async fn flush(&self) -> anyhow::Result<()> {
        let seqno = self.wal.append(WalEntry::NoOp).await?;
        self.wait_processed(seqno).await?;

        let index_snapshot = self.index.snapshot();
        for keyspace_id in index_snapshot.keyspaces.keys() {
            self.index.rotate_l0(*keyspace_id)?;
        }

        self.pending_compactions().await;

        let mut manifest = self.index.snapshot().manifest();
        // Anything in l0 must already be present in the WAL. We can't write the run IDs here
        // because there's nothing for recovery to open.
        for keyspace in manifest.keyspaces.values_mut() {
            keyspace.levels[0].runs = Vec::new();
        }

        self.wal.append(WalEntry::Manifest(seqno, manifest)).await?;

        Ok(())
    }

    pub fn keyspaces(&self) -> Vec<KeyspaceId> {
        self.index.snapshot().keyspaces.keys().copied().collect()
    }

    pub fn manifest(&self) -> Manifest {
        self.index.snapshot().manifest()
    }

    pub fn find_split(&self) -> Option<Bound<Vec<u8>>> {
        let index_snapshot = self.index.snapshot();

        // This is an estimate that relies on the assumption that there are a reasonable number of
        // runs per LSM, say, in the hundreds to thousands, and a relatively small number of
        // keyspaces. That means we can basically ignore the fact that the runs overlap each other
        // among levels and among keyspaces in choosing our split.
        //
        // We're trying to pick a key that splits roughly in half _overall_ but we're splitting
        // across all of the keyspaces, and we want to prefer shorter split points over longer ones
        // because they're more likely to keep relevant data together.

        let mut runs = vec![];
        for (_, keyspace) in &index_snapshot.keyspaces {
            for level in &keyspace.levels {
                for run in &level.runs {
                    runs.push((&run.min_key, run.size()));
                }
            }
        }

        runs.sort_unstable_by(|a, b| Ord::cmp(a.0, b.0));

        let total_size: u64 = runs.iter().map(|(_, size)| *size as u64).sum();

        let mut running_size = 0u64;
        let mut maybe_candidate: Option<Vec<u8>> = None;
        let mut candidate_distance_from_mid = 0u64;
        for (lower, size) in &runs {
            running_size += *size as u64;

            if running_size > total_size / 5 {
                let new_candidate_distance_from_mid =
                    ((running_size as i64) - (total_size as i64 / 2)).abs() as u64;
                match maybe_candidate {
                    Some(ref candidate) => {
                        let new_candidate = shortest_between(runs[0].0, lower);
                        // If they're equal we'd prefer the one closer to the midpoint.
                        if new_candidate.len() < candidate.len()
                            || (new_candidate.len() == candidate.len()
                                && new_candidate_distance_from_mid < candidate_distance_from_mid)
                        {
                            maybe_candidate = Some(new_candidate);
                            candidate_distance_from_mid = new_candidate_distance_from_mid;
                        }
                    }
                    None => {
                        maybe_candidate = Some(lower.to_vec());
                        candidate_distance_from_mid = new_candidate_distance_from_mid;
                    }
                }
            }
            if running_size > total_size * 4 / 5 {
                break;
            }
        }

        maybe_candidate.map(|key| Bound::Before(key))
    }

    fn keyspace(
        snapshot: &IndexSnapshot,
        keyspace_id: KeyspaceId,
    ) -> anyhow::Result<KeyspaceReader<'_>> {
        Ok(KeyspaceReader(
            snapshot
                .keyspaces
                .get(&keyspace_id)
                .ok_or_else(|| anyhow!("{:?} not found", keyspace_id))?,
        ))
    }

    pub async fn load(&self, preloaded: Preloaded) -> anyhow::Result<()> {
        self.index.load(preloaded.snapshot)?;
        // We need to flush here otherwise after a crash and restart we'd lose track of the runs,
        // and could erroneously transition to Active with no data.
        self.flush().await
    }

    pub fn set_splits(&self, splits: Vec<Bound<Vec<u8>>>) {
        self.index.set_splits(splits);
    }

    /// Waits until at least the given sequence number has been processed.
    async fn wait_processed(&self, seqno: WalSeq) -> anyhow::Result<()> {
        let mut wal_processed = self.wal_processed.clone();
        while *wal_processed.borrow_and_update() < seqno {
            wal_processed
                .changed()
                .await
                .map_err(|_| WriteError::Other(anyhow!("wal processor missing")))?;
        }
        Ok(())
    }

    async fn recovery(
        l0_max_size: u64,
        wal: &Arc<dyn Wal>,
        storage: &dyn Storage,
    ) -> anyhow::Result<(Index, Option<WalSeq>)> {
        let oldest_seqno = wal.oldest_available().await?;
        let mut newest_seqno = None;
        let mut wal_stream = wal.read(oldest_seqno);

        let mut entries = VecDeque::new();
        let mut index = Index::new();

        while let Some((seqno, entry)) = wal_stream.try_next().await? {
            match entry {
                WalEntry::NoOp => {}
                WalEntry::Write(ts, kvs) => {
                    entries.push_back((seqno, ts, kvs));
                }
                WalEntry::Manifest(included_seqno, manifest) => {
                    let trim_to_idx = entries
                        .binary_search_by_key(&included_seqno, |(seqno, _, _)| *seqno)
                        .unwrap_or_else(core::convert::identity);
                    entries.drain(0..trim_to_idx);

                    index = Index::from_manifest(storage, manifest).await?;
                }
            }
            newest_seqno = Some(seqno);
        }

        let index_snapshot = index.snapshot();
        for (seqno, ts, kvs) in entries {
            for (keyspace_id, key, value) in kvs {
                // It's possible that this revision is already present since the seqno in
                // WalEntry::Manifest is a lower bound, the manifest may already contain newer
                // writes.
                if let Some((existing_ts, existing_value)) = index_snapshot
                    .keyspaces
                    .get(&keyspace_id)
                    .map(|keyspace| keyspace.l0_active.get(ts, &key[..]))
                    .flatten()
                {
                    if existing_ts == ts {
                        if value != existing_value {
                            return Err(anyhow!(
                                "duplicate revision for {}@{} with differing values",
                                hexlify(&key[..]),
                                ts,
                            ));
                        }
                        continue;
                    }
                }

                index.insert(keyspace_id, seqno, key, ts, value)?;
            }
        }
        for (keyspace_id, keyspace) in &index.snapshot().keyspaces {
            // We're not _really_ respecting l0_max_size but it's not any better to have multiple
            // l0_sealed over one.
            if keyspace.l0_active.size() >= l0_max_size {
                index.rotate_l0(*keyspace_id)?;
            }
        }

        Ok((index, newest_seqno))
    }

    async fn process_wal(
        index: Arc<Index>,
        wal: Arc<dyn Wal>,
        start: WalSeq,
        wal_processed: tokio::sync::watch::Sender<WalSeq>,
        l0_max_size: u64,
    ) {
        // TODO: retry
        Self::process_wal_once(&index, &wal, start, wal_processed, l0_max_size)
            .await
            .unwrap();
    }

    async fn process_wal_once(
        index: &Index,
        wal: &Arc<dyn Wal>,
        start: WalSeq,
        wal_processed: tokio::sync::watch::Sender<WalSeq>,
        l0_max_size: u64,
    ) -> anyhow::Result<()> {
        let mut log = wal.tail(start);
        while let Some((seqno, entry)) = log.try_next().await? {
            match entry {
                WalEntry::NoOp => {}
                WalEntry::Write(ts, kvs) => {
                    for (keyspace_id, key, value) in kvs {
                        log::trace!(
                            "lsm processing write tx {:?} for {}/{}",
                            ts,
                            keyspace_id,
                            hexlify(&key[..])
                        );

                        let new_size = index.insert(keyspace_id, seqno, key, ts, value)?;
                        if new_size > l0_max_size {
                            index.rotate_l0(keyspace_id)?;
                        }
                    }
                }
                WalEntry::Manifest(_, _) => {}
            }
            _ = wal_processed.send(seqno);
        }
        Ok(())
    }

    pub(super) fn index_snapshot(&self) -> Arc<IndexSnapshot> {
        self.index.snapshot()
    }
}

pub(super) struct KeyspaceReader<'a>(pub &'a Keyspace);

impl<'a> KeyspaceReader<'a> {
    async fn get(
        &self,
        ts: Timestamp,
        k: &[u8],
    ) -> anyhow::Result<Option<(Timestamp, RevisionValue)>> {
        if let Some((revision_ts, v)) = self.0.l0_active.get(ts, k) {
            return Ok(Some((revision_ts, v)));
        }
        let maybe_revision = self
            .0
            .l0_sealed
            .iter()
            .map(|memtable| memtable.get(ts, k))
            .filter_map(core::convert::identity)
            .max_by_key(|(ts, _)| *ts);
        if let Some((revision_ts, v)) = maybe_revision {
            return Ok(Some((revision_ts, v)));
        }
        for level in &self.0.levels {
            if let Some(run) = level.run_for_key(k) {
                if let Some((revision_ts, v)) = run.get(ts, k).await? {
                    return Ok(Some((revision_ts, v)));
                }
            }
        }
        Ok(None)
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<LsmRevision>, Option<Range<Vec<u8>>>)> {
        if range.is_empty() {
            return Ok((vec![], None));
        }

        let mut streams = Vec::with_capacity(
            1  // l0_active
                + self.0.l0_sealed.len()
                + self.0.levels.len(),
        );
        {
            let revisions: Vec<_> = self
                .0
                .l0_active
                .scan(ts, range.clone(), direction)
                .map(|revision| Ok(revision))
                .collect();
            streams.push(futures::stream::iter(revisions.into_iter()).boxed());
        }
        for l0_run in &self.0.l0_sealed {
            streams.push(
                futures::stream::iter(
                    l0_run
                        .scan(ts, range.clone(), direction)
                        .map(|revision| Ok(revision)),
                )
                .boxed(),
            );
        }
        for i in 1..self.0.levels.len() {
            let overlapping_runs = self.0.levels[i].range(range.to_vec());

            if overlapping_runs.is_empty() {
                continue;
            }

            streams.push(
                futures::stream::iter(match direction {
                    Direction::Asc => IteratorEither::Left(overlapping_runs.iter()),
                    Direction::Desc => IteratorEither::Right(overlapping_runs.iter().rev()),
                })
                .inspect(|run| {
                    assert!(
                        !run.range().intersection(&range.to_vec()).is_empty(),
                        "trying to scan {:?}, got run with range {:?}",
                        range,
                        run.range()
                    )
                })
                .map(|run| run.scan(ts, range.to_vec(), direction))
                .flatten()
                .boxed(),
            );
        }
        let mut merged = match direction {
            Direction::Asc => merge_sorted_streams(streams).peekable().boxed(),
            Direction::Desc => merge_sorted_streams(
                streams
                    .into_iter()
                    .map(|stream| {
                        stream.map(|result| {
                            result.map(|revision| {
                                OrdEqByFirst(
                                    (Reverse(revision.key), Reverse(revision.ts)),
                                    revision.value,
                                )
                            })
                        })
                    })
                    .collect(),
            )
            .map(|result| {
                result.map(
                    |OrdEqByFirst((Reverse(key), Reverse(ts)), value)| LsmRevision {
                        key,
                        ts,
                        value,
                    },
                )
            })
            .peekable()
            .boxed(),
        };

        let mut page = vec![];
        while let Some(revision) = merged.next().await.transpose()? {
            if let Some(LsmRevision {
                key: last_key,
                ts: last_ts,
                ..
            }) = page.last()
            {
                if last_key == &revision.key {
                    assert!(
                        *last_ts > revision.ts,
                        "revisions for {} not in reverse timestamp order: got {} followed by {}",
                        hexlify(last_key),
                        *last_ts,
                        revision.ts
                    );
                    continue;
                }
            }
            page.push(revision);
            if page.len() == limit {
                break;
            }
        }

        let continue_cursor = match page.last() {
            Some(LsmRevision { key: last_key, .. }) => Some(match direction {
                Direction::Asc => Range {
                    lower: Bound::After(last_key.clone()),
                    upper: range.upper.clone().map(Vec::from),
                },
                Direction::Desc => Range {
                    lower: range.lower.clone().map(Vec::from),
                    upper: Bound::Before(last_key.clone()),
                },
            }),
            None => None,
        };

        page = page
            .into_iter()
            .filter(|revision| match revision.value {
                RevisionValue::Tombstone => false,
                _ => true,
            })
            .collect();

        Ok((page, continue_cursor))
    }

    pub(super) async fn history_page(
        &self,
        key: &[u8],
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<(Timestamp, RevisionValue)>, Option<HistoryRange>)> {
        let mut streams = Vec::with_capacity(self.0.levels.len());
        let mut l0_streams = Vec::with_capacity(1 + self.0.l0_sealed.len());
        {
            let revisions: Vec<_> = self
                .0
                .l0_active
                .history(key, range, direction)
                .map(|revision| Ok(revision))
                .collect();
            l0_streams.push(futures::stream::iter(revisions.into_iter()).boxed());
        }
        for l0_run in &self.0.l0_sealed {
            l0_streams.push(
                futures::stream::iter(
                    l0_run
                        .history(key, range, direction)
                        .map(|revision| Ok(revision)),
                )
                .boxed(),
            );
        }

        streams.push(match direction {
            Direction::Asc => merge_sorted_streams(
                l0_streams
                    .into_iter()
                    .map(|s| s.map(|result| result.map(|(ts, value)| OrdEqByFirst(ts, value))))
                    .collect(),
            )
            .map(|result| result.map(|OrdEqByFirst(ts, value)| (ts, value)))
            .boxed(),
            Direction::Desc => merge_sorted_streams(
                l0_streams
                    .into_iter()
                    .map(|s| {
                        s.map(|result| result.map(|(ts, value)| OrdEqByFirst(Reverse(ts), value)))
                    })
                    .collect(),
            )
            .map(|result| result.map(|OrdEqByFirst(Reverse(ts), value)| (ts, value)))
            .boxed(),
        });

        for level in &self.0.levels[1..] {
            if let Some(run) = level.run_for_key(key) {
                streams.push(run.history(key, range, direction).boxed());
            }
        }

        if direction == Direction::Asc {
            streams.reverse();
        }

        let mut stream = futures::stream::iter(streams.into_iter()).flatten();

        let mut page = vec![];
        while let Some(revision) = stream.try_next().await? {
            page.push(revision);
            if page.len() >= limit {
                break;
            }
        }

        let continue_cursor = match page.last() {
            None => None,
            Some((last_ts, _)) => match direction {
                Direction::Asc => match range {
                    HistoryRange::Until(max) | HistoryRange::Between(_, max) => {
                        let min = last_ts.plus_one();
                        if min > max {
                            None
                        } else {
                            Some(HistoryRange::Between(min, max))
                        }
                    }
                    HistoryRange::All | HistoryRange::Since(_) => {
                        Some(HistoryRange::Since(last_ts.plus_one()))
                    }
                },
                Direction::Desc => match range {
                    HistoryRange::All | HistoryRange::Until(_) => {
                        Some(HistoryRange::Until(last_ts.minus_one()))
                    }
                    HistoryRange::Between(min, _) | HistoryRange::Since(min) => {
                        let max = last_ts.minus_one();
                        if min > max {
                            None
                        } else {
                            Some(HistoryRange::Between(min, max))
                        }
                    }
                },
            },
        };

        Ok((page, continue_cursor))
    }
}
