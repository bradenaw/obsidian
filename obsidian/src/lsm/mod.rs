mod block;
mod compactor;
mod index;
mod memtable;
mod preload;
mod run;
mod util;

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Display;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::anyhow;
use futures::stream::StreamExt;
use futures::TryStreamExt;
use uuid::Uuid;

use crate::lsm::compactor::Compactor;
use crate::lsm::index::Index;
use crate::lsm::index::IndexSnapshot;
use crate::lsm::index::Keyspace;
use crate::lsm::memtable::Memtable;
pub(crate) use crate::lsm::preload::Preloaded;
pub(crate) use crate::lsm::preload::Preloader;
use crate::lsm::run::Run;
use crate::lsm::util::LsmRevision;
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
use crate::Precondition;
use crate::Range;
use crate::Revision;
use crate::RevisionValue;
use crate::Timestamp;
use crate::WalEntry;
use crate::WalSeq;
use crate::WriteError;

pub(crate) struct LsmBuilder {
    l0_max_size: u64,
    run_size_target: u64,
    block_size_target: u64,
    wal: Arc<dyn Wal>,
    storage: Arc<dyn Storage>,
}

impl LsmBuilder {
    pub fn new(wal: Arc<dyn Wal>, storage: Arc<dyn Storage>) -> Self {
        LsmBuilder {
            l0_max_size: 8_000_000,
            run_size_target: 64_000_000,
            block_size_target: 32768,
            wal: wal,
            storage: storage,
        }
    }

    pub fn l0_max_size(mut self, x: u64) -> Self {
        self.l0_max_size = x;
        self
    }

    pub fn run_size_target(mut self, x: u64) -> Self {
        self.run_size_target = x;
        self
    }

    pub fn block_size_target(mut self, x: u64) -> Self {
        self.block_size_target = x;
        self
    }

    pub fn wal(mut self, wal: Arc<dyn Wal>) -> Self {
        self.wal = wal;
        self
    }

    pub async fn build(self) -> anyhow::Result<Lsm> {
        Lsm::new(
            self.l0_max_size,
            self.run_size_target,
            self.block_size_target,
            self.wal,
            self.storage,
        )
        .await
    }
}

impl Clone for LsmBuilder {
    fn clone(&self) -> Self {
        Self {
            l0_max_size: self.l0_max_size.clone(),
            run_size_target: self.run_size_target.clone(),
            block_size_target: self.block_size_target.clone(),
            wal: self.wal.clone(),
            storage: self.storage.clone(),
        }
    }
}

pub(crate) struct Lsm {
    l0_max_size: u64,
    run_size_target: u64,
    block_size_target: u64,

    wal: Arc<dyn Wal>,
    index: Arc<Index>,
    compactor: Compactor,
    storage: Arc<dyn Storage>,

    bg: Background,
    wal_processed: tokio::sync::watch::Receiver<WalSeq>,
}

impl Lsm {
    pub async fn new(
        l0_max_size: u64,
        run_size_target: u64,
        block_size_target: u64,
        wal: Arc<dyn Wal>,
        storage: Arc<dyn Storage>,
    ) -> anyhow::Result<Self> {
        let (index, newest_seqno) = Self::recovery(l0_max_size, &wal, storage.deref()).await?;

        let index_arc = Arc::new(index);

        let bg = Background::new();
        let (wal_processed_send, wal_processed_recv) = tokio::sync::watch::channel(WalSeq(0));
        bg.spawn(Self::process_wal(
            Arc::clone(&index_arc),
            Arc::clone(&wal),
            newest_seqno.unwrap_or(WalSeq(1)),
            wal_processed_send,
            l0_max_size,
        ));

        let compactor = Compactor::new(
            Arc::clone(&storage),
            Arc::clone(&index_arc),
            1, // parallelism
            run_size_target,
            block_size_target,
        );

        Ok(Self {
            l0_max_size,
            run_size_target,
            block_size_target,

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
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<(), WriteError> {
        let index_snapshot = self.index.snapshot();

        for precond in preconds {
            let res = Self::keyspace(&index_snapshot, precond.keyspace_id())?
                .get(ts, precond.key())
                .await?;
            match precond {
                Precondition::NotChangedSince(_, _, ts) => {
                    if let Some((last_write_ts, _)) = res {
                        if last_write_ts > ts {
                            return Err(WriteError::PreconditionFailed);
                        }
                    }
                }
            }
        }

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

    async fn create_keyspace_with_depth(
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
}

#[derive(Eq, PartialEq, Hash, Clone, Copy)]
pub(crate) struct RunId(Uuid);

impl RunId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }

    fn encode_fixed(&self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out.copy_from_slice(self.0.as_bytes());
        out
    }
}

impl Display for RunId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl Debug for RunId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("run:")?;
        Display::fmt(self, f)
    }
}

#[derive(Clone)]
pub(crate) struct Manifest {
    pub(crate) keyspaces: HashMap<KeyspaceId, KeyspaceManifest>,
}

impl Manifest {
    pub fn new() -> Self {
        Self {
            keyspaces: HashMap::new(),
        }
    }
}

impl Manifest {
    pub fn runs(&self) -> impl Iterator<Item = (KeyspaceId, usize, &RunManifest)> {
        self.keyspaces
            .iter()
            .map(|(keyspace_id, keyspace)| {
                keyspace
                    .levels
                    .iter()
                    .enumerate()
                    .map(move |(i, level)| level.runs.iter().map(move |run| (*keyspace_id, i, run)))
                    .flatten()
            })
            .flatten()
    }
}

impl Debug for Manifest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut keyspace_ids: Vec<_> = self.keyspaces.keys().collect();
        keyspace_ids.sort_unstable();

        write!(f, "manifest\n")?;
        for keyspace_id in keyspace_ids {
            let keyspace = &self.keyspaces[keyspace_id];
            write!(f, "  {:?}\n", keyspace_id)?;
            for (i, level) in keyspace.levels.iter().enumerate() {
                write!(f, "    l{}\n", i)?;
                for run_manifest in &level.runs {
                    write!(
                        f,
                        "      {:?} {:?}\n",
                        run_manifest.run_id, run_manifest.range
                    )?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct KeyspaceManifest {
    pub(crate) levels: Vec<LevelManifest>,
}

#[derive(Clone, Debug)]
pub(crate) struct LevelManifest {
    pub(crate) runs: Vec<RunManifest>,
}

#[derive(Clone, Debug)]
pub(crate) struct RunManifest {
    pub(crate) run_id: RunId,
    pub(crate) range: Range<Vec<u8>>,
}

struct KeyspaceReader<'a>(&'a Keyspace);

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

    async fn history_page(
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::HashSet;
    use std::sync::Arc;

    use byteorder::BigEndian;
    use byteorder::ByteOrder;
    use futures::TryStreamExt;
    use proptest::prelude::*;

    use super::KeyspaceReader;
    use super::Lsm;
    use super::LsmBuilder;
    use crate::lsm::index::Keyspace;
    use crate::lsm::index::Level;
    use crate::lsm::memtable::Memtable;
    use crate::lsm::run::dump_run;
    use crate::lsm::run::Run;
    use crate::lsm::run::RunBuilder;
    use crate::lsm::util::LsmRevision;
    use crate::lsm::RunId;
    use crate::runtime::Wal;
    use crate::test::MemFileReader;
    use crate::test::MemStorage;
    use crate::test::MemWal;
    use crate::util::binary_search_by_idx;
    use crate::Bound;
    use crate::ColoGroupId;
    use crate::Direction;
    use crate::HistoryRange;
    use crate::KeyspaceId;
    use crate::Mutation;
    use crate::Precondition;
    use crate::Range;
    use crate::Revision;
    use crate::RevisionValue;
    use crate::Timestamp;
    use crate::WalSeq;
    use crate::WriteError;

    #[tokio::test]
    async fn test_put_get() -> anyhow::Result<()> {
        let lsm = LsmBuilder::new(Arc::new(MemWal::new()), Arc::new(MemStorage::new()))
            .build()
            .await?;
        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
        let k = b"abc";
        let not_k = b"def";
        let v = b"foo";

        lsm.create_keyspace_with_depth(keyspace_id, 2 /*depth*/)
            .await?;
        lsm.write(
            Timestamp(5),
            vec![],
            BTreeMap::from([((keyspace_id, k.to_vec()), Mutation::Put(v.to_vec()))]),
        )
        .await?;
        assert_eq!(lsm.get(Timestamp(4), keyspace_id, k).await?, None);
        assert_eq!(
            lsm.get(Timestamp(5), keyspace_id, k).await?,
            Some((Timestamp(5), RevisionValue::Regular(v.to_vec())))
        );
        assert_eq!(
            lsm.get(Timestamp(6), keyspace_id, k).await?,
            Some((Timestamp(5), RevisionValue::Regular(v.to_vec())))
        );
        assert_eq!(lsm.get(Timestamp(4), keyspace_id, not_k).await?, None);
        assert_eq!(lsm.get(Timestamp(5), keyspace_id, not_k).await?, None);
        assert_eq!(lsm.get(Timestamp(6), keyspace_id, not_k).await?, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_tx() -> anyhow::Result<()> {
        let lsm = LsmBuilder::new(Arc::new(MemWal::new()), Arc::new(MemStorage::new()))
            .build()
            .await?;

        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
        let ka = b"a";
        let kb = b"b";

        lsm.create_keyspace_with_depth(keyspace_id, 2 /*depth*/)
            .await?;
        lsm.write(
            Timestamp(5),
            vec![],
            BTreeMap::from([
                ((keyspace_id, ka.to_vec()), Mutation::Put(b"a0".to_vec())),
                ((keyspace_id, kb.to_vec()), Mutation::Put(b"b0".to_vec())),
            ]),
        )
        .await?;

        assert!(matches!(
            lsm.write(
                Timestamp(10),
                vec![Precondition::NotChangedSince(
                    keyspace_id,
                    ka.to_vec(),
                    Timestamp(4),
                )],
                BTreeMap::from([((keyspace_id, ka.to_vec()), Mutation::Put(b"a1".to_vec()))]),
            )
            .await,
            Err(WriteError::PreconditionFailed),
        ));

        lsm.write(
            Timestamp(10),
            vec![Precondition::NotChangedSince(
                keyspace_id,
                ka.to_vec(),
                Timestamp(5),
            )],
            BTreeMap::from([
                ((keyspace_id, ka.to_vec()), Mutation::Put(b"a1".to_vec())),
                ((keyspace_id, kb.to_vec()), Mutation::Delete),
            ]),
        )
        .await?;

        assert_eq!(lsm.get(Timestamp(4), keyspace_id, ka).await?, None);
        assert_eq!(lsm.get(Timestamp(4), keyspace_id, kb).await?, None);
        assert_eq!(
            lsm.get(Timestamp(9), keyspace_id, ka).await?,
            Some((Timestamp(5), RevisionValue::Regular(b"a0".to_vec())))
        );
        assert_eq!(
            lsm.get(Timestamp(9), keyspace_id, kb).await?,
            Some((Timestamp(5), RevisionValue::Regular(b"b0".to_vec())))
        );
        assert_eq!(
            lsm.get(Timestamp(10), keyspace_id, ka).await?,
            Some((Timestamp(10), RevisionValue::Regular(b"a1".to_vec())))
        );
        assert_eq!(
            lsm.get(Timestamp(10), keyspace_id, kb).await?,
            Some((Timestamp(10), RevisionValue::Tombstone))
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_compact_l0() -> anyhow::Result<()> {
        _ = pretty_env_logger::try_init();
        let lsm = LsmBuilder::new(Arc::new(MemWal::new()), Arc::new(MemStorage::new()))
            .l0_max_size(128)
            .block_size_target(128)
            .run_size_target(512)
            .build()
            .await?;
        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
        lsm.create_keyspace_with_depth(keyspace_id, 2 /*depth*/)
            .await?;
        let mut map = BTreeMap::new();
        let mut last_ts = Timestamp::ZERO;
        for _ in 0..10 {
            let compacted = lsm.pending_compactions();
            // We consider these writes to be 10 bytes (1 key + 8 ts + 1 value), so this is
            // enough to overfill a memtable.
            for i in 0..24 {
                let v = (i % 179) as u8;
                last_ts = Timestamp(last_ts.0 + 1);
                lsm.write(
                    last_ts,
                    vec![],
                    BTreeMap::from([((keyspace_id, vec![i as u8]), Mutation::Put(vec![v]))]),
                )
                .await?;
                map.insert(i as u8, v);
            }
            compacted.await;

            for (k, v) in &map {
                assert_eq!(
                    lsm.get(last_ts, keyspace_id, &[*k]).await?.map(|(_, b)| b),
                    Some(RevisionValue::Regular(vec![*v])),
                );
            }
        }

        // Make sure we actually did ever do a compaction.
        assert!(
            lsm.index
                .snapshot()
                .keyspaces
                .get(&keyspace_id)
                .unwrap()
                .levels[1]
                .runs
                .len()
                >= 1
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_compact_l1() -> anyhow::Result<()> {
        _ = pretty_env_logger::try_init();

        let lsm = LsmBuilder::new(Arc::new(MemWal::new()), Arc::new(MemStorage::new()))
            .l0_max_size(128)
            .block_size_target(128)
            .run_size_target(512)
            .build()
            .await?;
        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
        lsm.create_keyspace_with_depth(keyspace_id, 3 /*depth*/)
            .await?;
        let mut map = BTreeMap::new();
        let mut last_ts = Timestamp::ZERO;
        let mut ctr = 1u32;
        for j in 0..10 {
            loop {
                // We consider these writes to be 10 bytes (1 key + 8 ts + 1 value), so this is
                // enough to overfill a memtable.
                for i in 0..24 {
                    let k = (j * 5 + i) as u8;
                    let mut v = [0u8; 4];
                    BigEndian::write_u32(&mut v, ctr);
                    ctr += 1;
                    lsm.write(
                        Timestamp(ctr as u64),
                        vec![],
                        BTreeMap::from([((keyspace_id, vec![k]), Mutation::Put(v.to_vec()))]),
                    )
                    .await?;
                    last_ts = Timestamp(ctr as u64);
                    map.insert(k, v.to_vec());
                }

                lsm.pending_compactions().await;
                if lsm
                    .index
                    .snapshot()
                    .keyspaces
                    .get(&keyspace_id)
                    .unwrap()
                    .levels[2]
                    .runs
                    .len()
                    >= (j + 1) as usize
                {
                    break;
                }
            }

            dump_lsm(&lsm).await?;

            for (k, v) in &map {
                let actual = lsm.get(last_ts, keyspace_id, &[*k]).await?.map(|(_, b)| b);
                assert_eq!(actual, Some(RevisionValue::Regular(v.clone())));
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_recovery() -> anyhow::Result<()> {
        let wal = Arc::new(MemWal::new()) as Arc<dyn Wal>;
        let storage = Arc::new(MemStorage::new());

        let lsm = LsmBuilder::new(Arc::clone(&wal), storage.clone())
            .l0_max_size(128)
            .block_size_target(128)
            .run_size_target(512)
            .build()
            .await?;

        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
        lsm.create_keyspace_with_depth(keyspace_id, 2 /*depth*/)
            .await?;

        let mut map = BTreeMap::new();
        let mut write_ts = 5;
        for _ in 0..10 {
            // We consider these writes to be 10 bytes (1 key + 8 ts + 1 value), so this is
            // enough to overfill a memtable.
            for i in 0..24 {
                let v = (i % 179) as u8;
                lsm.write(
                    Timestamp(write_ts),
                    vec![],
                    BTreeMap::from([((keyspace_id, vec![i as u8]), Mutation::Put(vec![v]))]),
                )
                .await?;
                write_ts += 2;
                map.insert(i as u8, v);
            }
            lsm.pending_compactions().await;

            for (k, v) in &map {
                assert_eq!(
                    lsm.get(Timestamp(write_ts), keyspace_id, &[*k])
                        .await?
                        .map(|(_, b)| b),
                    Some(RevisionValue::Regular(vec![*v])),
                );
            }
        }

        // Make sure we actually did ever do a compaction.
        assert!(
            lsm.index
                .snapshot()
                .keyspaces
                .get(&keyspace_id)
                .unwrap()
                .levels[1]
                .runs
                .len()
                >= 1
        );

        lsm.flush().await?;

        drop(lsm);

        // Rebuild the LSM from the same WAL and storage, this should recover everything.
        let lsm = LsmBuilder::new(wal, storage).build().await?;

        for (k, v) in &map {
            assert_eq!(
                lsm.get(Timestamp(write_ts), keyspace_id, &[*k])
                    .await?
                    .map(|(_, b)| b),
                Some(RevisionValue::Regular(vec![*v]))
            );
        }

        Ok(())
    }

    #[test]
    fn test_binary_search_by_key() {
        for n in 1..32 {
            for i in 0..n {
                assert_eq!(binary_search_by_idx(n, i, |x| x), Ok(i));
            }
        }
        for n in 1..32 {
            for i in 0..=n {
                assert_eq!(binary_search_by_idx(n, 2 * i, |x| 2 * x + 1), Err(i));
            }
        }
    }

    #[tokio::test]
    async fn test_scan_page() -> anyhow::Result<()> {
        let lsm = LsmBuilder::new(Arc::new(MemWal::new()), Arc::new(MemStorage::new()))
            .l0_max_size(32)
            .block_size_target(48)
            .run_size_target(96)
            .build()
            .await?;
        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
        lsm.create_keyspace_with_depth(keyspace_id, 3 /*depth*/)
            .await?;

        let writes = [
            //   ts=0123456789
            ("a", b" o  o    o"),
            ("b", b"   o     o"),
            ("c", b"   o x    "),
            ("d", b"   oxo    "),
            ("e", b"    o   o "),
            ("f", b"     o  o "),
            ("g", b" o x  o  o"),
            ("h", b"  o oxo  o"),
            ("i", b"  o  oo o "),
            ("j", b" xoxoxoxox"),
            ("k", b"        o "),
            ("l", b" ooooooooo"),
        ];

        //let mut expecteds = vec![];
        for ts in 1..writes[0].1.len() {
            //let mut expected = match expecteds.last() {
            //    Some(prev) => prev.clone(),
            //    None => BTreeMap::new(),
            //};

            for (key, versions) in writes {
                let mutation = match versions[ts] {
                    b'o' => Mutation::Put(format!("{} {}", key, ts).into()),
                    b'x' => Mutation::Delete,
                    _ => continue,
                };

                //let value = match mutation {
                //    Mutation::Put(v) => RevisionValue::Regular(v),
                //    Mutation::Delete => RevisionValue::Tombstone,
                //};
                lsm.write(
                    Timestamp(ts as u64),
                    vec![],
                    BTreeMap::from([((keyspace_id, key.into()), mutation)]),
                )
                .await?;

                //expected.insert(key, value);
            }
            if ts < writes[0].1.len() - 2 && ts % 3 == 0 {
                lsm.pending_compactions().await;
            }
            //expecteds.push(expected);
        }

        async fn check(
            lsm: &Lsm,
            ts: Timestamp,
            keyspace_id: KeyspaceId,
            range: Range<Vec<u8>>,
            expected: Vec<(&str, usize)>,
        ) -> anyhow::Result<()> {
            for direction in [Direction::Asc, Direction::Desc] {
                for page_size in 1..=expected.len() {
                    println!("== check");
                    let mut maybe_cursor: Option<Range<Vec<u8>>> = Some(range.clone());
                    let mut results = vec![];
                    while let Some(cursor) = maybe_cursor {
                        let (page, continue_cursor) = lsm
                            .scan_page(ts, keyspace_id, cursor.borrow(), direction, page_size)
                            .await?;

                        println!(
                            "scan_page(ts={}, /*keyspace_id*/, {:?}, {:?}, {}) -> ({:?}, {:?})",
                            ts, cursor, direction, page_size, continue_cursor, page,
                        );
                        assert!(page.len() <= page_size);
                        results.extend(page);
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
                            .map(|(key, ts)| Revision {
                                key: (keyspace_id, (key).into()),
                                ts: Timestamp(ts as u64),
                                value: RevisionValue::Regular(format!("{} {}", key, ts).into()),
                            })
                            .collect::<Vec<Revision>>(),
                        "scan_page(ts={:?}, /*keyspace_id*/, /*cursor*/, direction={:?}, page_size={})",
                        ts,
                        direction,
                        page_size,
                    );
                }
            }

            Ok(())
        }

        dump_lsm(&lsm).await?;

        check(
            &lsm,
            Timestamp(5),
            keyspace_id,
            Range {
                lower: Bound::Before("b".into()),
                upper: Bound::After("e".into()),
            },
            vec![("b", 3), ("d", 5), ("e", 4)],
        )
        .await?;

        check(
            &lsm,
            Timestamp(4),
            keyspace_id,
            Range::all(),
            vec![
                ("a", 4),
                ("b", 3),
                ("c", 3),
                // d got deleted at 4
                ("e", 4),
                // f doesn't exist yet
                ("h", 4),
                ("i", 2),
                ("j", 4),
                // k doesn't exist yet
                ("l", 4),
            ],
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_history_page() -> anyhow::Result<()> {
        let diagram = vec![
            //                         1
            //   ts= 1 2 3 4 5 6 7 8 9 0
            ("a", b"   o  |  o|  o|o o| ".as_slice()),
            (" ", b"------+   |   | +-+ "),
            ("b", b" o   x|o  |o o|x|o  "),
            (" ", b"----+-+---+---+ +-+ "),
            ("c", b"   o|o o     o|o x| "),
            (" ", b"----+-+---+   | +-+ "),
            ("d", b"     o|o o|x o| |o  "),
        ];

        let keyspace = keyspace_from_diagram(diagram).await?;

        async fn check(
            keyspace: &Keyspace,
            key: &[u8],
            range: HistoryRange,
            expected: &[(usize, bool)],
        ) -> anyhow::Result<()> {
            for direction in [Direction::Asc, Direction::Desc] {
                for page_size in 1..=expected.len() {
                    let mut maybe_cursor = Some(range.clone());
                    let mut results = vec![];
                    while let Some(cursor) = maybe_cursor {
                        let (page, continue_cursor) = KeyspaceReader(keyspace)
                            .history_page(key, cursor, direction, page_size)
                            .await?;

                        println!(
                            "history_page(key = {:?}, cursor = {:?}, direction={:?}, page_size={}) -> ({:?}, {:?})",
                            key,
                            cursor,
                            direction,
                            page_size,
                            page,
                            continue_cursor,
                        );

                        assert!(page.len() <= page_size);
                        results.extend(page);
                        maybe_cursor = continue_cursor;
                    }

                    if direction == Direction::Desc {
                        results.reverse();
                    }

                    assert_eq!(
                        results,
                        expected
                            .into_iter()
                            .map(|(ts, is_tombstone)| {
                                let revision = lsm_diagram_revision(key, *ts, *is_tombstone);
                                (revision.ts, revision.value)
                            })
                            .collect::<Vec<_>>(),
                        "history_page(key = {:?}, range = {:?}, direction={:?}, page_size={})",
                        key,
                        range,
                        direction,
                        page_size,
                    );
                }
            }
            Ok(())
        }

        let all_b_versions = vec![
            (1, false),
            (3, true),
            (4, false),
            (6, false),
            (7, false),
            (8, true),
            (9, false),
        ];

        check(&keyspace, b"b", HistoryRange::All, &all_b_versions).await?;

        check(
            &keyspace,
            b"b",
            HistoryRange::Between(Timestamp(1), Timestamp(9)),
            &all_b_versions,
        )
        .await?;

        check(
            &keyspace,
            b"b",
            HistoryRange::Until(Timestamp(9)),
            &all_b_versions,
        )
        .await?;

        check(
            &keyspace,
            b"b",
            HistoryRange::Since(Timestamp(1)),
            &all_b_versions,
        )
        .await?;

        Ok(())
    }

    fn bound_strategy() -> impl Strategy<Value = Bound<Vec<u8>>> {
        prop_oneof![
            Just(Bound::BeforeAll),
            proptest::collection::vec(u8::arbitrary(), 0..16).prop_map(|v| Bound::Before(v)),
            proptest::collection::vec(u8::arbitrary(), 0..16).prop_map(|v| Bound::After(v)),
            proptest::collection::vec(u8::arbitrary(), 0..16).prop_map(|v| Bound::AfterPrefix(v)),
            Just(Bound::AfterAll),
        ]
    }
    fn range_strategy() -> impl Strategy<Value = Range<Vec<u8>>> {
        (bound_strategy(), bound_strategy()).prop_map(|(lower, upper)| Range { lower, upper })
    }

    proptest! {
        #[test]
        #[ignore]
        fn proptest_lsm_scan(
            keys in proptest::collection::btree_set(
                proptest::collection::vec(u8::arbitrary(), 0..16),
                1..100,
            ),
            write_indexes in proptest::collection::vec(any::<prop::sample::Index>(), 1..4096),
            log_indexes in proptest::collection::vec(any::<prop::sample::Index>(), 1000),
            ranges in proptest::collection::vec(range_strategy(), 1000),
            direction in proptest::sample::select(std::borrow::Cow::Owned(vec![
                Direction::Asc,
                //Direction::Desc,
            ])),
        ) {
            tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(async {
                let keys_vec: Vec<_> = keys.iter().collect();

                let mut writes = vec![];

                let lsm = LsmBuilder::new(Arc::new(MemWal::new()), Arc::new(MemStorage::new()))
                    .l0_max_size(128)
                    .block_size_target(128)
                    .run_size_target(512)
                    .build()
                    .await
                    .unwrap();
                let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
                lsm.create_keyspace_with_depth(keyspace_id, 3 /*depth*/).await.unwrap();

                let mut write_ts = 5;
                for (i, index) in write_indexes.iter().enumerate() {
                    let key = keys_vec[index.index(keys_vec.len())];
                    let mut value = vec![0; 16];
                    BigEndian::write_u64(&mut value[8..], i as u64);
                    lsm
                        .write(
                            Timestamp(write_ts),
                            vec![],
                            BTreeMap::from([((keyspace_id, key.clone()), Mutation::Put(value.clone()))]),
                        )
                        .await
                        .unwrap();
                    writes.push((key.clone(), Timestamp(write_ts), value.clone()));
                    write_ts += 2;
                }

                for (log_index_gen, range) in std::iter::zip(log_indexes, ranges) {
                    let log_idx = log_index_gen.index(writes.len());
                    let ts = writes[log_idx].1;

                    let mut expected = BTreeMap::new();
                    for (key, ts, value) in writes[..=log_idx].iter() {
                        if !range.contains(key) {
                            continue;
                        }
                        expected.insert(key, (ts, value));
                    }


                    let mut maybe_cursor = Some(range.clone());
                    let mut results = vec![];
                    while let Some(cursor) = maybe_cursor {
                        let (mut page, continue_cursor) = lsm.scan_page(
                            ts,
                            keyspace_id,
                            cursor.borrow(),
                            direction,
                            100,
                        ).await.unwrap();
                        results.append(&mut page);
                        assert!(Some(cursor) != continue_cursor);
                        maybe_cursor = continue_cursor;
                    }

                    let mut expected_recs: Vec<Revision> = expected.into_iter().map(|(key, (ts, value))| {
                        Revision{key: (keyspace_id, key.clone()), ts: *ts, value: RevisionValue::Regular(value.clone())}
                    }).collect();
                    if direction == Direction::Desc {
                        expected_recs.reverse();
                    }

                    assert_eq!(results, expected_recs);
                }
            });
        }
    }

    async fn dump_lsm(lsm: &Lsm) -> anyhow::Result<()> {
        let index_snapshot = lsm.index.snapshot();
        for (keyspace_id, keyspace) in &index_snapshot.keyspaces {
            println!("keyspace_id {:?}", keyspace_id);
            dump_keyspace(&keyspace).await?;
        }

        Ok(())
    }

    async fn dump_keyspace(keyspace: &Keyspace) -> anyhow::Result<()> {
        println!("== manifest =====");
        println!("l0_active");
        {
            let memtable = &keyspace.l0_active;
            println!(
                "  {} ({} bytes) {:?}",
                memtable.id(),
                memtable.size(),
                memtable.range(),
            );
        }
        println!("l0_sealed");
        for memtable in &keyspace.l0_sealed {
            println!(
                "  {} ({} bytes) {:?}",
                memtable.id(),
                memtable.size(),
                memtable.range(),
            );
        }
        for (i, level) in keyspace.levels[1..]
            .iter()
            .enumerate()
            .map(|(i, level)| (i + 1, level))
        {
            println!("l{} ({} bytes)", i, level.size());
            for run in &level.runs {
                println!("  {} ({} bytes) {:?}", run.id(), run.size(), run.range());
            }
        }
        println!("============");

        println!("== kvs =====");
        println!("l0_active");
        {
            let memtable = &keyspace.l0_active;
            println!(
                "  {} ({} bytes) {:?}",
                memtable.id(),
                memtable.size(),
                memtable.range(),
            );
            memtable.dump();
        }
        println!("l0_sealed");
        for memtable in &keyspace.l0_sealed {
            println!(
                "  {} ({} bytes) {:?}",
                memtable.id(),
                memtable.size(),
                memtable.range(),
            );
            memtable.dump();
        }
        for (i, level) in keyspace.levels[1..]
            .iter()
            .enumerate()
            .map(|(i, level)| (i + 1, level))
        {
            println!("l{} ({} bytes)", i, level.size());
            for run in &level.runs {
                println!("  {} ({} bytes) {:?}", run.id(), run.size(), run.range());
                dump_run(&run).await?;
            }
        }
        println!("============");
        Ok(())
    }

    #[tokio::test]
    async fn test_keyspace_from_diagram() -> anyhow::Result<()> {
        let diagram = vec![
            //                         1
            //   ts= 1 2 3 4 5 6 7 8 9 0
            ("a", b"   o  |  o|  o|o o| ".as_slice()),
            (" ", b"------+   |   | +-+ "),
            ("b", b" o   x|o  |o o|x|o  "),
            (" ", b"----+-+---+---+ +-+ "),
            ("c", b"   o|o o     o|o x| "),
            (" ", b"----+-+---+   | +-+ "),
            ("d", b"     o|o o|x o| |o  "),
        ];

        let keyspace = keyspace_from_diagram(diagram).await?;

        let a = "a";
        let b = "b";
        let c = "c";
        let d = "d";

        assert_eq!(
            keyspace.l0_active.iter().collect::<Vec<_>>(),
            vec![
                lsm_diagram_revision(b.as_bytes(), 9, false),
                lsm_diagram_revision(d.as_bytes(), 9, false),
            ],
        );
        assert_eq!(
            keyspace.l0_sealed[0].iter().collect::<Vec<_>>(),
            vec![
                lsm_diagram_revision(a.as_bytes(), 9, false),
                lsm_diagram_revision(a.as_bytes(), 8, false),
                lsm_diagram_revision(b.as_bytes(), 8, true),
                lsm_diagram_revision(c.as_bytes(), 9, true),
                lsm_diagram_revision(c.as_bytes(), 8, false),
            ],
        );

        assert_eq!(
            keyspace.levels[1..]
                .iter()
                .map(|level| {
                    level
                        .runs
                        .iter()
                        .map(|run| {
                            futures::executor::block_on(run.stream().try_collect::<Vec<_>>())
                        })
                        .collect::<anyhow::Result<Vec<_>>>()
                })
                .collect::<anyhow::Result<Vec<_>>>()?,
            vec![
                vec![
                    vec![(a, 7, false), (b, 7, false), (b, 6, false)],
                    vec![
                        (c, 7, false),
                        (c, 4, false),
                        (c, 3, false),
                        (d, 7, false),
                        (d, 6, true),
                    ],
                ],
                vec![
                    vec![(a, 5, false), (b, 4, false)],
                    vec![(d, 5, false), (d, 4, false)],
                ],
                vec![
                    vec![(a, 2, false)],
                    vec![(b, 3, true), (b, 1, false)],
                    vec![(d, 3, false)],
                ],
                vec![vec![(c, 2, false)]],
            ]
            .into_iter()
            .map(|level| {
                level
                    .into_iter()
                    .map(|run| {
                        run.into_iter()
                            .map(|(key, ts, is_tombstone)| {
                                lsm_diagram_revision(key.as_bytes(), ts, is_tombstone)
                            })
                            .collect::<Vec<LsmRevision>>()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>(),
        );

        Ok(())
    }

    fn lsm_diagram_value(key: &[u8], ts: usize) -> RevisionValue {
        RevisionValue::Regular(format!("{:?} {}", key, ts).into())
    }

    fn lsm_diagram_revision(key: &[u8], ts: usize, is_tombstone: bool) -> LsmRevision {
        LsmRevision {
            key: key.into(),
            ts: Timestamp(ts as u64),
            value: match is_tombstone {
                false => lsm_diagram_value(key, ts),
                true => RevisionValue::Tombstone,
            },
        }
    }

    async fn keyspace_from_diagram(diagram: Vec<(&str, &[u8])>) -> anyhow::Result<Keyspace> {
        fn find_touching(
            diagram: &[(&str, &[u8])],
            visited: &mut HashSet<(usize, usize)>,
            x: usize,
            y: usize,
        ) -> Vec<LsmRevision> {
            fn find_touching_inner(
                diagram: &[(&str, &[u8])],
                visited: &mut HashSet<(usize, usize)>,
                x: usize,
                y: usize,
                out: &mut Vec<LsmRevision>,
            ) {
                if visited.contains(&(x, y)) {
                    return;
                }
                visited.insert((x, y));

                let key_str = diagram[y].0;
                let key = key_str.as_bytes().to_vec();
                let ts = Timestamp((x / 2 + 1) as u64);

                if let Some(value) = match diagram[y].1[x] {
                    b'o' => Some(lsm_diagram_value(&key, ts.0 as usize)),
                    b'x' => Some(RevisionValue::Tombstone),
                    b' ' => None,
                    _ => return,
                } {
                    out.push(LsmRevision { key, ts, value });
                }

                for (dx, dy) in [(0isize, -1isize), (1, 0), (0, 1), (-1, 0)] {
                    let next_x = (x as isize) + dx;
                    let next_y = (y as isize) + dy;

                    if next_x < 0
                        || next_x >= diagram[0].1.len() as isize
                        || next_y < 0
                        || next_y >= diagram.len() as isize
                    {
                        continue;
                    }

                    find_touching_inner(diagram, visited, next_x as usize, next_y as usize, out);
                }
            }

            let mut out = vec![];
            find_touching_inner(diagram, visited, x, y, &mut out);
            out
        }

        let mut visited = HashSet::new();

        let x_max = diagram[0].1.len() - 1;
        let l0_active_revisions = find_touching(&diagram[..], &mut visited, x_max, 0);
        let l0_active = Memtable::new();
        for revision in l0_active_revisions {
            l0_active.insert(WalSeq(1), revision.key, revision.ts, revision.value);
        }
        let mut keyspace = Keyspace {
            l0_active: Arc::new(l0_active),
            l0_sealed: vec![Arc::new(Memtable::new())],
            levels: vec![Level { runs: Vec::new() }],
        };
        for x in (0..=x_max).rev().filter(|x| x % 2 == 1) {
            let mut level = Level { runs: Vec::new() };
            for y in (0..diagram.len()).filter(|y| y % 2 == 0) {
                let revisions = find_touching(&diagram[..], &mut visited, x, y);
                if revisions.is_empty() {
                    continue;
                }

                if keyspace.l0_sealed[0].is_empty() {
                    for revision in revisions {
                        keyspace.l0_sealed[0].insert(
                            WalSeq(1),
                            revision.key,
                            revision.ts,
                            revision.value,
                        );
                    }
                } else {
                    let mut v = vec![];
                    let mut run_builder = RunBuilder::new(
                        &mut v,
                        RunId::new(),
                        KeyspaceId(ColoGroupId(1), 1),
                        1024, // block_size_target
                    );
                    for revision in revisions {
                        run_builder.push(revision).await?;
                    }
                    run_builder.finish().await?;
                    let run = Run::open(Arc::new(MemFileReader::new(v))).await?;
                    level.runs.push(Arc::new(run));
                }
            }

            if level.runs.is_empty() {
                continue;
            }

            keyspace.levels.push(level);
        }

        Ok(keyspace)
    }
}
