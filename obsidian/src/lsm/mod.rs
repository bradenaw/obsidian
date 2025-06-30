mod block;
mod memtable;
mod run;
mod util;

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::future::Future;
use std::ops::Deref;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use anyhow::anyhow;
use futures::future;
use futures::stream::Stream;
use futures::stream::StreamExt;
use futures::SinkExt;
use futures::TryStreamExt;
use rand::Rng;
use uuid::Uuid;

use crate::lsm::memtable::Memtable;
use crate::lsm::run::Run;
use crate::lsm::util::LsmRevision;
use crate::range::intersect_in_ranges_by_key;
use crate::range::Bound;
use crate::range::KeyOrBound;
use crate::range::Range;
use crate::storage::MemStorage;
use crate::storage::Storage;
use crate::types::Direction;
use crate::types::HistoryRange;
use crate::types::Key;
use crate::types::KeyspaceId;
use crate::types::Mutation;
use crate::types::Precondition;
use crate::types::Revision;
use crate::types::RevisionValue;
use crate::types::Timestamp;
use crate::types::WriteError;
use crate::util::merge_sorted;
use crate::util::merge_sorted_streams;
use crate::util::AtomicArc;
use crate::util::Background;
use crate::util::IteratorEither;
use crate::util::OrdEqByFirst;
use crate::wal;

pub(crate) struct LsmBuilder {
    l0_max_size: u64,
    run_target_size: u64,
    block_size: u64,
    wal: Option<Arc<wal::Wal<WalEntry>>>,
    storage: Option<Arc<MemStorage>>,
}

impl LsmBuilder {
    pub fn new() -> Self {
        LsmBuilder {
            l0_max_size: 8_000_000,
            run_target_size: 64_000_000,
            block_size: 32768,
            wal: None,
            storage: None,
        }
    }

    pub fn l0_max_size(mut self, x: u64) -> Self {
        self.l0_max_size = x;
        self
    }

    pub fn run_target_size(mut self, x: u64) -> Self {
        self.run_target_size = x;
        self
    }

    pub fn block_size(mut self, x: u64) -> Self {
        self.block_size = x;
        self
    }

    pub fn wal(mut self, wal: Arc<wal::Wal<WalEntry>>) -> Self {
        self.wal = Some(wal);
        self
    }

    pub fn storage(mut self, storage: Arc<MemStorage>) -> Self {
        self.storage = Some(storage);
        self
    }

    pub async fn build(self) -> anyhow::Result<Lsm> {
        Lsm::new(
            self.l0_max_size,
            self.run_target_size,
            self.block_size,
            self.wal
                .unwrap_or_else(|| Arc::new(wal::Wal::new(16384, Duration::from_millis(5)))),
            self.storage.unwrap_or_else(|| Arc::new(MemStorage::new())),
        )
        .await
    }
}

pub(crate) struct Lsm {
    l0_max_size: u64,
    run_target_size: u64,
    block_size: u64,

    inner: Arc<AtomicArc<HashMap<KeyspaceId, Arc<LsmInner>>>>,
    wal: Arc<wal::Wal<WalEntry>>,
    storage: Arc<MemStorage>,

    bg: Background,
    wal_processed: tokio::sync::watch::Receiver<wal::SeqNo>,
}

impl Lsm {
    pub async fn new(
        l0_max_size: u64,
        run_target_size: u64,
        block_size: u64,
        wal: Arc<wal::Wal<WalEntry>>,
        storage: Arc<MemStorage>,
    ) -> anyhow::Result<Self> {
        let (manifests, newest_seqno) = Self::recovery(&wal, &storage).await?;

        let lsms = {
            let mut lsms = HashMap::new();

            for (keyspace_id, manifest) in manifests {
                lsms.insert(
                    keyspace_id,
                    Arc::new(
                        LsmInner::new(
                            l0_max_size,
                            run_target_size,
                            block_size,
                            keyspace_id,
                            manifest,
                            wal.clone(),
                            storage.clone(),
                        )
                        .await?,
                    ),
                );
            }

            lsms
        };

        let inner = Arc::new(AtomicArc::new(Arc::new(lsms)));

        let bg = Background::new();
        let (wal_processed_send, wal_processed_recv) = tokio::sync::watch::channel(wal::SeqNo(0));
        bg.spawn(Self::process_wal(
            inner.clone(),
            wal.clone(),
            newest_seqno.unwrap_or(wal::SeqNo(0)),
            wal_processed_send,
        ));

        Ok(Self {
            l0_max_size,
            run_target_size,
            block_size,

            inner,
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
        self.inner
            .load()
            .get(&keyspace_id)
            .ok_or_else(|| anyhow!("{:?} not found", keyspace_id))?
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
        let (page, continue_cursor) = self
            .inner
            .load()
            .get(&keyspace_id)
            .ok_or_else(|| anyhow!("{:?} not found", keyspace_id))?
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
        self.inner
            .load()
            .get(&keyspace_id)
            .ok_or_else(|| anyhow!("{:?} not found", keyspace_id))?
            .history_page(key, range, direction, limit)
            .await
    }

    pub async fn write(
        &self,
        ts: Timestamp,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<(), WriteError> {
        let keyspaces = self.inner.load();

        for precond in preconds {
            let res = keyspaces
                .get(&precond.keyspace_id())
                .ok_or_else(|| anyhow!("{:?} not found", precond.keyspace_id()))?
                .inner
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

        let mut wal_processed = self.wal_processed.clone();
        while *wal_processed.borrow_and_update() < seqno {
            wal_processed
                .changed()
                .await
                .map_err(|_| WriteError::Other(anyhow!("wal processor missing")))?;
        }

        Ok(())
    }

    pub async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        loop {
            let inner = self.inner.load();
            let mut inner_new = (*inner).clone();
            inner_new.entry(keyspace_id).or_insert(Arc::new(
                LsmInner::new(
                    self.l0_max_size,
                    self.run_target_size,
                    self.block_size,
                    keyspace_id,
                    Manifest::new(7),
                    self.wal.clone(),
                    self.storage.clone(),
                )
                .await?,
            ));

            if self.inner.compare_and_swap(&inner, Arc::new(inner_new)) {
                break;
            }
        }

        Ok(())
    }

    pub async fn pending_compactions(&self) {
        let inner = self.inner.load();
        future::join_all(
            inner
                .values()
                .map(|keyspace_lsm| keyspace_lsm.pending_compactions()),
        )
        .await;
    }

    // TODO: move up a layer to tablet, since we can store the keyspaces that exist in a known
    // keyspace for the tablet
    pub fn keyspaces(&self) -> Vec<KeyspaceId> {
        self.inner.load().keys().copied().collect()
    }

    async fn recovery(
        wal: &wal::Wal<WalEntry>,
        storage: &MemStorage,
    ) -> anyhow::Result<(HashMap<KeyspaceId, Manifest>, Option<wal::SeqNo>)> {
        let oldest_seqno = wal.oldest_available();
        let mut newest_seqno = None;
        let mut wal_stream = wal.stream(oldest_seqno, false).boxed();

        let mut bufs = HashMap::new();
        let mut manifest_uuids = HashMap::new();

        while let Some((seqno, entry)) = wal_stream.try_next().await? {
            match entry {
                WalEntry::Write(ts, kvs) => {
                    let mut kvs_by_keyspace = HashMap::new();
                    for (keyspace_id, key, value) in kvs {
                        kvs_by_keyspace
                            .entry(keyspace_id)
                            .or_insert_with(Vec::new)
                            .push((key, value));
                    }
                    for (keyspace_id, keyspace_kvs) in kvs_by_keyspace {
                        bufs.entry(keyspace_id)
                            .or_insert_with(VecDeque::new)
                            .push_back((seqno, ts, keyspace_kvs));
                    }
                }
                WalEntry::Manifest(keyspace_id, included_seqno, levels) => {
                    let buf = bufs.entry(keyspace_id).or_insert_with(VecDeque::new);
                    let trim_to_idx = buf
                        .binary_search_by_key(&included_seqno, |(seqno, _, _)| *seqno)
                        .unwrap_or_else(core::convert::identity);
                    buf.drain(0..=trim_to_idx);
                    manifest_uuids.insert(keyspace_id, levels);
                }
            }
            newest_seqno = Some(seqno);
        }

        let mut manifests = HashMap::new();
        for (keyspace_id, buf) in bufs {
            let mut memtable = Memtable::new();
            for (seqno, ts, kvs) in buf {
                for (key, value) in kvs {
                    memtable.insert(seqno, key, ts, value);
                }
            }

            // TODO: no unwrap by putting both in the same map
            let keyspace_manifest_uuids = manifest_uuids.get(&keyspace_id).unwrap();
            let mut manifest = Manifest::new(7);
            manifest.l0_sealed.push(Arc::new(memtable));

            for i in 1..keyspace_manifest_uuids.len() {
                let mut runs = Vec::with_capacity(keyspace_manifest_uuids[i].len());
                for run_uuid in &keyspace_manifest_uuids[i] {
                    let run = Run::open(storage.get(&run_uuid.to_string()).await?).await?;
                    runs.push(run);
                }
                runs.sort_by_key(|run| run.range().lower);
                manifest.levels[i] = Level { runs };
            }

            manifests.insert(keyspace_id, manifest);
        }

        Ok((manifests, newest_seqno))
    }

    async fn process_wal(
        inner: Arc<AtomicArc<HashMap<KeyspaceId, Arc<LsmInner>>>>,
        wal: Arc<wal::Wal<WalEntry>>,
        start: wal::SeqNo,
        wal_processed: tokio::sync::watch::Sender<wal::SeqNo>,
    ) {
        // TODO: retry
        Self::process_wal_once(inner, wal, start, wal_processed)
            .await
            .unwrap();
    }

    async fn process_wal_once(
        inner: Arc<AtomicArc<HashMap<KeyspaceId, Arc<LsmInner>>>>,
        wal: Arc<wal::Wal<WalEntry>>,
        start: wal::SeqNo,
        wal_processed: tokio::sync::watch::Sender<wal::SeqNo>,
    ) -> anyhow::Result<()> {
        let mut log = wal.stream(start, true).boxed();
        while let Some((seqno, entry)) = log.try_next().await? {
            match entry {
                WalEntry::Write(ts, kvs) => {
                    for (keyspace_id, key, value) in kvs {
                        inner
                            .load()
                            .get(&keyspace_id)
                            .unwrap()
                            // TODO, just pull up a level so we don't have to reach down to inner here
                            .inner
                            .insert(seqno, key, ts, value);
                    }
                }
                WalEntry::Manifest(_, _, _) => {}
            }
            _ = wal_processed.send(seqno);
        }
        Ok(())
    }
}

struct LsmInner {
    inner: Arc<LsmInnerInner>,

    bg: Background,
    compacted: tokio::sync::broadcast::Receiver<()>,
}

impl LsmInner {
    pub async fn new(
        l0_max_size: u64,
        run_target_size: u64,
        block_size: u64,
        keyspace_id: KeyspaceId,
        manifest: Manifest,
        wal: Arc<wal::Wal<WalEntry>>,
        storage: Arc<MemStorage>,
    ) -> anyhow::Result<Self> {
        let (l0_compact_notify, l0_compact_ready) = tokio::sync::mpsc::channel::<()>(1);
        let (compacted_notify, compacted) = tokio::sync::broadcast::channel(1);
        let inner = Arc::new(LsmInnerInner::new(
            l0_max_size,
            run_target_size,
            block_size,
            manifest,
            l0_compact_notify,
        ));

        let bg = Background::new();
        bg.spawn(Self::compaction_loop(
            l0_max_size,
            run_target_size,
            block_size,
            keyspace_id,
            inner.clone(),
            storage.clone(),
            wal.clone(),
            l0_compact_ready,
            compacted_notify,
        ));

        Ok(Self {
            inner,
            bg,
            compacted,
        })
    }

    pub async fn get(
        &self,
        ts: Timestamp,
        key: &[u8],
    ) -> anyhow::Result<Option<(Timestamp, RevisionValue)>> {
        self.inner.get(ts, key).await
    }

    pub async fn scan_page(
        &self,
        ts: Timestamp,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<LsmRevision>, Option<Range<Vec<u8>>>)> {
        self.inner.scan_page(ts, range, direction, limit).await
    }

    pub async fn history_page(
        &self,
        key: &[u8],
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<(Timestamp, RevisionValue)>, Option<HistoryRange>)> {
        self.inner.history_page(key, range, direction, limit).await
    }

    pub fn next_compaction(&self) -> impl Future<Output = ()> {
        let mut compacted = self.compacted.resubscribe();
        async move {
            _ = compacted.recv().await;
            ()
        }
    }

    pub async fn pending_compactions(&self) {
        loop {
            let compacted = self.next_compaction();
            if self.inner.manifest.load().l0_sealed.len() == 0 {
                break;
            }
            compacted.await;
        }
    }

    async fn compaction_loop(
        l0_max_size: u64,
        run_target_size: u64,
        block_size: u64,
        keyspace_id: KeyspaceId,
        inner: Arc<LsmInnerInner>,
        storage: Arc<MemStorage>,
        wal: Arc<wal::Wal<WalEntry>>,
        mut l0_compact_ready: tokio::sync::mpsc::Receiver<()>,
        compacted_notify: tokio::sync::broadcast::Sender<()>,
    ) {
        while let Some(_) = l0_compact_ready.recv().await {
            Self::compact(
                l0_max_size,
                run_target_size,
                block_size,
                keyspace_id,
                inner.clone(),
                storage.clone(),
                &wal,
                compacted_notify.clone(),
            )
            .await
            .unwrap();
        }
    }

    async fn compact(
        l0_max_size: u64,
        run_target_size: u64,
        block_size: u64,
        keyspace_id: KeyspaceId,
        inner: Arc<LsmInnerInner>,
        storage: Arc<MemStorage>,
        wal: &wal::Wal<WalEntry>,
        compacted_notify: tokio::sync::broadcast::Sender<()>,
    ) -> anyhow::Result<()> {
        let mut manifest = inner.manifest.load();

        while !manifest.l0_sealed.is_empty() {
            let (runs, remove_ids, seqno) = Self::compact_l0(
                run_target_size,
                block_size,
                keyspace_id,
                &manifest,
                &storage,
            )
            .await?;
            if runs.is_empty() && remove_ids.is_empty() {
                return Ok(());
            }
            loop {
                let new_manifest =
                    Arc::new(manifest.with_ingest(1, runs.clone(), remove_ids.clone()));
                if inner
                    .manifest
                    .compare_and_swap(&manifest, new_manifest.clone())
                {
                    manifest = new_manifest;
                    break;
                }
                manifest = inner.manifest.load();
            }
            // This seqno might be split across multiple memtables, so we can't trim the max seen
            // yet.
            let seqno_ingested = wal::SeqNo(seqno.0.saturating_sub(1));
            wal.append(WalEntry::Manifest(
                keyspace_id,
                seqno_ingested,
                manifest
                    .levels
                    .iter()
                    .map(|level| level.runs.iter().map(|run| run.id()).collect::<Vec<_>>())
                    .collect::<Vec<_>>(),
            ))
            .await?;
            // TODO: We can delete any files that don't appear in the above manifest here.
            wal.trim(seqno_ingested).await?;

            'levels: for i in 1..manifest.levels.len() - 1 {
                while manifest.levels[i].size() as u64 > l0_max_size * 10_u64.pow(i as u32) {
                    let (runs, remove_ids) = Self::compact_from(
                        run_target_size,
                        block_size,
                        keyspace_id,
                        &manifest,
                        &storage,
                        i,
                    )
                    .await?;
                    if runs.is_empty() && remove_ids.is_empty() {
                        break 'levels;
                    }
                    loop {
                        let new_manifest =
                            Arc::new(manifest.with_ingest(i + 1, runs.clone(), remove_ids.clone()));
                        if inner
                            .manifest
                            .compare_and_swap(&manifest, new_manifest.clone())
                        {
                            manifest = new_manifest;
                            break;
                        }
                        manifest = inner.manifest.load();
                    }
                    // TODO: should probably write the manifest to WAL even though only l0
                    // compactions can move seqno_ingested because otherwise we're throwing away a
                    // bunch of our hard-earned compaction work
                }
            }
            let _ = compacted_notify.send(());
        }

        Ok(())
    }

    async fn compact_l0(
        run_target_size: u64,
        block_size: u64,
        keyspace_id: KeyspaceId,
        manifest: &Manifest,
        storage: &MemStorage,
    ) -> anyhow::Result<(Vec<Run<Arc<Vec<u8>>>>, HashSet<Uuid>, wal::SeqNo)> {
        // We must always compact the oldest l0, because get, etc. assume that everything in
        // memtables is newer than anything in any lower levels.
        let chosen_l0 = &manifest.l0_sealed[0];
        let chosen_l0_id = chosen_l0.id();
        let chosen_l0_range = chosen_l0.range();
        if chosen_l0_range.is_empty() {
            let mut removes = HashSet::new();
            removes.insert(chosen_l0_id);
            return Ok((vec![], removes, wal::SeqNo(0)));
        }

        let seqno = chosen_l0.max_seqno();

        let (new_runs, mut removes) = Self::compact_inner(
            run_target_size,
            block_size,
            keyspace_id,
            manifest,
            storage,
            1,
            chosen_l0_range,
            futures::stream::iter(chosen_l0.iter().map(|revision| Ok(revision))),
        )
        .await?;
        removes.insert(chosen_l0_id);

        Ok((new_runs, removes, seqno))
    }

    async fn compact_from(
        run_target_size: u64,
        block_size: u64,
        keyspace_id: KeyspaceId,
        manifest: &Manifest,
        storage: &MemStorage,
        level: usize,
    ) -> anyhow::Result<(Vec<Run<Arc<Vec<u8>>>>, HashSet<Uuid>)> {
        if manifest.levels[level].runs.is_empty() {
            return Ok((vec![], HashSet::new()));
        }
        let idx = rand::thread_rng().gen_range(0..manifest.levels[level].runs.len());
        let run = &manifest.levels[level].runs[idx];
        let run_range = run.range();

        let run_id = run.id();

        let (new_runs, mut removes) = Self::compact_inner(
            run_target_size,
            block_size,
            keyspace_id,
            manifest,
            storage,
            level + 1,
            run_range,
            run.stream(),
        )
        .await?;
        removes.insert(run_id);

        Ok((new_runs, removes))
    }

    async fn compact_inner(
        run_target_size: u64,
        block_size: u64,
        keyspace_id: KeyspaceId,
        manifest: &Manifest,
        storage: &MemStorage,
        into_level: usize,
        entries_range: Range<Vec<u8>>,
        entries: impl Stream<Item = anyhow::Result<LsmRevision>> + Send,
    ) -> anyhow::Result<(Vec<Run<Arc<Vec<u8>>>>, HashSet<Uuid>)> {
        let overlapping_runs = manifest.levels[into_level].overlapping_runs(entries_range);

        let removes = overlapping_runs.iter().map(|run| run.id()).collect();

        let existing_iter =
            futures::stream::iter(overlapping_runs.iter().map(Run::stream)).flatten();

        let mut sorted = merge_sorted_streams(vec![existing_iter.boxed(), entries.boxed()])
            .boxed()
            .peekable();

        let mut runs = Vec::new();
        while let Some(_) = Pin::new(&mut sorted).peek().await {
            let mut curr_size = 0u64;
            let (mut tx, rx) = futures::channel::mpsc::channel(256);

            let id = Uuid::new_v4();
            let (mut writer, reader) = tokio::io::duplex(16384);
            future::try_join3(
                storage.put(&id.to_string(), reader),
                async {
                    Run::<()>::write(&mut writer, id, keyspace_id, block_size, rx).await?;
                    drop(writer);
                    Ok(())
                },
                async {
                    while let Some(revision) = sorted.next().await.transpose()? {
                        let revision_size =
                            (revision.key.len() as u64) + 8 + (revision.value.len() as u64);
                        curr_size += revision_size;
                        let break_after = {
                            // All of the revisions for a single key need to end up in the same run, so once
                            // we've gone over the target size look for a break between keys.
                            if curr_size > run_target_size {
                                if let Some(Ok(next_revision)) = Pin::new(&mut sorted).peek().await
                                {
                                    if revision.key != next_revision.key {
                                        true
                                    } else {
                                        false
                                    }
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        };
                        tx.send(Ok(revision)).await?;
                        if break_after {
                            break;
                        }
                    }
                    drop(tx);

                    Ok(())
                },
            )
            .await?;

            let run = Run::open(storage.get(&id.to_string()).await?).await?;
            runs.push(run);
        }

        Ok((runs, removes))
    }
}

struct LsmInnerInner {
    l0_max_size: u64,
    run_target_size: u64,
    block_size: u64,

    l0_compact_notify: tokio::sync::mpsc::Sender<()>,
    l0_active: AtomicArc<RwLock<MaybeActiveMemtable>>,
    manifest: AtomicArc<Manifest>,
}

impl LsmInnerInner {
    fn new(
        l0_max_size: u64,
        run_target_size: u64,
        block_size: u64,
        initial_manifest: Manifest,
        l0_compact_notify: tokio::sync::mpsc::Sender<()>,
    ) -> Self {
        let l0_active = Arc::new(RwLock::new(MaybeActiveMemtable::Active(Memtable::new())));
        Self {
            l0_max_size,
            run_target_size,
            block_size,
            l0_compact_notify,
            l0_active: AtomicArc::new(l0_active.clone()),
            manifest: AtomicArc::new(Arc::new(initial_manifest.with_ingest_l0(l0_active))),
        }
    }

    async fn get(
        &self,
        ts: Timestamp,
        k: &[u8],
    ) -> anyhow::Result<Option<(Timestamp, RevisionValue)>> {
        let manifest = self.manifest.load();

        // Any memtable might have the latest for the key, so must check all of them.
        let maybe_revision = Iterator::chain(
            manifest
                .l0_active
                .iter()
                .map(|(_, memtable)| memtable.read().unwrap().get(ts, k)),
            manifest
                .l0_sealed
                .iter()
                .map(|memtable| memtable.get(ts, k)),
        )
        .filter_map(core::convert::identity)
        .max_by_key(|(ts, _)| *ts);
        if let Some((revision_ts, v)) = maybe_revision {
            return Ok(Some((revision_ts, v)));
        }
        for level in &manifest.levels {
            if let Some((revision_ts, v)) = level.get(ts, k).await? {
                return Ok(Some((revision_ts, v)));
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

        let manifest = self.manifest.load();

        let mut streams = Vec::with_capacity(
            manifest.l0_active.len() + manifest.l0_sealed.len() + manifest.levels.len(),
        );
        for l0_active in &manifest.l0_active {
            let l0_run = l0_active.1.read().unwrap();
            let revisions: Vec<_> = l0_run
                .scan(ts, range.clone(), direction)
                .map(|revision| Ok(revision))
                .collect();
            streams.push(futures::stream::iter(revisions.into_iter()).boxed());
        }
        for l0_run in &manifest.l0_sealed {
            streams.push(
                futures::stream::iter(
                    l0_run
                        .scan(ts, range.clone(), direction)
                        .map(|revision| Ok(revision)),
                )
                .boxed(),
            );
        }
        for i in 1..manifest.levels.len() {
            let overlapping_runs = manifest.levels[i].overlapping_runs(range.to_vec());

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
                    assert!(*last_ts > revision.ts);
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
        let manifest = self.manifest.load();

        let mut streams = Vec::with_capacity(manifest.levels.len());
        let mut l0_streams =
            Vec::with_capacity(manifest.l0_active.len() + manifest.l0_sealed.len());
        for l0_active in &manifest.l0_active {
            let l0_run = l0_active.1.read().unwrap();
            let revisions: Vec<_> = l0_run
                .history(key, range, direction)
                .map(|revision| Ok(revision))
                .collect();
            l0_streams.push(futures::stream::iter(revisions.into_iter()).boxed());
        }
        for l0_run in &manifest.l0_sealed {
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

        for level in &manifest.levels[1..] {
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

    fn insert(&self, seqno: wal::SeqNo, k: Vec<u8>, ts: Timestamp, v: RevisionValue) {
        loop {
            let l0_active = self.l0_active.load();
            let overfilled = {
                let mut guard = l0_active.write().unwrap();
                if let MaybeActiveMemtable::Active(memtable) = &mut *guard {
                    let pre_size = memtable.size();
                    let post_size = memtable.insert(seqno, k.clone(), ts, v.clone());
                    pre_size < self.l0_max_size && post_size >= self.l0_max_size
                } else {
                    // Only happens if there's already a new one inserted into self.l0_active, so
                    // just try again.
                    continue;
                }
            };
            if overfilled {
                let old_memtable_id = { l0_active.read().unwrap().id() };
                // Make a new memtable.
                let new_memtable =
                    Arc::new(RwLock::new(MaybeActiveMemtable::Active(Memtable::new())));
                // Add it to the manifest, so that by the time it receives any writes, it's
                // also visible to readers.
                loop {
                    let manifest = self.manifest.load();
                    if self.manifest.compare_and_swap(
                        &manifest,
                        Arc::new(manifest.with_ingest_l0(new_memtable.clone())),
                    ) {
                        break;
                    }
                }
                // Swap the new memtable in for self.l0_active, so that's where all of the new
                // writes go.
                self.l0_active.compare_and_swap(&l0_active, new_memtable);

                // Seal the old memtable and mark it as such in the manifest so it's eligible for
                // compaction.
                loop {
                    let manifest = self.manifest.load();
                    if self.manifest.compare_and_swap(
                        &manifest,
                        Arc::new(manifest.with_mark_sealed(old_memtable_id)),
                    ) {
                        break;
                    }
                }

                let _ = self.l0_compact_notify.try_send(());
            }
            return;
        }
    }
}

enum MaybeActiveMemtable {
    Active(Memtable),
    Sealed(Arc<Memtable>),
}

impl Deref for MaybeActiveMemtable {
    type Target = Memtable;

    fn deref(&self) -> &Self::Target {
        match self {
            MaybeActiveMemtable::Active(memtable) => &memtable,
            MaybeActiveMemtable::Sealed(arc_memtable) => &arc_memtable,
        }
    }
}

impl MaybeActiveMemtable {
    fn seal(self) -> (Self, Arc<Memtable>) {
        match self {
            MaybeActiveMemtable::Active(memtable) => {
                let arc = Arc::new(memtable);
                (MaybeActiveMemtable::Sealed(arc.clone()), arc)
            }
            MaybeActiveMemtable::Sealed(ref arc_memtable) => {
                let arc = arc_memtable.clone();
                (self, arc)
            }
        }
    }
}

// Mostly immutable, except that:
// (a) memtables in l0_active may still be receiving writes.
// (b) l0_active memtables may swap from active to sealed.
struct Manifest {
    // May still be receiving writes.
    l0_active: Vec<(Uuid, Arc<RwLock<MaybeActiveMemtable>>)>,
    // Guaranteed to be read-only. In insertion order.
    l0_sealed: Vec<Arc<Memtable>>,
    levels: Vec<Level>,
}

impl Manifest {
    fn new(n_levels: usize) -> Self {
        Self {
            l0_active: vec![],
            l0_sealed: vec![],
            levels: (0..n_levels).map(|_| Level::new()).collect(),
        }
    }

    fn with_mark_sealed(&self, id: Uuid) -> Self {
        let mut l0_active = Vec::with_capacity(self.l0_active.len() - 1);
        let mut l0_sealed = self.l0_sealed.clone();

        for (memtable_id, arc_rwlock_memtable) in &self.l0_active {
            if *memtable_id == id {
                let mut guard = arc_rwlock_memtable.write().unwrap();
                let mut temp = MaybeActiveMemtable::Sealed(Arc::new(Memtable::new()));
                // Awkward, but we need ownership, so we'd have to wrap with an Option and deal with it
                // being missing or we have to make a temporary one that we'll destroy in a second.
                std::mem::swap(guard.deref_mut(), &mut temp);
                let (new_maybe_active_memtable, memtable) = temp.seal();
                *guard = new_maybe_active_memtable;
                l0_sealed.push(memtable);
            } else {
                l0_active.push((*memtable_id, arc_rwlock_memtable.clone()));
            }
        }

        Self {
            l0_active,
            l0_sealed,
            levels: self.levels.clone(),
        }
    }

    fn with_ingest_l0(&self, memtable: Arc<RwLock<MaybeActiveMemtable>>) -> Self {
        let id = memtable.read().unwrap().id();
        Self {
            l0_active: self
                .l0_active
                .iter()
                .chain(std::iter::once(&(id, memtable)))
                .map(|(id, table)| (id.clone(), table.clone()))
                .collect(),
            l0_sealed: self.l0_sealed.clone(),
            levels: self.levels.clone(),
        }
    }

    // TODO: Return an error if not all `remove`s appear in the manifest.
    fn with_ingest(
        &self,
        into_level: usize,
        mut add: Vec<Run<Arc<Vec<u8>>>>,
        remove: HashSet<Uuid>,
    ) -> Self {
        let mut levels = Vec::with_capacity(self.levels.len());
        for (i, old_level) in self.levels.iter().enumerate() {
            let filtered_old_level = old_level
                .runs
                .iter()
                .filter(|run| !remove.contains(&((*run).id())))
                .cloned();
            if i == into_level {
                levels.push(Level {
                    runs: merge_sorted(vec![
                        IteratorEither::Left(
                            filtered_old_level.map(|run| OrdEqByFirst(run.range().lower, run)),
                        ),
                        IteratorEither::Right(
                            std::mem::take(&mut add)
                                .into_iter()
                                .map(|run| OrdEqByFirst(run.range().lower, run)),
                        ),
                    ])
                    .map(|OrdEqByFirst(_, run)| run)
                    .collect(),
                });
            } else {
                levels.push(Level {
                    runs: filtered_old_level.collect(),
                });
            }
        }
        Self {
            l0_active: self.l0_active.clone(),
            l0_sealed: self
                .l0_sealed
                .iter()
                .filter(|memtable| !remove.contains(&memtable.id()))
                .cloned()
                .collect(),
            levels,
        }
    }
}

#[derive(Clone)]
struct Level {
    // In sorted order by range.
    runs: Vec<Run<Arc<Vec<u8>>>>,
}

impl Level {
    fn new() -> Self {
        Self { runs: vec![] }
    }

    async fn get(
        &self,
        ts: Timestamp,
        k: &[u8],
    ) -> anyhow::Result<Option<(Timestamp, RevisionValue)>> {
        let run = match self.run_for_key(k) {
            Some(run) => run,
            None => return Ok(None),
        };
        run.get(ts, k).await
    }

    fn size(&self) -> usize {
        self.runs.iter().map(|run| run.size()).sum()
    }

    fn run_for_key<'a>(&'a self, k: &[u8]) -> Option<&'a Run<Arc<Vec<u8>>>> {
        let idx = self
            .runs
            .binary_search_by_key(&KeyOrBound::Key(k.to_vec()), |run| {
                KeyOrBound::Bound(run.range().upper)
            })
            .unwrap_or_else(core::convert::identity);
        if idx >= self.runs.len() {
            return None;
        }
        let run = &self.runs[idx];
        if !run.range().contains(&k.to_vec()) {
            return None;
        }
        Some(run)
    }

    fn overlapping_runs(&self, range: Range<Vec<u8>>) -> &[Run<Arc<Vec<u8>>>] {
        intersect_in_ranges_by_key(range.borrow(), &self.runs, |run| run.range())
    }
}

#[derive(Clone, Debug)]
pub(crate) enum WalEntry {
    Write(Timestamp, Vec<(KeyspaceId, Vec<u8>, RevisionValue)>),
    Manifest(KeyspaceId, wal::SeqNo, Vec<Vec<Uuid>>),
}

impl wal::Entry for WalEntry {
    fn size(&self) -> u64 {
        match self {
            WalEntry::Write(_, kvs) => {
                8 + kvs.iter().map(|(_, k, v)| k.len() + v.len()).sum::<usize>() as u64
            }
            WalEntry::Manifest(_, _, levels) => {
                16u64 + (levels.iter().map(|level| level.len() as u64).sum::<u64>() * 16u64)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::RwLock;
    use std::time::Duration;

    use byteorder::BigEndian;
    use byteorder::ByteOrder;
    use futures::TryStreamExt;
    use proptest::prelude::*;
    use uuid::Uuid;

    use super::Level;
    use super::Lsm;
    use super::LsmBuilder;
    use super::LsmInnerInner;
    use super::Manifest;
    use super::MaybeActiveMemtable;
    use crate::lsm::memtable::Memtable;
    use crate::lsm::run::dump_run;
    use crate::lsm::run::Run;
    use crate::lsm::util::LsmRevision;
    use crate::range::Bound;
    use crate::range::Range;
    use crate::storage::MemStorage;
    use crate::types::ColoGroupId;
    use crate::types::Direction;
    use crate::types::HistoryRange;
    use crate::types::KeyspaceId;
    use crate::types::Mutation;
    use crate::types::Precondition;
    use crate::types::Revision;
    use crate::types::RevisionValue;
    use crate::types::Timestamp;
    use crate::types::WriteError;
    use crate::util::binary_search_by_idx;
    use crate::wal;
    use crate::wal::SeqNo;

    #[tokio::test]
    async fn test_put_get() -> anyhow::Result<()> {
        let lsm = LsmBuilder::new().build().await?;
        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
        let k = b"abc";
        let not_k = b"def";
        let v = b"foo";

        lsm.create_keyspace(keyspace_id).await?;
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
        let lsm = LsmBuilder::new().build().await?;

        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
        let ka = b"a";
        let kb = b"b";

        lsm.create_keyspace(keyspace_id).await?;
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
        let lsm = LsmBuilder::new()
            .l0_max_size(128)
            .block_size(128)
            .run_target_size(512)
            .build()
            .await?;
        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
        lsm.create_keyspace(keyspace_id).await?;
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
            lsm.inner
                .load()
                .get(&keyspace_id)
                .unwrap()
                .inner
                .manifest
                .load()
                .levels[1]
                .runs
                .len()
                >= 1
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_compact_l1() -> anyhow::Result<()> {
        let lsm = LsmBuilder::new()
            .l0_max_size(128)
            .block_size(128)
            .run_target_size(512)
            .build()
            .await?;
        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
        lsm.create_keyspace(keyspace_id).await?;
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
                    .inner
                    .load()
                    .get(&keyspace_id)
                    .unwrap()
                    .inner
                    .manifest
                    .load()
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
        let wal = Arc::new(wal::Wal::new(16, Duration::from_millis(2)));
        let storage = Arc::new(MemStorage::new());

        let lsm = LsmBuilder::new()
            .l0_max_size(128)
            .block_size(128)
            .run_target_size(512)
            .wal(wal.clone())
            .storage(storage.clone())
            .build()
            .await?;

        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
        lsm.create_keyspace(keyspace_id).await?;

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
            lsm.inner
                .load()
                .get(&keyspace_id)
                .unwrap()
                .inner
                .manifest
                .load()
                .levels[1]
                .runs
                .len()
                >= 1
        );

        drop(lsm);

        // Rebuild the LSM from the same WAL and storage, this should recover everything.
        let lsm = LsmBuilder::new().wal(wal).storage(storage).build().await?;

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
        let lsm = LsmBuilder::new()
            .l0_max_size(32)
            .block_size(48)
            .run_target_size(96)
            .build()
            .await?;
        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
        lsm.create_keyspace(keyspace_id).await?;

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

        let lsm = lsm_from_diagram(diagram).await?;

        async fn check(
            lsm: &LsmInnerInner,
            key: &[u8],
            range: HistoryRange,
            expected: &[(usize, bool)],
        ) -> anyhow::Result<()> {
            for direction in [Direction::Asc, Direction::Desc] {
                for page_size in 1..=expected.len() {
                    let mut maybe_cursor = Some(range.clone());
                    let mut results = vec![];
                    while let Some(cursor) = maybe_cursor {
                        let (page, continue_cursor) =
                            lsm.history_page(key, cursor, direction, page_size).await?;

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

        dump_lsm_inner_inner(&lsm).await?;

        let all_b_versions = vec![
            (1, false),
            (3, true),
            (4, false),
            (6, false),
            (7, false),
            (8, true),
            (9, false),
        ];

        check(&lsm, b"b", HistoryRange::All, &all_b_versions).await?;

        check(
            &lsm,
            b"b",
            HistoryRange::Between(Timestamp(1), Timestamp(9)),
            &all_b_versions,
        )
        .await?;

        check(
            &lsm,
            b"b",
            HistoryRange::Until(Timestamp(9)),
            &all_b_versions,
        )
        .await?;

        check(
            &lsm,
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

                let lsm = LsmBuilder::new()
                    .l0_max_size(128)
                    .block_size(128)
                    .run_target_size(512)
                    .build()
                    .await
                    .unwrap();
                let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
                lsm.create_keyspace(keyspace_id).await.unwrap();

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
        let inner = lsm.inner.load();
        for (keyspace_id, lsm) in &*inner {
            println!("keyspace_id {:?}", keyspace_id);
            dump_lsm_inner_inner(&lsm.inner).await?;
        }

        Ok(())
    }

    async fn dump_lsm_inner_inner(lsm: &LsmInnerInner) -> anyhow::Result<()> {
        let manifest = lsm.manifest.load();
        dump_manifest(&manifest);

        println!("== kvs =====");
        println!("l0_active");
        for (_, memtable_lock) in &manifest.l0_active {
            let memtable = memtable_lock.read().unwrap();
            println!(
                "  {} ({} bytes) {:?}",
                memtable.id(),
                memtable.size(),
                memtable.range(),
            );
            memtable.dump();
        }
        println!("l0_sealed");
        for memtable in &manifest.l0_sealed {
            println!(
                "  {} ({} bytes) {:?}",
                memtable.id(),
                memtable.size(),
                memtable.range(),
            );
            memtable.dump();
        }
        for (i, level) in manifest.levels[1..]
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

    fn dump_manifest(manifest: &Manifest) {
        println!("== manifest =====");
        println!("l0_active");
        for (_, memtable_lock) in &manifest.l0_active {
            let memtable = memtable_lock.read().unwrap();
            println!(
                "  {} ({} bytes) {:?}",
                memtable.id(),
                memtable.size(),
                memtable.range(),
            );
        }
        println!("l0_sealed");
        for memtable in &manifest.l0_sealed {
            println!(
                "  {} ({} bytes) {:?}",
                memtable.id(),
                memtable.size(),
                memtable.range(),
            );
        }
        for (i, level) in manifest.levels[1..]
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
    }

    #[tokio::test]
    async fn test_lsm_from_diagram() -> anyhow::Result<()> {
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

        let lsm = lsm_from_diagram(diagram).await?;
        let manifest = lsm.manifest.load();
        let l0_active_guard = manifest.l0_active[0].1.write().unwrap();

        let a = "a";
        let b = "b";
        let c = "c";
        let d = "d";

        assert_eq!(
            l0_active_guard.iter().collect::<Vec<_>>(),
            vec![
                lsm_diagram_revision(b.as_bytes(), 9, false),
                lsm_diagram_revision(d.as_bytes(), 9, false),
            ],
        );
        assert_eq!(
            manifest.l0_sealed[0].iter().collect::<Vec<_>>(),
            vec![
                lsm_diagram_revision(a.as_bytes(), 9, false),
                lsm_diagram_revision(a.as_bytes(), 8, false),
                lsm_diagram_revision(b.as_bytes(), 8, true),
                lsm_diagram_revision(c.as_bytes(), 9, true),
                lsm_diagram_revision(c.as_bytes(), 8, false),
            ],
        );

        assert_eq!(
            manifest.levels[1..]
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

    async fn lsm_from_diagram(diagram: Vec<(&str, &[u8])>) -> anyhow::Result<LsmInnerInner> {
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
        let mut l0_active = Memtable::new();
        for revision in l0_active_revisions {
            l0_active.insert(SeqNo(1), revision.key, revision.ts, revision.value);
        }
        let mut l0_sealed = Memtable::new();

        let mut manifest = Manifest::new(1);
        for x in (0..=x_max).rev().filter(|x| x % 2 == 1) {
            let mut level = Level::new();
            for y in (0..diagram.len()).filter(|y| y % 2 == 0) {
                let revisions = find_touching(&diagram[..], &mut visited, x, y);
                if revisions.is_empty() {
                    continue;
                }

                if l0_sealed.size() == 0 {
                    for revision in revisions {
                        l0_sealed.insert(SeqNo(1), revision.key, revision.ts, revision.value);
                    }
                } else {
                    let mut v = vec![];
                    Run::<()>::write(
                        &mut v,
                        Uuid::new_v4(),
                        KeyspaceId(ColoGroupId(1), 1),
                        1024, // block_size
                        futures::stream::iter(revisions.into_iter().map(|revision| Ok(revision))),
                    )
                    .await?;
                    let run = Run::open(Arc::new(v)).await?;
                    level.runs.push(run);
                }
            }

            if level.runs.is_empty() {
                continue;
            }

            manifest.levels.push(level);
        }

        manifest.l0_active = vec![(
            l0_active.id(),
            Arc::new(RwLock::new(MaybeActiveMemtable::Active(l0_active))),
        )];
        manifest.l0_sealed = vec![Arc::new(l0_sealed)];

        let (l0_compact_notify, _) = tokio::sync::mpsc::channel::<()>(1);
        let lsm = LsmInnerInner::new(64_000_000, 64_000_000, 32768, manifest, l0_compact_notify);

        Ok(lsm)
    }
}
