#![allow(dead_code)]
#![feature(generators)]
#![feature(is_sorted)]
#![feature(map_first_last)]

use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ops::DerefMut;
use std::os::unix::fs::FileExt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::anyhow;
use async_stream::stream;
use async_stream::try_stream;
use async_trait::async_trait;
use byteorder::ByteOrder;
use byteorder::LittleEndian;
use futures::pin_mut;
use futures::stream::Stream;
use futures::stream::StreamExt;
use futures::SinkExt;
use rand::Rng;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

mod memtable;
mod range;
mod sequencer;

use crate::memtable::Memtable;
use crate::range::Bound;
use crate::range::KeyOrBound;
use crate::range::Range;
use crate::sequencer::Sequencer;

struct LsmBuilder {
    l0_max_size: u64,
    run_target_size: u64,
    block_size: u64,
}

impl LsmBuilder {
    fn new() -> Self {
        LsmBuilder {
            l0_max_size: 8_000_000,
            run_target_size: 64_000_000,
            block_size: 32768,
        }
    }

    fn l0_max_size(mut self, x: u64) -> Self {
        self.l0_max_size = x;
        self
    }

    fn run_target_size(mut self, x: u64) -> Self {
        self.run_target_size = x;
        self
    }

    fn block_size(mut self, x: u64) -> Self {
        self.block_size = x;
        self
    }

    fn build(self) -> Lsm {
        Lsm::new(self.l0_max_size, self.run_target_size, self.block_size)
    }
}

struct Lsm {
    inner: Arc<LsmInner>,
    compaction: tokio::task::JoinHandle<()>,
    compacted: tokio::sync::broadcast::Receiver<()>,
}

impl Lsm {
    pub fn new(l0_max_size: u64, run_target_size: u64, block_size: u64) -> Self {
        let (l0_compact_notify, l0_compact_ready) = tokio::sync::mpsc::channel::<()>(1);
        let (compacted_notify, compacted) = tokio::sync::broadcast::channel(1);
        let inner = Arc::new(LsmInner::new(
            l0_max_size,
            run_target_size,
            block_size,
            l0_compact_notify,
        ));
        let inner_ = inner.clone();
        let compaction = tokio::spawn(Self::compaction_loop(
            run_target_size,
            block_size,
            inner_.clone(),
            l0_compact_ready,
            compacted_notify,
        ));
        Self {
            inner,
            compaction,
            compacted,
        }
    }

    pub async fn get(&self, ts: u64, k: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get(ts, k).await
    }

    pub async fn scan_page(
        &self,
        ts: u64,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<Record>, Option<Range<Vec<u8>>>)> {
        self.inner.scan_page(ts, range, direction, limit).await
    }

    pub async fn put(&self, k: Vec<u8>, v: Vec<u8>) -> anyhow::Result<u64> {
        self.inner.put(k, v).await
    }

    pub async fn next_compaction(&self) {
        let mut compacted = self.compacted.resubscribe();
        let _ = compacted.recv().await;
    }

    async fn compaction_loop(
        run_target_size: u64,
        block_size: u64,
        inner: Arc<LsmInner>,
        mut l0_compact_ready: tokio::sync::mpsc::Receiver<()>,
        compacted_notify: tokio::sync::broadcast::Sender<()>,
    ) {
        while let Some(_) = l0_compact_ready.recv().await {
            Self::compact(
                run_target_size,
                block_size,
                inner.clone(),
                compacted_notify.clone(),
            )
            .await
            .unwrap();
        }
    }

    async fn compact(
        run_target_size: u64,
        block_size: u64,
        inner: Arc<LsmInner>,
        compacted_notify: tokio::sync::broadcast::Sender<()>,
    ) -> anyhow::Result<()> {
        let mut manifest = inner.manifest.load();

        if manifest.l0_sealed.is_empty() {
            // Wait for the next signal.
            return Ok(());
        }

        let (runs, remove_ids) = Self::compact_l0(run_target_size, block_size, &manifest).await?;
        if runs.is_empty() && remove_ids.is_empty() {
            return Ok(());
        }
        loop {
            if inner.manifest.compare_and_swap(
                &manifest,
                Arc::new(manifest.with_ingest(1, runs.clone(), remove_ids.clone())),
            ) {
                break;
            }
            manifest = inner.manifest.load();
        }
        let _ = compacted_notify.send(());

        //for i in 1..self.levels.len() - 1 {
        //    if self.levels[i].size() as u64 <= self.l0_max_size * 10_u64.pow(i as u32) {
        //        break;
        //    }
        //    self.compact_from(i).await?;
        //}

        Ok(())
    }

    async fn compact_l0(
        run_target_size: u64,
        block_size: u64,
        manifest: &Manifest,
    ) -> anyhow::Result<(Vec<Run<Vec<u8>>>, HashSet<Uuid>)> {
        let (chosen_l0_id, chosen_l0) = manifest
            .l0_sealed
            .iter()
            .skip(rand::thread_rng().gen_range(0..manifest.l0_sealed.len()))
            .next()
            .unwrap();
        let (min_key, max_key) = match chosen_l0.range() {
            Some(r) => r,
            // l0 is empty, nothing to do
            None => return Ok((vec![], HashSet::new())),
        };

        let (new_runs, mut removes) = Self::compact_inner(
            run_target_size,
            block_size,
            manifest,
            1,
            min_key,
            max_key,
            futures::stream::iter(chosen_l0.iter().map(|(k, ts, v)| {
                Ok(Record {
                    key: k,
                    ts,
                    value: v,
                })
            })),
        )
        .await?;
        removes.insert(*chosen_l0_id);

        Ok((new_runs, removes))
    }

    //async fn compact_from(&mut self, level: usize) -> anyhow::Result<()> {
    //    if self.levels[level].runs.is_empty() {
    //        return Ok(());
    //    }
    //    let idx = rand::thread_rng().gen_range(0..self.levels[level].runs.len());
    //    let run = self.levels[level].runs.remove(idx);
    //    let (min_key, max_key) = match run.range() {
    //        Some((min_key, max_key)) => (min_key, max_key),
    //        None => return Ok(()),
    //    };
    //    self.compact_inner(level + 1, min_key, max_key, run.stream())
    //        .await
    //}

    async fn compact_inner(
        run_target_size: u64,
        block_size: u64,
        manifest: &Manifest,
        into_level: usize,
        min_key: Vec<u8>,
        max_key: Vec<u8>,
        entries: impl Stream<Item = anyhow::Result<Record>> + Send,
    ) -> anyhow::Result<(Vec<Run<Vec<u8>>>, HashSet<Uuid>)> {
        let overlapping_runs = manifest.levels[into_level].overlapping_runs(min_key, max_key);

        let removes = overlapping_runs.iter().map(|run| run.id()).collect();

        let existing_iter = futures::stream::iter(overlapping_runs.iter().map(Run::stream))
            .flatten()
            .map(|result| {
                result.map(|record| OrdEqByFirst((record.key, Reverse(record.ts)), record.value))
            });

        let mut sorted = merge_sorted_streams(vec![
            existing_iter.boxed(),
            entries
                .map(|result| {
                    result
                        .map(|record| OrdEqByFirst((record.key, Reverse(record.ts)), record.value))
                })
                .boxed(),
        ])
        .map(|result| {
            result.map(|OrdEqByFirst((key, Reverse(ts)), value)| Record { key, ts, value })
        })
        .boxed()
        .peekable();

        let mut runs = Vec::new();
        while let Some(_) = Pin::new(&mut sorted).peek().await {
            let mut curr_size = 0u64;
            let (mut tx, rx) = futures::channel::mpsc::channel(1);

            let run_handle = tokio::spawn(async move {
                let mut run_out = vec![];
                Run::<()>::write(&mut run_out, 0, block_size, rx).await?;
                Ok::<_, anyhow::Error>(run_out)
            });

            while let Some(record) = sorted.next().await.transpose()? {
                let record_size = (record.key.len() as u64) + 8 + (record.value.len() as u64);
                curr_size += record_size;
                let break_after = {
                    // All of the records for a single key need to end up in the same run, so once
                    // we've gone over the target size look for a break between keys.
                    if curr_size > run_target_size {
                        if let Some(Ok(next_record)) = Pin::new(&mut sorted).peek().await {
                            if record.key != next_record.key {
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
                tx.send(Ok(record)).await?;
                if break_after {
                    break;
                }
            }
            drop(tx);
            let run_out = run_handle.await??;
            let run_size = run_out.len();
            runs.push(Run::open(run_out, run_size).await?);
        }

        Ok((runs, removes))
    }
}

impl Drop for Lsm {
    fn drop(&mut self) {
        self.compaction.abort();
    }
}

struct LsmInner {
    l0_max_size: u64,
    run_target_size: u64,
    block_size: u64,

    l0_compact_notify: tokio::sync::mpsc::Sender<()>,
    sequencer: Sequencer,
    // Outer lock just protects the arc, hold in W to swap it.
    // Inner lock protects memtable, use as normal.
    // Also exists in manifest.l0_active, so reads can go there.
    l0_active: AtomicArc<RwLock<MaybeActiveMemtable>>,
    manifest: AtomicArc<Manifest>,
}

impl LsmInner {
    fn new(
        l0_max_size: u64,
        run_target_size: u64,
        block_size: u64,
        l0_compact_notify: tokio::sync::mpsc::Sender<()>,
    ) -> Self {
        let l0_active = Arc::new(RwLock::new(MaybeActiveMemtable::Active(Memtable::new())));
        Self {
            l0_max_size,
            run_target_size,
            block_size,
            l0_compact_notify,
            sequencer: Sequencer::new(),
            l0_active: AtomicArc::new(l0_active.clone()),
            manifest: AtomicArc::new(Arc::new(Manifest::new(7).with_ingest_l0(l0_active))),
        }
    }

    async fn get(&self, ts: u64, k: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        self.sequencer.wait_for_safe_read(ts).await?;

        let manifest = self.manifest.load();

        // Any memtable might have the latest for the key, so must check all of them.
        let maybe_record = Iterator::chain(
            manifest
                .l0_active
                .iter()
                .map(|(_, memtable)| memtable.read().unwrap().get(ts, k)),
            manifest
                .l0_sealed
                .values()
                .map(|memtable| memtable.get(ts, k)),
        )
        .filter_map(core::convert::identity)
        .max_by_key(|(ts, _)| *ts);
        if let Some((_, v)) = maybe_record {
            return match v {
                Value::Regular(v) => Ok(Some(v)),
                Value::Tombstone => Ok(None),
            };
        }
        for level in &manifest.levels {
            if let Some((_, v)) = level.get(ts, k).await? {
                return match v {
                    Value::Regular(v) => Ok(Some(v)),
                    Value::Tombstone => Ok(None),
                };
            }
        }
        Ok(None)
    }

    async fn scan_page(
        &self,
        ts: u64,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<Record>, Option<Range<Vec<u8>>>)> {
        self.sequencer.wait_for_safe_read(ts).await?;
        if range.is_empty() {
            return Ok((vec![], None));
        }

        if direction == Direction::Desc {
            todo!();
        }

        let manifest = self.manifest.load();

        let l0_active_guards: Vec<_> = manifest
            .l0_active
            .iter()
            .map(|(_, memtable)| memtable.read().unwrap())
            .collect();

        let mut streams = Vec::with_capacity(
            manifest.l0_active.len() + manifest.l0_sealed.len() + manifest.levels.len(),
        );
        for l0_run in &l0_active_guards {
            streams.push(
                futures::stream::iter(l0_run.scan_asc(ts, range.clone()).map(|record| Ok(record)))
                    .boxed(),
            );
        }
        for l0_run in manifest.l0_sealed.values() {
            streams.push(
                futures::stream::iter(l0_run.scan_asc(ts, range.clone()).map(|record| Ok(record)))
                    .boxed(),
            );
        }
        for i in 1..manifest.levels.len() {
            let level = &manifest.levels[i];
            if level.runs.is_empty() {
                continue;
            }
            let start_idx = binary_search_by_idx(
                level.runs.len(),
                range.lower.clone().map(Vec::from),
                |idx| Bound::After(level.runs[idx].range().unwrap().1),
            )
            .unwrap_or_else(core::convert::identity);
            let end_idx = binary_search_by_idx(
                level.runs.len(),
                range.upper.clone().map(Vec::from),
                |idx| Bound::Before(level.runs[idx].range().unwrap().0),
            )
            .unwrap_or_else(|idx| {
                if idx == level.runs.len() {
                    idx - 1
                } else {
                    idx
                }
            });

            if end_idx < start_idx {
                continue;
            }

            streams.push(
                futures::stream::iter(level.runs[start_idx..=end_idx].iter())
                    .map(|run| run.scan(ts, range.to_vec(), direction))
                    .flatten()
                    .boxed(),
            );
        }
        let mut merged = merge_sorted_streams(streams).peekable().boxed_local();
        let mut page = vec![];
        while let Some(record) = merged.next().await.transpose()? {
            if let Some(Record {
                key: last_key,
                ts: last_ts,
                ..
            }) = page.last()
            {
                if last_key == &record.key {
                    assert!(*last_ts > record.ts);
                    continue;
                }
            }
            if let Value::Tombstone = record.value {
                continue;
            }
            page.push(record);
            if page.len() == limit {
                break;
            }
        }

        let continue_cursor = match page.last() {
            Some(Record { key: last_key, .. }) => Some(Range {
                lower: Bound::After(last_key.clone()),
                upper: range.upper.clone().map(Vec::from),
            }),
            None => None,
        };
        Ok((page, continue_cursor))
    }

    async fn put(&self, k: Vec<u8>, v: Vec<u8>) -> anyhow::Result<u64> {
        let ts = self.sequencer.start_write();
        loop {
            let l0_active = self.l0_active.load();
            let new_size = {
                let mut guard = l0_active.write().unwrap();
                if let MaybeActiveMemtable::Active(memtable) = &mut *guard {
                    memtable.put(k.clone(), ts, v.clone())
                } else {
                    // Only happens if there's already a new one inserted into self.l0_active, so
                    // just try again.
                    continue;
                }
            };
            self.sequencer.finish_write(ts);
            if new_size > self.l0_max_size {
                // TODO: Choose which thread is going to do this.
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
            return Ok(ts);
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
    l0_active: HashMap<Uuid, Arc<RwLock<MaybeActiveMemtable>>>,
    // Guaranteed to be read-only.
    l0_sealed: HashMap<Uuid, Arc<Memtable>>,
    levels: Vec<Level>,
}

impl Manifest {
    fn new(n_levels: usize) -> Self {
        Self {
            l0_active: HashMap::new(),
            l0_sealed: HashMap::new(),
            levels: (0..n_levels).map(|_| Level::new()).collect(),
        }
    }

    fn with_mark_sealed(&self, id: Uuid) -> Self {
        let mut l0_active = self.l0_active.clone();
        let mut l0_sealed = self.l0_sealed.clone();

        if let Some(maybe_active_memtable) = l0_active.remove(&id) {
            let mut guard = maybe_active_memtable.write().unwrap();
            let mut temp = MaybeActiveMemtable::Sealed(Arc::new(Memtable::new()));
            // Awkward, but we need ownership, so we'd have to wrap with an Option and deal with it
            // being missing or we have to make a temporary one that we'll destroy in a second.
            std::mem::swap(guard.deref_mut(), &mut temp);
            let (new_maybe_active_memtable, memtable) = temp.seal();
            *guard = new_maybe_active_memtable;
            l0_sealed.insert(id, memtable);
        }

        Self {
            l0_active,
            l0_sealed,
            levels: self.levels.clone(),
        }
    }

    fn with_ingest_l0(&self, memtable: Arc<RwLock<MaybeActiveMemtable>>) -> Self {
        Self {
            l0_active: self
                .l0_active
                .iter()
                .chain(std::iter::once((&memtable.read().unwrap().id(), &memtable)))
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
        mut add: Vec<Run<Vec<u8>>>,
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
                        Box::new(
                            filtered_old_level.map(|run| OrdEqByFirst(run.range().unwrap().0, run)),
                        ) as Box<dyn Iterator<Item = _>>,
                        Box::new(
                            std::mem::take(&mut add)
                                .into_iter()
                                .map(|run| OrdEqByFirst(run.range().unwrap().0, run)),
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
                .filter(|(id, _)| !remove.contains(&id))
                .map(|(id, memtable)| (id.clone(), memtable.clone()))
                .collect(),
            levels,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Value {
    Regular(Vec<u8>),
    Tombstone,
}

impl Value {
    fn len(&self) -> usize {
        match self {
            Value::Regular(v) => v.len(),
            Value::Tombstone => 0,
        }
    }
}

#[derive(Clone)]
struct Level {
    // In sorted order by range.
    runs: Vec<Run<Vec<u8>>>,
}

impl Level {
    fn new() -> Self {
        Self { runs: vec![] }
    }

    async fn get(&self, ts: u64, k: &[u8]) -> anyhow::Result<Option<(u64, Value)>> {
        let idx = match self
            .runs
            .binary_search_by_key(&k.to_vec(), |run| run.range().unwrap().1)
        {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        if idx >= self.runs.len() {
            return Ok(None);
        }
        self.runs[idx].get(ts, k).await
    }

    fn size(&self) -> usize {
        self.runs.iter().map(|run| run.size()).sum()
    }

    fn overlapping_runs(&self, min_key: Vec<u8>, max_key: Vec<u8>) -> &[Run<Vec<u8>>] {
        let start_idx = self
            .runs
            .binary_search_by_key(&min_key, |run| run.range().unwrap().1)
            .unwrap_or_else(core::convert::identity);

        let end_idx = match self
            .runs
            .binary_search_by_key(&max_key, |run| run.range().unwrap().0)
        {
            Ok(idx) => idx + 1,
            Err(idx) => idx,
        };

        &self.runs[start_idx..end_idx]
    }
}

pub fn merge_sorted<'a, T: Ord + 'a>(
    mut iters: Vec<impl Iterator<Item = T> + 'a>,
) -> impl Iterator<Item = T> + 'a {
    let mut h: BinaryHeap<(std::cmp::Reverse<T>, usize)> = BinaryHeap::new();
    h.reserve_exact(iters.len());
    for i in 0..iters.len() {
        if let Some(t) = iters[i].next() {
            h.push((std::cmp::Reverse(t), i));
        }
    }
    std::iter::from_fn(move || {
        let (t, i) = h.pop()?;
        if let Some(t) = iters[i].next() {
            h.push((std::cmp::Reverse(t), i));
        }
        Some(t.0)
    })
}

pub fn merge_sorted_streams<T: Ord + Send>(
    mut streams: Vec<impl Stream<Item = anyhow::Result<T>> + Unpin + Send>,
) -> impl Stream<Item = anyhow::Result<T>> + Send {
    try_stream! {
        let mut h: BinaryHeap<(std::cmp::Reverse<T>, usize)> = BinaryHeap::new();
        h.reserve_exact(streams.len());
        let n = streams.len();
        for i in 0..n {
            if let Some(t) = streams[i].next().await.transpose()? {
                h.push((std::cmp::Reverse(t), i));
            }
        }
        while let Some((t, i)) = h.pop() {
            if let Some(t) = streams[i].next().await.transpose()? {
                h.push((std::cmp::Reverse(t), i));
            }
            yield t.0;
        }
    }
}

pub struct OrdEqByFirst<A, B>(pub A, pub B);

impl<A: Eq, B> Eq for OrdEqByFirst<A, B> {}
impl<A: Eq, B> PartialEq for OrdEqByFirst<A, B> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl<A: Ord, B> Ord for OrdEqByFirst<A, B> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}
impl<A: Ord, B> PartialOrd for OrdEqByFirst<A, B> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct Block<'a, R> {
    values_len: usize,
    n_versions: usize,
    min_ts: u64,
    ts_bytes: usize,
    offset_bytes: usize,
    index: PrefixCompressedKV<u16>,
    versions_bytes: Vec<u8>,
    header_offset: u64,
    r: &'a R,
}

const BLOCK_INDEX_HEADER_SIZE: usize = 18;

impl<'a, R> Block<'a, R> {
    // Assumes that kvs values are in reverse order by timestamp.
    //
    // Returns the encoded block and the offset of the header within the block.
    pub fn encode(kvs: &BTreeMap<Vec<u8>, Vec<(u64, Value)>>) -> anyhow::Result<(Vec<u8>, usize)> {
        let (min_ts, max_ts, values_len) = kvs
            .values()
            .map(|versions| {
                let min_ts = versions.last()?.0;
                let max_ts = versions.first()?.0;
                let values_len: usize = versions.iter().map(|(_, v)| v.len()).sum();
                Some((min_ts, max_ts, values_len))
            })
            .flatten()
            .reduce(
                |(min_ts0, max_ts0, values_len0), (min_ts1, max_ts1, values_len1)| {
                    (
                        std::cmp::min(min_ts0, min_ts1),
                        std::cmp::max(max_ts0, max_ts1),
                        values_len0 + values_len1,
                    )
                },
            )
            .ok_or_else(|| anyhow!("malformed block"))?;
        // Shift here for room for the tombstone bit.
        let bytes_per_ts_offset = std::cmp::max(byte_width((max_ts - min_ts) << 1), 1);
        let bytes_per_value_offset = std::cmp::max(byte_width(values_len as u64), 1);

        let mut index: BTreeMap<Vec<u8>, u16> = BTreeMap::new();
        let mut block = vec![];

        let mut n_versions = 0;
        let mut versions = Vec::new();
        for (key, key_versions) in kvs {
            index.insert(key.clone(), n_versions as u16);
            for (ts, value) in key_versions {
                let value_offset = block.len();
                let tombstone_bit = match value {
                    Value::Regular(value) => {
                        block.extend_from_slice(&value[..]);
                        0
                    }
                    Value::Tombstone => 1,
                };
                let ts_offset_and_tombstone = ((ts - min_ts) << 1) | tombstone_bit;

                let mut buf = [0u8; 16];
                LittleEndian::write_u64(&mut buf[..], ts_offset_and_tombstone);
                LittleEndian::write_u64(&mut buf[bytes_per_ts_offset..], value_offset as u64);
                versions.extend_from_slice(&buf[..bytes_per_ts_offset + bytes_per_value_offset]);
            }
            n_versions += key_versions.len();
        }
        let values_len = block.len();
        let encoded_index = PrefixCompressedKV::encode(&index);

        let mut header = [0u8; BLOCK_INDEX_HEADER_SIZE];
        LittleEndian::write_u32(&mut header[0..4], values_len as u32);
        LittleEndian::write_u16(&mut header[4..6], n_versions as u16);
        LittleEndian::write_u64(&mut header[6..14], min_ts);
        LittleEndian::write_u16(&mut header[14..16], encoded_index.len() as u16);
        header[16] = bytes_per_ts_offset as u8;
        header[17] = bytes_per_value_offset as u8;

        let header_idx = block.len();
        block.extend_from_slice(&header[..]);
        block.extend_from_slice(&encoded_index[..]);
        block.extend_from_slice(&versions[..]);

        Ok((block, header_idx))
    }
}

impl<'a, R: AsyncReadExactAt> Block<'a, R> {
    pub async fn open(r: &'a R, header_offset: u64) -> anyhow::Result<Block<'a, R>> {
        let mut header = [0u8; BLOCK_INDEX_HEADER_SIZE];

        r.read_exact_at(&mut header[..], header_offset).await?;

        let values_len = LittleEndian::read_u32(&header[0..4]) as usize;
        let n_versions = LittleEndian::read_u16(&header[4..6]) as usize;
        let min_ts = LittleEndian::read_u64(&header[6..14]);
        let index_len = LittleEndian::read_u16(&header[14..16]) as usize;
        let bytes_per_ts_offset = header[16] as usize;
        let bytes_per_value_offset = header[17] as usize;
        let versions_len = n_versions * (bytes_per_ts_offset + bytes_per_value_offset);

        let mut index_bytes = vec![0u8; index_len];
        r.read_exact_at(
            &mut index_bytes[..],
            header_offset + (BLOCK_INDEX_HEADER_SIZE as u64),
        )
        .await?;
        let index = PrefixCompressedKV::decode(index_bytes);

        let mut versions_bytes = vec![0u8; versions_len];
        r.read_exact_at(
            &mut versions_bytes[..],
            header_offset + (BLOCK_INDEX_HEADER_SIZE as u64) + (index_len as u64),
        )
        .await?;

        Ok(Self {
            r,
            values_len,
            n_versions,
            min_ts,
            index,
            versions_bytes,
            header_offset,
            ts_bytes: bytes_per_ts_offset,
            offset_bytes: bytes_per_value_offset,
        })
    }

    fn versions(&self) -> BlockVersions<'_> {
        BlockVersions {
            ts_bytes: self.ts_bytes,
            offset_bytes: self.offset_bytes,
            min_ts: self.min_ts,
            end_offset: self.values_len,
            b: &self.versions_bytes[..],
        }
    }

    fn versions_for_key(&self, key_idx: usize) -> BlockVersions<'_> {
        let start_idx = self.index.get_value(key_idx) as usize;
        let end_idx = if key_idx == self.index.len() - 1 {
            self.n_versions
        } else {
            self.index.get_value(key_idx + 1) as usize
        };
        self.versions().slice(start_idx, end_idx)
    }

    async fn value<'b>(
        &'b self,
        versions: &BlockVersions<'b>,
        idx: usize,
    ) -> anyhow::Result<Value> {
        let (value_start, value_end) = match versions.value_offsets(idx) {
            Some(v) => v,
            None => return Ok(Value::Tombstone),
        };
        let value_len = value_end - value_start;

        let mut value = vec![0u8; value_len];
        self.r
            .read_exact_at(
                &mut value[..],
                self.header_offset - (self.values_len as u64) + (value_start as u64),
            )
            .await?;

        Ok(Value::Regular(value))
    }

    pub async fn get(&self, ts: u64, k: &[u8]) -> anyhow::Result<Option<(u64, Value)>> {
        let key_idx = match self.index.search(k) {
            Ok(idx) => idx,
            Err(_) => return Ok(None),
        };
        let key_versions = self.versions_for_key(key_idx);

        let version_idx = binary_search_by_idx(key_versions.len(), Reverse(ts), |idx| {
            Reverse(key_versions.ts(idx))
        })
        .unwrap_or_else(core::convert::identity);
        if version_idx == key_versions.len() {
            return Ok(None);
        }
        let record_ts = key_versions.ts(version_idx);

        Ok(Some((
            record_ts,
            self.value(&key_versions, version_idx).await?,
        )))
    }
}

struct BlockVersions<'a> {
    ts_bytes: usize,
    offset_bytes: usize,
    min_ts: u64,
    end_offset: usize,
    b: &'a [u8],
}

impl<'a> BlockVersions<'a> {
    fn len(&self) -> usize {
        self.b.len() / (self.ts_bytes + self.offset_bytes)
    }

    fn elem(&self, idx: usize) -> (u64, bool, usize) {
        let width = self.ts_bytes + self.offset_bytes;
        let elem = &self.b[width * idx..width * (idx + 1)];
        let mut ts_offset_buf = [0u8; 8];
        ts_offset_buf[..self.ts_bytes].copy_from_slice(&elem[..self.ts_bytes]);
        let ts_offset_and_tombstone = LittleEndian::read_u64(&ts_offset_buf[..]);
        let tombstone = ts_offset_and_tombstone & 1 == 1;
        let ts = (ts_offset_and_tombstone >> 1) + self.min_ts;

        let mut value_offset_buf = [0u8; 8];
        value_offset_buf[..self.offset_bytes].copy_from_slice(&elem[self.ts_bytes..]);
        let value_offset = LittleEndian::read_u64(&value_offset_buf[..]) as usize;
        (ts, tombstone, value_offset)
    }

    fn slice(&self, start_idx: usize, end_idx: usize) -> BlockVersions<'a> {
        let width = self.ts_bytes + self.offset_bytes;
        let b = &self.b[start_idx * width..end_idx * width];
        let end_offset = if end_idx == self.len() {
            self.end_offset
        } else {
            self.elem(end_idx).2
        };
        BlockVersions {
            ts_bytes: self.ts_bytes,
            offset_bytes: self.offset_bytes,
            min_ts: self.min_ts,
            end_offset,
            b,
        }
    }

    fn ts(&self, idx: usize) -> u64 {
        self.elem(idx).0
    }

    fn value_offsets(&self, idx: usize) -> Option<(usize, usize)> {
        let (_, tombstone, start) = self.elem(idx);
        if tombstone {
            return None;
        }
        let end = if idx == self.len() - 1 {
            self.end_offset
        } else {
            self.elem(idx + 1).2
        };
        Some((start, end))
    }
}

#[derive(Eq, PartialEq, Clone, Debug)]
struct Record {
    key: Vec<u8>,
    ts: u64,
    value: Value,
}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Record {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.cmp(&other.key) {
            Ordering::Equal => {}
            ord => return ord,
        }
        self.ts.cmp(&other.ts).reverse()
    }
}

#[derive(Eq, PartialEq, Copy, Clone)]
enum Direction {
    Asc,
    Desc,
}

#[derive(Clone)]
struct PrefixCompressedKV<V> {
    v: PhantomData<V>,
    offset_width: usize,
    prefix_len: usize,
    n: usize,
    suffixes_len: usize,
    data: Vec<u8>,
}

const PREFIX_COMPRESSED_KV_HEADER_SIZE: usize = 9;

impl<V: FixedSizeSerializable> PrefixCompressedKV<V> {
    fn encode(m: &BTreeMap<Vec<u8>, V>) -> Vec<u8> {
        let prefix: Vec<u8> = match (m.first_key_value(), m.last_key_value()) {
            (Some((first_key, _)), Some((last_key, _))) => {
                longest_shared_prefix(&first_key[..], &last_key[..])
            }
            _ => vec![],
        };
        let suffixes_len: usize = m.keys().map(|k| k.len() - prefix.len()).sum();

        let offset_width = std::cmp::max(byte_width(suffixes_len as u64), 1);

        let offset_and_value_width = offset_width + V::size();
        let mut suffixes = Vec::with_capacity(suffixes_len);
        let mut offset_and_values = Vec::with_capacity(m.len() * offset_and_value_width);

        let mut offset_and_value = vec![0u8; std::cmp::max(4, offset_and_value_width)];
        for (k, v) in m {
            let offset = suffixes.len();

            suffixes.extend_from_slice(&k[prefix.len()..]);

            for i in 0..offset_and_value.len() {
                offset_and_value[i] = 0;
            }
            LittleEndian::write_u32(&mut offset_and_value[..], offset as u32);
            v.write(&mut offset_and_value[offset_width..]);
            offset_and_values.extend_from_slice(&offset_and_value[..offset_and_value_width]);
        }

        let mut header = [0u8; PREFIX_COMPRESSED_KV_HEADER_SIZE];
        header[0] = offset_width as u8;
        LittleEndian::write_u16(&mut header[1..3], m.len() as u16);
        LittleEndian::write_u16(&mut header[3..5], prefix.len() as u16);
        LittleEndian::write_u32(&mut header[5..9], suffixes.len() as u32);

        let mut out = Vec::with_capacity(
            header.len() + prefix.len() + offset_and_values.len() + suffixes.len(),
        );

        out.extend_from_slice(&header[..]);
        out.extend_from_slice(&prefix[..]);
        out.extend_from_slice(&offset_and_values[..]);
        out.extend_from_slice(&suffixes[..]);

        out
    }

    fn decode(data: Vec<u8>) -> Self {
        let header = &data[0..PREFIX_COMPRESSED_KV_HEADER_SIZE];
        let offset_width = header[0] as usize;
        let n = LittleEndian::read_u16(&header[1..3]) as usize;
        let prefix_len = LittleEndian::read_u16(&header[3..5]) as usize;
        let suffixes_len = LittleEndian::read_u32(&header[5..9]) as usize;

        Self {
            offset_width,
            n,
            prefix_len,
            suffixes_len,
            data,
            v: PhantomData,
        }
    }

    fn len(&self) -> usize {
        self.n
    }

    fn prefix(&self) -> &[u8] {
        &self.data
            [PREFIX_COMPRESSED_KV_HEADER_SIZE..PREFIX_COMPRESSED_KV_HEADER_SIZE + self.prefix_len]
    }

    fn suffixes(&self) -> &[u8] {
        let start = PREFIX_COMPRESSED_KV_HEADER_SIZE
            + self.prefix_len
            + self.n * (self.offset_width + V::size());
        &self.data[start..]
    }

    fn offset_and_values(&self) -> &[u8] {
        let start = PREFIX_COMPRESSED_KV_HEADER_SIZE + self.prefix_len;
        let end = start + self.n * (self.offset_width + V::size());
        &self.data[start..end]
    }

    fn search(&self, k: &[u8]) -> Result<usize, usize> {
        let prefix = self.prefix();
        if !k.starts_with(&prefix) {
            match k.cmp(&prefix) {
                Ordering::Equal => unreachable!(),
                Ordering::Less => return Err(0),
                Ordering::Greater => return Err(self.len()),
            }
        }
        let suffix = &k[prefix.len()..];
        binary_search_by_idx(self.len(), suffix, |idx| self.get_suffix(idx))
    }

    fn offset(&self, idx: usize) -> usize {
        let width = self.offset_width + V::size();
        let offset_start = idx * width;
        let offset_end = offset_start + self.offset_width;
        let mut offset: u32 = 0;
        for (i, b) in self.offset_and_values()[offset_start..offset_end]
            .iter()
            .enumerate()
        {
            offset |= (*b as u32) << (i * 8);
        }
        offset as usize
    }

    fn get_suffix(&self, idx: usize) -> &[u8] {
        let start = self.offset(idx);
        let end = if idx == self.len() - 1 {
            self.suffixes_len
        } else {
            self.offset(idx + 1)
        };
        &self.suffixes()[start..end]
    }

    fn get_key(&self, idx: usize) -> Vec<u8> {
        let prefix = self.prefix();
        let suffix = self.get_suffix(idx);
        let mut k = Vec::with_capacity(prefix.len() + suffix.len());
        k.extend_from_slice(prefix);
        k.extend_from_slice(suffix);
        k
    }

    fn get_value(&self, idx: usize) -> V {
        let width = self.offset_width + V::size();
        let offset_start = idx * width + self.offset_width;
        let offset_end = offset_start + V::size();
        V::read(&self.offset_and_values()[offset_start..offset_end])
    }
}

trait FixedSizeSerializable {
    fn size() -> usize;
    fn read(b: &[u8]) -> Self;
    fn write(&self, b: &mut [u8]);
}

impl FixedSizeSerializable for u16 {
    fn size() -> usize {
        2
    }
    fn read(b: &[u8]) -> Self {
        LittleEndian::read_u16(b)
    }
    fn write(&self, b: &mut [u8]) {
        LittleEndian::write_u16(b, *self);
    }
}

impl FixedSizeSerializable for u32 {
    fn size() -> usize {
        4
    }
    fn read(b: &[u8]) -> Self {
        LittleEndian::read_u32(b)
    }
    fn write(&self, b: &mut [u8]) {
        LittleEndian::write_u32(b, *self);
    }
}

struct LittleEndianU32(u32);

impl From<&[u8]> for LittleEndianU32 {
    fn from(b: &[u8]) -> Self {
        LittleEndianU32(LittleEndian::read_u32(b))
    }
}

#[derive(Clone)]
struct Run<R> {
    r: R,

    id: Uuid,
    size: usize,
    keyspace_id: u32,
    min_ts: u64,
    max_ts: u64,

    index: PrefixCompressedKV<u32>,

    min_key: Vec<u8>,
    max_key: Vec<u8>,
}

const INDEX_BLOCK_HEADER_SIZE: usize = 44;

impl<R> Run<R> {
    // Assumes S is in (key, rev(ts)) order, and assumes termination at a reasonable size limit.
    async fn write<W: AsyncWrite + Unpin, S: Stream<Item = anyhow::Result<Record>>>(
        w: &mut W,
        keyspace_id: u32,
        block_size_limit: u64,
        s: S,
    ) -> anyhow::Result<()> {
        async fn flush<W: AsyncWrite + Unpin>(
            w: &mut W,
            bytes_written: &mut usize,
            index: &mut BTreeMap<Vec<u8>, u32>,
            last_key: &mut Vec<u8>,
            buffer: &BTreeMap<Vec<u8>, Vec<(u64, Value)>>,
        ) -> anyhow::Result<()> {
            let (first_key, last_key_) = match (buffer.first_key_value(), buffer.last_key_value()) {
                (Some((first_key, _)), Some((last_key, _))) => (first_key, last_key),
                _ => anyhow::bail!("empty block"),
            };
            *last_key = last_key_.clone();

            let (block, header_offset_in_block) = Block::<()>::encode(buffer)?;
            w.write_all(&block[..]).await?;
            let header_offset_in_file = *bytes_written + header_offset_in_block;

            index.insert(first_key.clone(), header_offset_in_file as u32);

            *bytes_written += block.len();

            Ok(())
        }

        pin_mut!(s);

        let mut buffer: BTreeMap<Vec<u8>, Vec<(u64, Value)>> = BTreeMap::new();
        let mut bytes_written = 0;
        let mut buffer_size = BLOCK_INDEX_HEADER_SIZE as u64;
        let mut index: BTreeMap<Vec<u8>, u32> = BTreeMap::new();
        let mut min_ts = u64::MAX;
        let mut max_ts = 0;
        let mut last_key = vec![];
        while let Some(record) = s.next().await.transpose()? {
            let record_size = {
                let key_len = if buffer.contains_key(&record.key) {
                    0
                } else {
                    (record.key.len() as u64) + 4
                };
                (key_len as u64) + 10 + (record.value.len() as u64)
            };

            if !buffer.is_empty()
                && buffer_size + record_size > block_size_limit
                && !buffer.contains_key(&record.key)
            {
                flush(w, &mut bytes_written, &mut index, &mut last_key, &buffer).await?;
                buffer.clear();
                buffer_size = 0;
            }

            if let Some(prev_record) = buffer
                .get(&record.key)
                .map(|versions| versions.last())
                .flatten()
            {
                assert!(prev_record.0 > record.ts);
            }
            buffer
                .entry(record.key)
                .or_insert_with(Vec::new)
                .push((record.ts, record.value));
            buffer_size += record_size;

            min_ts = std::cmp::min(min_ts, record.ts);
            max_ts = std::cmp::max(max_ts, record.ts);
        }
        if !buffer.is_empty() {
            flush(w, &mut bytes_written, &mut index, &mut last_key, &buffer).await?;
        }

        let index_compressed = PrefixCompressedKV::encode(&index);

        let index_block_offset = bytes_written;
        let mut header = [0u8; INDEX_BLOCK_HEADER_SIZE];
        let id = Uuid::new_v4();
        header[0..16].copy_from_slice(&id.as_bytes()[..]);
        LittleEndian::write_u32(&mut header[16..20], keyspace_id);
        LittleEndian::write_u64(&mut header[20..28], min_ts);
        LittleEndian::write_u64(&mut header[28..36], max_ts);
        LittleEndian::write_u32(&mut header[36..40], last_key.len() as u32);
        LittleEndian::write_u32(&mut header[40..44], index_compressed.len() as u32);
        w.write_all(&header[..]).await?;
        w.write_all(&last_key[..]).await?;
        w.write_all(&index_compressed).await?;

        let mut index_block_offset_buf = [0u8; 4];
        LittleEndian::write_u32(&mut index_block_offset_buf[..], index_block_offset as u32);
        w.write_all(&index_block_offset_buf[..]).await?;

        Ok(())
    }
}

impl<R: AsyncReadExactAt> Run<R> {
    async fn open(r: R, size: usize) -> anyhow::Result<Self> {
        let file_len = r.len().await?;
        let mut index_block_offset_buf = [0u8; 4];
        r.read_exact_at(&mut index_block_offset_buf[..], file_len - 4)
            .await?;
        let index_block_offset = LittleEndian::read_u32(&index_block_offset_buf[..]);

        let mut header = [0u8; INDEX_BLOCK_HEADER_SIZE];
        r.read_exact_at(&mut header[..], index_block_offset as u64)
            .await?;

        let id = {
            let mut uuid_bytes = [0u8; 16];
            uuid_bytes.copy_from_slice(&header[0..16]);
            Uuid::from_bytes(uuid_bytes)
        };
        let keyspace_id = LittleEndian::read_u32(&header[16..20]);
        let min_ts = LittleEndian::read_u64(&header[20..28]);
        let max_ts = LittleEndian::read_u64(&header[28..36]);
        let max_key_len = LittleEndian::read_u32(&header[36..40]);
        let index_len = LittleEndian::read_u32(&header[40..44]);

        let max_key = {
            let mut max_key = vec![0u8; max_key_len as usize];
            r.read_exact_at(
                &mut max_key[..],
                (index_block_offset as u64) + (header.len() as u64),
            )
            .await?;
            max_key
        };

        let index = {
            let mut index_bytes = vec![0u8; index_len as usize];
            r.read_exact_at(
                &mut index_bytes[..],
                (index_block_offset as u64) + (header.len() as u64) + (max_key_len as u64),
            )
            .await?;
            PrefixCompressedKV::decode(index_bytes)
        };

        let min_key = index.get_key(0);

        Ok(Self {
            r,

            id,
            size,
            keyspace_id,
            min_ts,
            max_ts,
            index,

            min_key,
            max_key,
        })
    }

    fn id(&self) -> Uuid {
        self.id
    }

    fn size(&self) -> usize {
        self.size
    }

    async fn get(&self, ts: u64, k: &[u8]) -> anyhow::Result<Option<(u64, Value)>> {
        if ts < self.min_ts {
            return Ok(None);
        }
        let block_header_idx = match self.index.search(k) {
            Ok(idx) => idx,
            Err(idx) => {
                if idx == 0 {
                    return Ok(None);
                }
                idx - 1
            }
        };
        let block_header_offset = self.index.get_value(block_header_idx);
        let block = Block::open(&self.r, block_header_offset as u64).await?;
        block.get(ts, k).await
    }

    fn scan(
        &self,
        ts: u64,
        range: Range<Vec<u8>>,
        direction: Direction,
    ) -> impl Stream<Item = anyhow::Result<Record>> + '_ {
        try_stream! {
            if direction == Direction::Desc {
                todo!();
            }

            if ts < self.min_ts {
                return;
            }

            let lower_block_idx = binary_search_by_idx(
                self.index.len(),
                KeyOrBound::Bound(range.lower.clone()),
                |idx| KeyOrBound::Key(self.index.get_key(idx)),
            )
            .unwrap_or_else(|idx| if idx != 0 { idx - 1 } else { idx });

            'outer: for i in lower_block_idx..self.index.len() {
                let block_header_offset = self.index.get_value(i);
                let block = Block::open(&self.r, block_header_offset as u64).await?;
                let lower_key_idx = if i == lower_block_idx {
                    binary_search_by_idx(
                        block.index.len(),
                        KeyOrBound::Bound(range.lower.clone()),
                        |idx| KeyOrBound::Key(block.index.get_key(idx)),
                    )
                    .unwrap_or_else(core::convert::identity)
                } else {
                    0
                };

                for j in lower_key_idx..block.index.len() {
                    let key = block.index.get_key(j);
                    if !range.contains(&key) {
                        break 'outer;
                    }

                    let versions = block.versions_for_key(j);
                    let version_idx = binary_search_by_idx(versions.len(), Reverse(ts), |idx| {
                        Reverse(versions.ts(idx))
                    })
                    .unwrap_or_else(core::convert::identity);
                    if version_idx == versions.len() {
                        continue;
                    }

                    let ts = versions.ts(version_idx);
                    let value = block.value(&versions, version_idx).await?;
                    if let Value::Tombstone = value {
                        continue;
                    }

                    yield Record { key, ts, value };
                }
            }
        }
    }

    fn range(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        Some((self.min_key.clone(), self.max_key.clone()))
    }

    fn stream(&self) -> impl Stream<Item = anyhow::Result<Record>> + '_ {
        try_stream! {
            for i in 0..self.index.len() {
                let block_header_offset = self.index.get_value(i);
                let block = Block::open(&self.r, block_header_offset as u64).await?;
                for j in 0..block.index.len() {
                    let key = block.index.get_key(j);
                    let versions = block.versions_for_key(j);
                    for k in 0..versions.len() {
                        let ts = versions.ts(k);
                        let value = block.value(&versions, k).await?;
                        yield Record{key: key.clone(), ts, value};
                    }
                }
            }
        }
    }

    fn into_stream(self) -> impl Stream<Item = anyhow::Result<Record>> {
        stream! {
            let mut s = self.stream().boxed_local();
            while let Some(x) = s.next().await {
                yield x;
            }
        }
    }
}

#[async_trait]
trait AsyncReadExactAt {
    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<()>;
    async fn len(&self) -> anyhow::Result<u64>;
}

#[async_trait]
impl AsyncReadExactAt for Vec<u8> {
    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<()> {
        Ok(buf.copy_from_slice(&self[(offset as usize)..(offset as usize) + buf.len()]))
    }
    async fn len(&self) -> anyhow::Result<u64> {
        Ok(self.len() as u64)
    }
}

#[async_trait]
impl AsyncReadExactAt for File {
    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<()> {
        // TODO: This requires an extra allocation because spawn_blocking can't hold onto a mut ref
        // to buf because compiler isn't smart enough to know that we immediately await it and that
        // awaiting it implies that the function is done running.
        //
        // Static-sized reads are not the common case here it seems, so it might be worth just
        // changing this function to take a length and always do the allocation internally, or
        // figure out how tokio implements AsyncRead::read_exact() when poll_read() requires a
        // spawn_blocking.
        let mut inner_buf = vec![0u8; buf.len()];
        // We can safely clone this because the file descriptor's state is not affected by
        // read_exact_at.
        let other = self.try_clone()?;
        let mut inner_buf = tokio::task::spawn_blocking(move || {
            FileExt::read_exact_at(&other, &mut inner_buf, offset)?;
            Ok::<Vec<u8>, anyhow::Error>(inner_buf)
        })
        .await??;
        buf.copy_from_slice(&mut inner_buf);
        Ok(())
    }
    async fn len(&self) -> anyhow::Result<u64> {
        todo!()
    }
}

fn binary_search_by_idx<K: Ord, F: Fn(usize) -> K>(n: usize, k: K, f: F) -> Result<usize, usize> {
    let mut lower = 0;
    let mut upper = n;
    while lower < upper {
        let mid = (lower + upper) / 2;
        let at_mid = f(mid);
        match k.cmp(&at_mid) {
            Ordering::Equal => return Ok(mid),
            Ordering::Less => upper = mid,
            Ordering::Greater => lower = mid + 1,
        }
    }
    Err(lower)
}

fn longest_shared_prefix(a: &[u8], b: &[u8]) -> Vec<u8> {
    std::iter::zip(a.iter(), b.iter())
        .take_while(|(a, b)| *a == *b)
        .map(|(a, _)| *a)
        .collect()
}

// Returns the number of bytes needed to represent x.
fn byte_width(x: u64) -> usize {
    let bits_needed = 64 - x.leading_zeros();
    ((bits_needed + 7) / 8) as usize
}

struct AtomicArc<T> {
    // TODO: figure out how to do this with actual atomic instructions
    lock: RwLock<Arc<T>>,
}

impl<T> AtomicArc<T> {
    fn new(t: Arc<T>) -> Self {
        Self {
            lock: RwLock::new(t),
        }
    }

    fn load(&self) -> Arc<T> {
        self.lock.read().unwrap().clone()
    }

    fn store(&self, t: Arc<T>) {
        let mut guard = self.lock.write().unwrap();
        *guard = t;
    }

    fn compare_and_swap(&self, prev: &Arc<T>, next: Arc<T>) -> bool {
        let mut guard = self.lock.write().unwrap();
        if Arc::ptr_eq(prev, &*guard) {
            *guard = next;
            return true;
        }
        false
    }
}

#[cfg(test)]
mod test {
    use std::cmp::Reverse;
    use std::collections::BTreeMap;
    use std::task::Poll;

    use byteorder::BigEndian;
    use byteorder::ByteOrder;
    use futures::poll;
    use futures::stream::StreamExt;
    use futures::FutureExt;
    use proptest::prelude::*;

    use crate::binary_search_by_idx;
    use crate::hexlify;
    use crate::range::Bound;
    use crate::range::Range;
    use crate::AsyncReadExactAt;
    use crate::Block;
    use crate::Direction;
    use crate::Lsm;
    use crate::LsmBuilder;
    use crate::Record;
    use crate::Run;
    use crate::Value;

    #[tokio::test]
    async fn test_put_get() -> anyhow::Result<()> {
        let lsm = LsmBuilder::new().build();
        let k = b"abc";
        let not_k = b"def";
        let v = b"foo";
        let write_ts = lsm.put(k.to_vec(), v.to_vec()).await?;
        assert_eq!(lsm.get(write_ts - 1, k).await?, None);
        assert_eq!(lsm.get(write_ts, k).await?, Some(v.to_vec()));
        assert_eq!(lsm.get(write_ts + 1, k).await?, Some(v.to_vec()));
        assert_eq!(lsm.get(write_ts - 1, not_k).await?, None);
        assert_eq!(lsm.get(write_ts, not_k).await?, None);
        assert_eq!(lsm.get(write_ts + 1, not_k).await?, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_compact_l0() -> anyhow::Result<()> {
        let lsm = LsmBuilder::new()
            .l0_max_size(128)
            .block_size(128)
            .run_target_size(512)
            .build();
        let mut map = BTreeMap::new();
        let mut last_ts = 0;
        //let mut runs_in_l1 = 0;
        for _ in 0..10 {
            let compaction = lsm.next_compaction().boxed_local();
            for i in 0..13 {
                let v = (i % 179) as u8;
                let put_ts = lsm.put(vec![i as u8], vec![v]).await?;
                last_ts = std::cmp::max(put_ts, last_ts);
                map.insert(i as u8, v);

                // Insert until we trigger a compaction.
                //if lsm.levels[1].runs.len() != runs_in_l1 {
                //    runs_in_l1 = lsm.levels[1].runs.len();
                //    break;
                //}
            }
            compaction.await;

            for (k, v) in &map {
                assert_eq!(lsm.get(last_ts, &[*k]).await?, Some(vec![*v]));
            }
        }

        // Make sure we actually did ever do a compaction.
        assert!(lsm.inner.manifest.load().levels[1].runs.len() >= 1);

        Ok(())
    }

    // #[tokio::test]
    // async fn test_compact_l1() -> anyhow::Result<()> {
    //     let mut lsm = LsmBuilder::new()
    //         .l0_max_size(64)
    //         .run_target_size(1024)
    //         .block_size(256)
    //         .build();
    //     let mut map = BTreeMap::new();
    //     let mut last_ts = 0;
    //     let mut runs_in_l2 = 0;
    //     for _ in 0..3 {
    //         for i in 0..usize::MAX {
    //             let v = (i % 179) as u8;
    //             let put_ts = lsm.put(vec![i as u8], vec![v]).await?;
    //             last_ts = std::cmp::max(put_ts, last_ts);
    //             map.insert(i as u8, v);

    //             // Insert until we trigger a compaction.
    //             if lsm.levels[2].runs.len() != runs_in_l2 {
    //                 runs_in_l2 = lsm.levels[2].runs.len();
    //                 break;
    //             }
    //         }

    //         for (k, v) in &map {
    //             assert_eq!(lsm.get(last_ts, &[*k]).await?, Some(vec![*v]));
    //         }
    //     }

    //     Ok(())
    // }

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
    async fn test_block() -> anyhow::Result<()> {
        let aa: Vec<u8> = "aa".into();
        let ab: Vec<u8> = "ab".into();
        let aa_279: Value = Value::Regular("foo".into());
        let aa_265: Value = Value::Regular("bar".into());
        let ab_341: Value = Value::Regular("baz".into());
        let ab_302: Value = Value::Regular("qux".into());
        let ab_290: Value = Value::Regular("garply".into());
        let kvs = {
            let mut kvs = BTreeMap::new();
            kvs.insert(
                aa.clone(),
                vec![(279, aa_279.clone()), (265, aa_265.clone())],
            );
            kvs.insert(
                ab.clone(),
                vec![
                    (341, ab_341.clone()),
                    (302, ab_302.clone()),
                    (297, Value::Tombstone),
                    (290, ab_290.clone()),
                ],
            );
            kvs
        };
        let (encoded, header_offset) = Block::<()>::encode(&kvs)?;

        let block = Block::open(&encoded, header_offset as u64).await?;

        assert_eq!(block.get(279, &aa[..]).await?, Some((279, aa_279.clone())));
        assert_eq!(block.get(265, &aa[..]).await?, Some((265, aa_265.clone())));
        assert_eq!(block.get(123, &aa[..]).await?, None);

        assert_eq!(block.get(295, &aa[..]).await?, Some((279, aa_279.clone())));
        assert_eq!(block.get(269, &aa[..]).await?, Some((265, aa_265.clone())));

        assert_eq!(block.get(341, &ab[..]).await?, Some((341, ab_341.clone())));
        assert_eq!(block.get(302, &ab[..]).await?, Some((302, ab_302.clone())));
        assert_eq!(
            block.get(297, &ab[..]).await?,
            Some((297, Value::Tombstone))
        );
        assert_eq!(block.get(290, &ab[..]).await?, Some((290, ab_290.clone())));
        assert_eq!(block.get(289, &ab[..]).await?, None);

        assert_eq!(block.get(500, &ab[..]).await?, Some((341, ab_341.clone())));
        assert_eq!(block.get(340, &ab[..]).await?, Some((302, ab_302.clone())));
        assert_eq!(
            block.get(300, &ab[..]).await?,
            Some((297, Value::Tombstone))
        );
        assert_eq!(block.get(296, &ab[..]).await?, Some((290, ab_290.clone())));

        Ok(())
    }

    #[tokio::test]
    async fn test_run_file() -> anyhow::Result<()> {
        fn rand_bytes(n: usize) -> Vec<u8> {
            let mut out = vec![0u8; n];
            rand::thread_rng().fill_bytes(&mut out);
            out
        }
        let records = vec![
            Record {
                key: b"prefixbar".to_vec(),
                ts: 20101,
                value: Value::Regular(rand_bytes(10_000)),
            },
            Record {
                key: b"prefixbar".to_vec(),
                ts: 19230,
                value: Value::Tombstone,
            },
            Record {
                key: b"prefixbar".to_vec(),
                ts: 10230,
                value: Value::Regular(rand_bytes(128)),
            },
            Record {
                key: b"prefixfoo".to_vec(),
                ts: 21925,
                value: Value::Regular(rand_bytes(10_000)),
            },
            Record {
                key: b"prefixfoo".to_vec(),
                ts: 12031,
                value: Value::Regular(rand_bytes(10_000)),
            },
        ];
        let mut v = vec![];
        Run::<()>::write(
            &mut v,
            1,
            32768,
            futures::stream::iter(records.iter().map(|record| Ok(record.clone()))),
        )
        .await
        .unwrap();

        let v_len = v.len();
        let run = Run::open(v, v_len).await?;

        assert_eq!(run.min_ts, 10230);
        assert_eq!(run.max_ts, 21925);
        assert_eq!(run.min_key, b"prefixbar".to_vec());
        assert_eq!(run.max_key, b"prefixfoo".to_vec());

        for record in records {
            assert_eq!(
                run.get(record.ts, &record.key).await?,
                Some((record.ts, record.value)),
            );
        }

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
        fn proptest_run_file(m in proptest::collection::btree_map(
            (proptest::collection::vec(u8::arbitrary(), 0..2), 0..(1u64 << 63)),
            proptest::option::of(proptest::collection::vec(u8::arbitrary(), 0..128)),
            1..4096,
        )) {
            let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();

            rt.block_on(async {
                let mut records = m.into_iter().map(|((key, ts), maybe_value)| Record{
                    key, ts, value: match maybe_value {
                        Some(v) => Value::Regular(v),
                        None => Value::Tombstone,
                    },
                }).collect::<Vec<Record>>();
                records.sort_by_key(|record| (record.key.clone(), Reverse(record.ts)));

                let mut v = vec![];
                Run::<()>::write(
                    &mut v,
                    1,
                    1024,
                    futures::stream::iter(records.iter().map(|record| Ok(record.clone()))),
                ).await.unwrap();

                let v_len = v.len();
                let run = Run::open(v, v_len).await.unwrap();

                dump_run_file(&run).await.unwrap();

                for record in &records {
                    assert_eq!(
                        run.get(record.ts, &record.key[..]).await.unwrap(),
                        Some((record.ts, record.value.clone())),
                    );
                }

                let streamed_out_records = run
                    .stream()
                    .collect::<Vec<anyhow::Result<Record>>>()
                    .await
                    .into_iter()
                    .collect::<anyhow::Result<Vec<Record>>>()
                    .unwrap();

                assert_eq!(streamed_out_records, records);
            });
        }

        #[test]
        fn proptest_lsm_scan(
            keys in proptest::collection::btree_set(
                proptest::collection::vec(u8::arbitrary(), 0..16),
                1..100,
            ),
            write_indexes in proptest::collection::vec(any::<prop::sample::Index>(), 1..4096),
            log_indexes in proptest::collection::vec(any::<prop::sample::Index>(), 1000),
            ranges in proptest::collection::vec(range_strategy(), 1000),
        ) {
            tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
                let keys_vec: Vec<_> = keys.iter().collect();

                let mut writes = vec![];

                let mut lsm = LsmBuilder::new()
                    .l0_max_size(128)
                    .block_size(128)
                    .run_target_size(512)
                    .build();
                for (i, index) in write_indexes.iter().enumerate() {
                    let key = keys_vec[index.index(keys_vec.len())];
                    let mut value = vec![0; 16];
                    BigEndian::write_u64(&mut value[8..], i as u64);
                    let ts = lsm.put(key.clone(), value.clone()).await.unwrap();
                    writes.push((key.clone(), ts, value.clone()));
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
                        let (mut page, continue_cursor) = lsm.scan_page(ts, cursor.borrow(), Direction::Asc, 100).await.unwrap();
                        results.append(&mut page);
                        maybe_cursor = continue_cursor;
                    }

                    let expected_recs: Vec<Record> = expected.into_iter().map(|(key, (ts, value))| {
                        Record{key: key.clone(), ts: *ts, value: Value::Regular(value.clone())}
                    }).collect();

                    assert_eq!(results, expected_recs);
                }
            });
        }
    }

    async fn dump_run_file<R: AsyncReadExactAt>(run: &Run<R>) -> anyhow::Result<()> {
        println!("    min_ts: {}", run.min_ts);
        println!("    max_ts: {}", run.max_ts);
        println!("    index");
        println!("    prefix: [{}]", hexlify(run.index.prefix()));
        for i in 0..run.index.len() {
            println!(
                "      {} header offset {}",
                hexlify(&run.index.get_key(i)),
                run.index.get_value(i)
            );
        }
        println!("    blocks");
        for i in 0..run.index.len() {
            println!("    == block {} ======", i);
            println!("    first key: [{}]", hexlify(&run.index.get_key(i)),);
            println!("    header_offset: {}", run.index.get_value(i));
            let header_offset = run.index.get_value(i);
            let block = Block::open(&run.r, header_offset as u64).await?;
            dump_block(&block).await?;
        }
        Ok(())
    }
    async fn dump_block<'a, R: AsyncReadExactAt>(block: &Block<'a, R>) -> anyhow::Result<()> {
        println!("    prefix: {}", hexlify(block.index.prefix()));
        println!("    n_keys: {}", block.index.len());
        println!("    n_versions: {}", block.n_versions);
        println!("    values_len: {}", block.values_len);
        println!("      == keys ======");
        for i in 0..block.index.len() {
            let key = block.index.get_key(i);
            let versions_offset = block.index.get_value(i);
            println!("        [{}] {}", hexlify(&key), versions_offset);
        }
        let versions = block.versions();
        println!("      == versions ======");
        for i in 0..block.n_versions {
            let value_str = match versions.value_offsets(i) {
                Some((value_start, value_end)) => format!("({}, {})", value_start, value_end),
                None => "<TOMBSTONE>".into(),
            };
            println!("        {} {} {}", i, versions.ts(i), value_str);
        }
        Ok(())
    }
}

fn dump_manifest(manifest: &Manifest) {
    println!("== manifest =====");
    println!("l0_active");
    for memtable_lock in manifest.l0_active.values() {
        match memtable_lock.read().unwrap().range() {
            Some((lower, upper)) => {
                println!("  [{}] [{}]", hexlify(&lower), hexlify(&upper));
            }
            None => println!("  (empty)"),
        }
    }
    println!("l0_sealed");
    for memtable in manifest.l0_sealed.values() {
        match memtable.range() {
            Some((lower, upper)) => {
                println!("  [{}] [{}]", hexlify(&lower), hexlify(&upper));
            }
            None => println!("  (empty)"),
        }
    }
    for (i, level) in manifest.levels[1..]
        .iter()
        .enumerate()
        .map(|(i, level)| (i + 1, level))
    {
        println!("l{}", i);
        for run in &level.runs {
            let (lower, upper) = run.range().unwrap();
            println!("  run [{}] [{}]", hexlify(&lower), hexlify(&upper));
        }
    }
    println!("============");
}
fn record_string(r: &Record) -> String {
    format!(
        "[{}] @ {}: {}",
        hexlify(&r.key),
        r.ts,
        value_string(&r.value)
    )
}
fn value_string(v: &Value) -> String {
    match v {
        Value::Regular(v) => format!("[{}]", hexlify(v)),
        Value::Tombstone => "<TOMBSTONE>".into(),
    }
}
fn bound_string(b: &Bound<Vec<u8>>) -> String {
    match b {
        Bound::BeforeAll => "before_all".into(),
        Bound::Before(v) => format!("before({})", hexlify(v)),
        Bound::After(v) => format!("after({})", hexlify(v)),
        Bound::AfterPrefix(v) => format!("after_prefix({})", hexlify(v)),
        Bound::AfterAll => "after_all".into(),
    }
}
fn range_string(r: &Range<Vec<u8>>) -> String {
    format!("({}, {})", bound_string(&r.lower), bound_string(&r.upper))
}
fn hexlify(b: &[u8]) -> String {
    b.iter().map(|b| format!("{:02x}", b)).collect()
}
