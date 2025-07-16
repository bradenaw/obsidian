use std::cmp;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use futures::FutureExt;
use futures::Stream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::meta::Meta;
use crate::meta::MetaKey;
use crate::meta::MetaReader;
use crate::obsidian::Router;
use crate::obsidian::TabletId;
use crate::range::Bound;
use crate::range::Range;
use crate::router::StaticRouter;
use crate::types::ColoGroupId;
use crate::types::Direction;
use crate::types::RevisionValue;
use crate::types::Timestamp;
use crate::util::hexlify;
use crate::util::wait_all;
use crate::util::Background;
use crate::util::Retry;
use crate::util::WaitableTimestamp;

pub(crate) struct MetaSynced {
    bg: Background,
    inner: Arc<RwLock<MetaSyncedInner>>,
}

#[derive(Clone)]
pub(crate) enum SyncType {
    Initial,
    Tx(HashSet<MetaKey>),
}

struct MetaSyncedInner {
    synced_ts: Arc<WaitableTimestamp>,
    kv: MetaSyncedSnapshot,
    router: StaticRouter,
    owned_ranges: HashMap<TabletId, HashMap<ColoGroupId, Range<Vec<u8>>>>,

    // For every change and the initial load, we'll send the update on each of these channels and
    // expect the other side to use the oneshot to acknowledge that it has completed or been
    // abandoned.
    subscribers: Vec<mpsc::Sender<(SyncType, MetaSyncedSnapshot, oneshot::Sender<()>)>>,
}

impl MetaSynced {
    pub(crate) fn new<M: Meta + Sync + Send + 'static>(m: M) -> Self {
        let bg = Background::new();

        let inner = Arc::new(RwLock::new(MetaSyncedInner {
            synced_ts: Arc::new(WaitableTimestamp::new()),
            kv: MetaSyncedSnapshot::new(),
            router: StaticRouter::new(HashMap::new()),
            owned_ranges: HashMap::new(),
            subscribers: vec![],
        }));

        bg.spawn({
            let inner = inner.clone();
            async move { MetaSyncedInner::sync(inner, m).await }
        });

        Self { bg, inner }
    }

    pub(crate) fn range_for_tablet(
        &self,
        tablet_id: TabletId,
        colo_group_id: ColoGroupId,
    ) -> Range<Vec<u8>> {
        if colo_group_id == ColoGroupId::META && tablet_id == TabletId::META {
            return Range::all();
        }
        if colo_group_id == ColoGroupId::SHARD_META {
            return TabletId::shard_meta_owned_range(tablet_id.0);
        }
        let inner = self.inner.read().unwrap();
        if let Some(range_set_by_colo_group_id) = inner.owned_ranges.get(&tablet_id) {
            if let Some(range_set) = range_set_by_colo_group_id.get(&colo_group_id) {
                return range_set.clone();
            }
        }
        Range::empty()
    }

    /// Subscribes to changes in `MetaSynced`. `f` will be called once, either immediately or when
    /// initial sync finishes, with `SyncType::Initial`. Every transaction that updates the
    /// `MetaSynced` after that point will be given as a `SyncType::Tx` with the changed keys.
    ///
    /// The synced timestamp (as observed by `wait()`) does not advance until all subscribers
    /// return, so that `wait()` also describes the log position that those subscribers are at.
    ///
    /// That also means it would be unwise to do anything terribly expensive inside f.
    pub(crate) async fn subscribe<F, Fut>(&self, f: F)
    where
        F: Fn(SyncType, MetaSyncedSnapshot) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let (maybe_initial, mut rx) = {
            let mut inner = self.inner.write().unwrap();
            let (tx, rx) = mpsc::channel(1);
            inner.subscribers.push(tx);

            if inner.synced_ts.get() > Timestamp::ZERO {
                (Some(inner.kv.clone()), rx)
            } else {
                (None, rx)
            }
        };

        if let Some(initial) = maybe_initial {
            f(SyncType::Initial, initial).await;
        }

        self.bg.spawn(async move {
            while let Some((sync_type, snapshot, done)) = rx.recv().await {
                f(sync_type, snapshot).await;
                // This only errors if the other side hung up, which means they're gone and we don't
                // care about them for the purposes of synced_ts.
                let _ = done.send(());
            }
        });
    }

    pub(crate) fn snapshot(&self) -> MetaSyncedSnapshot {
        let inner = self.inner.read().unwrap();
        inner.kv.clone()
    }

    pub(crate) async fn wait(&self, ts: Timestamp) -> anyhow::Result<()> {
        let synced_ts = {
            let inner = self.inner.read().unwrap();
            inner.synced_ts.clone()
        };
        synced_ts.wait(ts).await?;
        Ok(())
    }
}

impl Router for MetaSynced {
    fn tablet_id_for_key(
        &self,
        colo_group_id: ColoGroupId,
        key: &[u8],
    ) -> anyhow::Result<TabletId> {
        let inner = self.inner.read().unwrap();
        return inner.router.tablet_id_for_key(colo_group_id, key);
    }

    fn tablet_id_for_bound(
        &self,
        colo_group_id: ColoGroupId,
        bound: Bound<&[u8]>,
        direction: Direction,
    ) -> anyhow::Result<TabletId> {
        let inner = self.inner.read().unwrap();
        return inner
            .router
            .tablet_id_for_bound(colo_group_id, bound, direction);
    }
}

impl MetaSyncedInner {
    async fn sync<M: Meta>(inner_lock: Arc<RwLock<Self>>, meta: M) {
        let mut ts = Retry::new()
            .indefinitely(&async || -> anyhow::Result<Timestamp> {
                let ts = Self::initial_sync_once(&inner_lock, &meta).await?;
                Ok(ts)
            })
            .await;

        loop {
            ts = Retry::new()
                .indefinitely(&async || -> anyhow::Result<Timestamp> {
                    let new_ts = Self::incremental_sync_once(&inner_lock, &meta, ts).await?;
                    Ok(new_ts)
                })
                .await;
        }
    }

    async fn initial_sync_once<M: Meta>(
        inner_lock: &Arc<RwLock<Self>>,
        meta: &M,
    ) -> anyhow::Result<Timestamp> {
        let ts = Retry::new()
            .indefinitely(&|| async {
                let ts = meta.latest_snapshot().await?;
                Ok::<_, anyhow::Error>(ts)
            })
            .await;

        let mut kv = MetaSyncedSnapshot::new();

        let mut maybe_cursor = Some(Range::all());
        while let Some(cursor) = maybe_cursor {
            let (page, continue_cursor) = Retry::new()
                .indefinitely(&|| {
                    let cursor = cursor.clone();
                    async {
                        let out = meta.scan_page(ts, cursor).await?;
                        Ok::<_, anyhow::Error>(out)
                    }
                })
                .await;

            for record in page {
                kv.insert(record.key.1, record.value);
            }

            maybe_cursor = continue_cursor;
        }

        let (router, owned_ranges) = Self::regen_router(kv.clone()).await?;

        let snapshot = {
            let mut inner = inner_lock.write().unwrap();
            inner.kv = kv.clone();
            inner.router = router;
            inner.owned_ranges = owned_ranges;
            inner.kv.clone()
        };

        Self::notify_and_wait_subscribers(inner_lock, SyncType::Initial, snapshot).await;

        Ok(ts)
    }

    async fn notify_and_wait_subscribers(
        inner_lock: &Arc<RwLock<Self>>,
        sync_type: SyncType,
        snapshot: MetaSyncedSnapshot,
    ) {
        // This looks a little odd but:
        // a) this is only ever called from a single task
        // b) we can't hold any lock on inner while waiting the futures
        //
        // But we want to mutate inner.subscribers during in order to remove subscribers that have
        // already hung up.

        let mut subscribers = {
            let mut inner = inner_lock.write().unwrap();
            inner.subscribers.split_off(0)
        };

        let mut futures = vec![];
        let mut i = 0;
        while i < subscribers.len() {
            let (tx, rx) = oneshot::channel();
            if let Err(_) = subscribers[i]
                .send((sync_type.clone(), snapshot.clone(), tx))
                .await
            {
                subscribers.swap_remove(i);
                continue;
            }
            futures.push(rx.map(|_| ()));
            i += 1;
        }
        wait_all(futures.into_iter()).await;

        {
            let mut inner = inner_lock.write().unwrap();
            inner.subscribers.extend(subscribers.into_iter());
        }
    }

    async fn incremental_sync_once<M: Meta>(
        inner_lock: &Arc<RwLock<Self>>,
        meta: &M,
        ts: Timestamp,
    ) -> anyhow::Result<Timestamp> {
        let (revisions, new_ts) = Retry::new()
            .indefinitely(&|| async {
                let (revisions, new_ts) = meta.sync(ts).await?;
                Ok::<_, anyhow::Error>((revisions, new_ts))
            })
            .await;

        let mut sync_items = HashSet::new();

        let snapshot = {
            let mut inner = inner_lock.write().unwrap();
            for revision in &revisions {
                // If this fails to parse it must be a key structure that we don't know about, which
                // means that none of the readers should care about it either.
                //
                // Realistically this should not happen, because version upgrades should either add
                // the structure of a new key or actual usages of a new key, but not both.
                if let Ok(meta_key) = MetaKey::decode(&revision.key.1[..]) {
                    sync_items.insert(meta_key);
                } else {
                    log::warn!(
                        "ignoring unknown MetaKey during sync {:?}",
                        hexlify(&revision.key.1[..])
                    );
                }

                match &revision.value {
                    RevisionValue::Regular(v) => {
                        inner.kv.insert(revision.key.1.clone(), v.clone());
                    }
                    RevisionValue::Tombstone => {
                        inner.kv.remove(&revision.key.1);
                    }
                }
            }
            inner.kv.clone()
        };

        Self::notify_and_wait_subscribers(inner_lock, SyncType::Tx(sync_items), snapshot.clone())
            .await;

        let (router, owned_ranges) = Self::regen_router(snapshot).await?;

        {
            let mut inner = inner_lock.write().unwrap();
            inner.router = router;
            inner.owned_ranges = owned_ranges;
            // Important: this must be after all of the subscriber updates, since this timestamp
            // also describes _their_ state.
            inner.synced_ts.set(new_ts);
        }

        if new_ts == ts {
            Retry::new()
                .indefinitely(&|| async {
                    meta.wait_for_newer(ts).await?;
                    Ok::<_, anyhow::Error>(())
                })
                .await;
        }
        Ok(new_ts)
    }

    async fn regen_router(
        snapshot: MetaSyncedSnapshot,
    ) -> anyhow::Result<(
        StaticRouter,
        HashMap<TabletId, HashMap<ColoGroupId, Range<Vec<u8>>>>,
    )> {
        let mut ranges_by_colo_group = HashMap::new();
        let mut tablet_map = HashMap::new();

        for tablet_id in snapshot.tablet_ids().await? {
            let tablet_metadata = snapshot.tablet_metadata(tablet_id).await?;

            ranges_by_colo_group
                .entry(tablet_metadata.colo_group_id)
                .or_insert_with(Vec::new)
                .push((tablet_metadata.range.clone(), tablet_id));
            tablet_map
                .entry(tablet_id)
                .or_insert_with(HashMap::new)
                .insert(tablet_metadata.colo_group_id, tablet_metadata.range);
        }

        let mut routing_map = HashMap::new();
        for (colo_group_id, ranges) in ranges_by_colo_group.iter_mut() {
            ranges.sort_unstable_by_key(|(range, _)| range.lower.clone());

            let mut tablet_ids = vec![];
            let mut bounds = vec![];
            tablet_ids.push(ranges[0].1);
            for (range, tablet_id) in &ranges[1..] {
                bounds.push(range.lower.clone());
                tablet_ids.push(*tablet_id);
            }
            routing_map.insert(*colo_group_id, (bounds, tablet_ids));
        }

        Ok((StaticRouter::new(routing_map), tablet_map))
    }
}

#[derive(Clone)]
pub(crate) struct MetaSyncedSnapshot {
    // We have to clone these a lot, im::OrdMap makes this cheap.
    m: im::OrdMap<Vec<u8>, Vec<u8>>,
    // Keeping track of the maximum key length that exists in m is necessary to transform
    // Bound::AfterPrefix to a std::ops::Bound, since we need to be able to make an actual key
    // that's at the end of the prefix, which is done by padding the prefix to the maximum length
    // with 0xFF.
    max_key_len: usize,
}

impl MetaSyncedSnapshot {
    fn new() -> Self {
        Self {
            m: im::OrdMap::new(),
            max_key_len: 0,
        }
    }

    fn insert(&mut self, k: Vec<u8>, v: Vec<u8>) {
        self.max_key_len = cmp::max(self.max_key_len, k.len());
        self.m.insert(k, v);
    }

    fn remove(&mut self, k: &[u8]) {
        self.m.remove(k);
    }
}

#[async_trait]
impl MetaReader for MetaSyncedSnapshot {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.m.get(key).cloned())
    }

    fn scan(
        &self,
        range: Range<Vec<u8>>,
        direction: Direction,
    ) -> Box<dyn Stream<Item = anyhow::Result<(Vec<u8>, Vec<u8>)>> + Unpin + Send + '_> {
        match range.to_std_ops_bounds(self.max_key_len) {
            Some(range_bounds) => {
                let iter = self
                    .m
                    .range(range_bounds)
                    .map(|(k, v)| -> anyhow::Result<_> { Ok((k.clone(), v.clone())) });

                match direction {
                    Direction::Asc => Box::new(futures::stream::iter(iter)),
                    Direction::Desc => Box::new(futures::stream::iter(iter.rev())),
                }
            }
            None => Box::new(futures::stream::empty()),
        }
    }
}
