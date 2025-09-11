use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::anyhow;
use tokio::sync::watch;
use tokio::sync::Notify;

use crate::lsm::Lsm;
use crate::lsm::Manifest;
use crate::lsm::Preloaded;
use crate::meta::TabletState;
use crate::meta::TabletStateProperties;
use crate::obsidian::InternalError;
use crate::obsidian::TabletId;
use crate::range::Bound;
use crate::range::Range;
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

pub(super) struct ProtectedLsm<S: Storage> {
    tablet_id: TabletId,
    state: InfrequentlyChanged<TabletStateProperties>,
    changed: watch::Receiver<()>,
    on_change: watch::Sender<()>,
    lsm: Lsm<S>,
}

impl<S> ProtectedLsm<S>
where
    S: Storage,
{
    pub(super) fn new(tablet_id: TabletId, lsm: Lsm<S>, initial: TabletState) -> Self {
        let (on_change, changed) = watch::channel(());
        Self {
            tablet_id,
            state: InfrequentlyChanged::new(initial.properties()),
            changed,
            on_change,
            lsm,
        }
    }

    pub(super) async fn set_state(&self, state: TabletState) {
        self.state.store_and_wait(state.properties()).await;
        let _ = self.on_change.send(());
        log::info!(
            "{:?}: set_state({:?}) ({:?})",
            self.tablet_id,
            state,
            state.properties()
        );
    }

    pub(super) fn read<'a>(&'a self) -> Result<LsmReadGuard<'a, S>, InternalError> {
        let guard = self.state.load();

        if !guard.contains(TabletStateProperties::Readable) {
            return Err(InternalError::TabletNotReadable(self.tablet_id));
        }

        Ok(LsmReadGuard {
            guard,
            lsm: &self.lsm,
        })
    }

    pub(super) async fn wait_read<'a>(&'a self) -> LsmReadGuard<'a, S> {
        let guard = self.wait(TabletStateProperties::Readable).await;
        LsmReadGuard {
            guard,
            lsm: &self.lsm,
        }
    }

    pub(super) fn read_write<'a>(&'a self) -> Result<LsmReadWriteGuard<'a, S>, InternalError> {
        let guard = self.state.load();

        if guard.contains(TabletStateProperties::Readable | TabletStateProperties::Writable) {
            return Ok(LsmReadWriteGuard {
                guard,
                lsm: &self.lsm,
            });
        }

        Err(InternalError::TabletNotWriteable(self.tablet_id))
    }

    pub(super) async fn wait_read_write<'a>(&'a self) -> LsmReadWriteGuard<'a, S> {
        let guard = self
            .wait(TabletStateProperties::Readable | TabletStateProperties::Writable)
            .await;
        LsmReadWriteGuard {
            guard,
            lsm: &self.lsm,
        }
    }

    pub(super) fn load<'a>(&'a self) -> Result<LsmLoadGuard<'a, S>, InternalError> {
        let guard = self.state.load();

        if guard.contains(TabletStateProperties::Hydrating) {
            return Ok(LsmLoadGuard {
                guard,
                lsm: &self.lsm,
            });
        }

        Err(InternalError::TabletNotHydrating(self.tablet_id))
    }

    pub(super) fn manifest(&self) -> Manifest {
        self.lsm.manifest()
    }

    pub(super) async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>> {
        self.lsm
            .find_split()
            .ok_or_else(|| anyhow!("no split candidates for {:?}", self.tablet_id))
    }

    pub(super) async fn flush(&self) -> anyhow::Result<()> {
        self.lsm.flush().await
    }

    pub(super) fn set_splits(&self, splits: Vec<Bound<Vec<u8>>>) {
        self.lsm.set_splits(splits);
    }

    async fn wait<'a>(
        &'a self,
        props: TabletStateProperties,
    ) -> InfrequentlyChangedGuard<'a, TabletStateProperties> {
        let mut rx = self.changed.clone();

        loop {
            let _ = rx.borrow_and_update();

            {
                let guard = self.state.load();
                if guard.contains(props) {
                    return guard;
                }
            }

            rx.changed()
                .await
                .expect("tx also owned by self, must drop together");
        }
    }
}

pub(super) trait LsmRead {
    async fn get(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        key: &[u8],
    ) -> anyhow::Result<Option<(Timestamp, RevisionValue)>>;

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<Revision>, Option<Range<Vec<u8>>>)>;

    async fn history_page(
        &self,
        keyspace_id: KeyspaceId,
        key: &[u8],
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<(Timestamp, RevisionValue)>, Option<HistoryRange>)>;

    fn keyspaces(&self) -> Vec<KeyspaceId>;
}

pub(super) trait LsmReadWrite: LsmRead {
    async fn write(
        &self,
        ts: Timestamp,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<(), WriteError>;

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()>;
}

pub(super) struct LsmReadGuard<'a, S: Storage> {
    guard: InfrequentlyChangedGuard<'a, TabletStateProperties>,
    lsm: &'a Lsm<S>,
}

impl<'a, S> LsmRead for LsmReadGuard<'a, S>
where
    S: Storage,
{
    async fn get(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        key: &[u8],
    ) -> anyhow::Result<Option<(Timestamp, RevisionValue)>> {
        Ok(self.lsm.get(ts, keyspace_id, key).await?)
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<Revision>, Option<Range<Vec<u8>>>)> {
        Ok(self
            .lsm
            .scan_page(ts, keyspace_id, range, direction, limit)
            .await?)
    }

    async fn history_page(
        &self,
        keyspace_id: KeyspaceId,
        key: &[u8],
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<(Timestamp, RevisionValue)>, Option<HistoryRange>)> {
        Ok(self
            .lsm
            .history_page(keyspace_id, key, range, direction, limit)
            .await?)
    }

    fn keyspaces(&self) -> Vec<KeyspaceId> {
        self.lsm.keyspaces()
    }
}

pub(super) struct LsmReadWriteGuard<'a, S: Storage> {
    guard: InfrequentlyChangedGuard<'a, TabletStateProperties>,
    lsm: &'a Lsm<S>,
}

impl<'a, S> LsmRead for LsmReadWriteGuard<'a, S>
where
    S: Storage,
{
    async fn get(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        key: &[u8],
    ) -> anyhow::Result<Option<(Timestamp, RevisionValue)>> {
        Ok(self.lsm.get(ts, keyspace_id, key).await?)
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<Revision>, Option<Range<Vec<u8>>>)> {
        Ok(self
            .lsm
            .scan_page(ts, keyspace_id, range, direction, limit)
            .await?)
    }

    async fn history_page(
        &self,
        keyspace_id: KeyspaceId,
        key: &[u8],
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<(Timestamp, RevisionValue)>, Option<HistoryRange>)> {
        Ok(self
            .lsm
            .history_page(keyspace_id, key, range, direction, limit)
            .await?)
    }

    fn keyspaces(&self) -> Vec<KeyspaceId> {
        self.lsm.keyspaces()
    }
}

impl<'a, S> LsmReadWrite for LsmReadWriteGuard<'a, S>
where
    S: Storage,
{
    async fn write(
        &self,
        ts: Timestamp,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<(), WriteError> {
        Ok(self.lsm.write(ts, preconds, muts).await?)
    }

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        Ok(self.lsm.create_keyspace(keyspace_id).await?)
    }
}

pub(super) struct LsmLoadGuard<'a, S: Storage> {
    guard: InfrequentlyChangedGuard<'a, TabletStateProperties>,
    lsm: &'a Lsm<S>,
}

impl<'a, S> LsmLoadGuard<'a, S>
where
    S: Storage,
{
    pub async fn load(&self, preloaded: Preloaded<S::Reader>) -> anyhow::Result<()> {
        self.lsm.load(preloaded).await
    }
}

struct InfrequentlyChanged<T> {
    // TODO: Even the loaders will end up contending on the refcount. If T is clone, we could split
    // this into thread stripes.
    inner: arc_atomic::AtomicArc<(AtomicU64, T)>,
    notify: Notify,
}

impl<T> InfrequentlyChanged<T> {
    fn new(init: T) -> Self {
        Self {
            inner: arc_atomic::AtomicArc::new(Arc::new((AtomicU64::new(0), init))),
            notify: Notify::new(),
        }
    }

    fn load<'a>(&'a self) -> InfrequentlyChangedGuard<'a, T> {
        loop {
            let inner = self.inner.load();
            if inner.0.fetch_add(1, Ordering::SeqCst) < 1 << 63 {
                return InfrequentlyChangedGuard {
                    inner: inner,
                    notify: &self.notify,
                };
            }
        }
    }

    // Stores item into self and waits until all InfrequentlyChangedGuards with the old value are
    // dropped.
    async fn store_and_wait(&self, item: T) {
        let prev = self.inner.load();
        prev.0.fetch_or(1 << 63, Ordering::SeqCst);
        self.inner.store(Arc::new((AtomicU64::new(0), item)));
        loop {
            if prev.0.load(Ordering::SeqCst) == 1 << 63 {
                break;
            }
            self.notify.notified().await;
        }
    }
}

struct InfrequentlyChangedGuard<'a, T> {
    inner: Arc<(AtomicU64, T)>,
    notify: &'a Notify,
}

impl<'a, T> Deref for InfrequentlyChangedGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner.deref().1
    }
}

impl<'a, T> Drop for InfrequentlyChangedGuard<'a, T> {
    fn drop(&mut self) {
        let (ctr, _) = self.inner.deref();
        if ctr.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.notify.notify_waiters();
        }
    }
}
