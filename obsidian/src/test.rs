use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::anyhow;
use async_trait::async_trait;

use crate::lsm::LsmBuilder;
use crate::meta::Meta;
use crate::meta::MetaImpl;
use crate::meta_synced::MetaSynced;
use crate::obsidian::Frontend;
use crate::obsidian::InternalError;
use crate::obsidian::Router;
use crate::obsidian::TabletId;
use crate::obsidian::Tablets;
use crate::obsidian::TxOutcome;
use crate::obsidian::Txid;
use crate::range::Bound;
use crate::range::Range;
use crate::range::RangeSet;
use crate::storage::MemStorage;
use crate::tablet::LsmTablet;
use crate::tablet::Tablet;
use crate::types::ColoGroupId;
use crate::types::Direction;
use crate::types::HistoryRange;
use crate::types::KeyspaceId;
use crate::types::Mutation;
use crate::types::Precondition;
use crate::types::Record;
use crate::types::ShardId;
use crate::types::Timestamp;
use crate::types::Value;
use crate::util::encode;
use crate::util::AtomicArc;
use crate::util::Decode;
use crate::util::Encode;

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
    ) -> Result<(Vec<(Vec<u8>, Timestamp, Vec<u8>)>, Option<Range<Vec<u8>>>), InternalError> {
        T::scan_page(self, ts, keyspace_id, range, direction, limit).await
    }

    async fn history_page(
        &self,
        keyspace_id: KeyspaceId,
        key: &[u8],
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<(Timestamp, Value)>, Option<HistoryRange>), InternalError> {
        T::history_page(self, keyspace_id, key, range, direction, limit).await
    }

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<(KeyspaceId, Vec<u8>), Mutation>,
    ) -> Result<Timestamp, InternalError> {
        T::write(self, preconds, muts).await
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

struct MetaProxy<T> {
    inner: AtomicArc<Option<T>>,
}

impl<T> MetaProxy<T> {
    fn new() -> Self {
        Self {
            inner: AtomicArc::new(Arc::new(None)),
        }
    }

    fn put(&self, t: T) {
        self.inner.store(Arc::new(Some(t)))
    }
}

#[async_trait]
impl<T: Meta + Send + Sync> Meta for Arc<MetaProxy<T>> {
    async fn create_colo_group(&self, colo_group_id: ColoGroupId) -> anyhow::Result<()> {
        let inner = self.inner.load();
        if let Some(inner) = inner.deref() {
            return T::create_colo_group(inner, colo_group_id).await;
        }
        Err(anyhow!("MetaProxy not filled yet"))
    }

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        todo!();
    }

    async fn latest_snapshot(&self) -> anyhow::Result<Timestamp> {
        todo!();
    }

    async fn wait_for_newer(&self, ts: Timestamp) -> anyhow::Result<()> {
        todo!();
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        range: Range<Vec<u8>>,
    ) -> anyhow::Result<(Vec<(Vec<u8>, Timestamp, Vec<u8>)>, Option<Range<Vec<u8>>>)> {
        todo!();
    }

    async fn sync(&self, ts: Timestamp) -> anyhow::Result<(Vec<Record>, Timestamp)> {
        todo!();
    }
}

pub(crate) async fn new_with_single_byte_routing(n_tablets: usize) -> anyhow::Result<Frontend> {
    let tablets = Arc::new(StaticTablets {
        m: Mutex::new(HashMap::new()),
    });

    let meta_proxy = Arc::new(MetaProxy::new());

    let storage = Arc::new(MemStorage::new());
    let meta_tablet = LsmTablet::new(
        TabletId::META,
        LsmBuilder::new().storage(storage.clone()).build().await?,
        vec![(ColoGroupId::META, RangeSet::from(Range::all()))]
            .into_iter()
            .collect(),
        Box::new(tablets.clone()),
        Box::new(MetaSynced::new(meta_proxy.clone())),
    )
    .await?;

    let meta = MetaImpl::new(meta_tablet);
    meta_proxy.put(meta);

    for i in 0..n_tablets {
        let tablet_id = TabletId(ShardId(1), (i+2) as u64);
        let tablet = LsmTablet::new(
            tablet_id,
            LsmBuilder::new().storage(storage.clone()).build().await?,
            Box::new(tablets.clone()),
            Box::new(router.clone()),
        )
        .await?;
        for keyspace_id in keyspace_ids {
            tablet.create_keyspace(keyspace_id).await?;
        }
        let mut m = tablets.m.lock().unwrap();
        m.insert(*tablet_id, Arc::new(tablet));
    }

    Ok(Frontend::new(Box::new(router.clone()), Box::new(tablets)))
}

pub(crate) fn assert_roundtrip<E: Encode + Decode + Debug + Eq>(e: &E) -> anyhow::Result<()> {
    let encoded = encode(e);
    let decoded = E::decode(&encoded)?;
    assert_eq!(e, &decoded);
    Ok(())
}
