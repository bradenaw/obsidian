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
use crate::storage::CachedStorage;
use crate::storage::MemStorage;
use crate::tablet::LsmTablet;
use crate::tablet::Tablet;
use crate::types::ColoGroupId;
use crate::types::Direction;
use crate::types::HistoryRange;
use crate::types::Key;
use crate::types::KeyspaceId;
use crate::types::Mutation;
use crate::types::Precondition;
use crate::types::Record;
use crate::types::Revision;
use crate::types::ShardId;
use crate::types::Timestamp;
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
impl<T: Tablet + Send + Sync + ?Sized> Tablet for Arc<T> {
    async fn get(&self, ts: Timestamp, key: &Key) -> Result<Option<Record>, InternalError> {
        T::get(self, ts, key).await
    }

    async fn get_latest(&self, key: Key) -> Result<(Timestamp, Option<Record>), InternalError> {
        T::get_latest(self, key).await
    }

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> Result<Timestamp, InternalError> {
        T::latest_snapshot(self, keys).await
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<Range<Vec<u8>>>), InternalError> {
        T::scan_page(self, ts, keyspace_id, range, direction, limit).await
    }

    async fn history_page(
        &self,
        key: Key,
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Revision>, Option<HistoryRange>), InternalError> {
        T::history_page(self, key, range, direction, limit).await
    }

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        T::write(self, preconds, muts).await
    }

    async fn prepare(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        T::prepare(self, txid, preconds, muts).await
    }

    async fn try_commit(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<TxOutcome> {
        T::try_commit(self, txid, ts, precond_keys, mut_keys).await
    }

    async fn try_abort(&self, txid: Txid) -> anyhow::Result<TxOutcome> {
        T::try_abort(self, txid).await
    }

    async fn wait(&self, txid: Txid) -> Result<TxOutcome, InternalError> {
        T::wait(self, txid).await
    }

    async fn cleanup_committed(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        T::cleanup_committed(self, txid, ts, precond_keys, mut_keys).await
    }

    async fn wait_meta_sync(&self, ts: Timestamp) -> anyhow::Result<()> {
        T::wait_meta_sync(self, ts).await
    }
}

struct StaticTablets {
    m: Mutex<HashMap<TabletId, Arc<dyn Tablet + Send + Sync + 'static>>>,
}

impl Tablets for Arc<StaticTablets> {
    fn tablet(&self, tablet_id: TabletId) -> anyhow::Result<Box<dyn Tablet + Send + Sync>> {
        let m = self.m.lock().unwrap();
        let tablet_arc = m
            .get(&tablet_id)
            .ok_or_else(|| anyhow::anyhow!("no tablet for {}", tablet_id))?;

        Ok(Box::new(tablet_arc.clone()))
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
    async fn add_tablet(&self, _tablet_id: TabletId) -> anyhow::Result<()> {
        todo!()
    }

    async fn create_colo_group(
        &self,
        colo_group_id: ColoGroupId,
        initial_splits: Vec<Bound<Vec<u8>>>,
    ) -> anyhow::Result<()> {
        let inner = self.inner.load();
        if let Some(inner) = inner.deref() {
            return T::create_colo_group(inner, colo_group_id, initial_splits).await;
        }
        Err(anyhow!("MetaProxy not filled yet"))
    }

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        let inner = self.inner.load();
        if let Some(inner) = inner.deref() {
            return T::create_keyspace(inner, keyspace_id).await;
        }
        Err(anyhow!("MetaProxy not filled yet"))
    }

    async fn latest_snapshot(&self) -> anyhow::Result<Timestamp> {
        let inner = self.inner.load();
        if let Some(inner) = inner.deref() {
            return T::latest_snapshot(inner).await;
        }
        Err(anyhow!("MetaProxy not filled yet"))
    }

    async fn wait_for_newer(&self, ts: Timestamp) -> anyhow::Result<()> {
        let inner = self.inner.load();
        if let Some(inner) = inner.deref() {
            return T::wait_for_newer(inner, ts).await;
        }
        Err(anyhow!("MetaProxy not filled yet"))
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        range: Range<Vec<u8>>,
    ) -> anyhow::Result<(Vec<Record>, Option<Range<Vec<u8>>>)> {
        let inner = self.inner.load();
        if let Some(inner) = inner.deref() {
            return T::scan_page(inner, ts, range).await;
        }
        Err(anyhow!("MetaProxy not filled yet"))
    }

    async fn sync(&self, ts: Timestamp) -> anyhow::Result<(Vec<Revision>, Timestamp)> {
        let inner = self.inner.load();
        if let Some(inner) = inner.deref() {
            return T::sync(inner, ts).await;
        }
        Err(anyhow!("MetaProxy not filled yet"))
    }

    async fn tablet_ids(&self, ts: Timestamp) -> anyhow::Result<Vec<TabletId>> {
        let inner = self.inner.load();
        if let Some(inner) = inner.deref() {
            return T::tablet_ids(inner, ts).await;
        }
        Err(anyhow!("MetaProxy not filled yet"))
    }
}

pub(crate) async fn new_for_test(n_tablets: usize) -> anyhow::Result<Frontend> {
    let tablets = Arc::new(StaticTablets {
        m: Mutex::new(HashMap::new()),
    });

    let meta_proxy = Arc::new(MetaProxy::new());

    let storage = Arc::new(CachedStorage::new(
        MemStorage::new(),
        64, // page_size
        4,  // stripe_size_pages
        4,  // n_stripes
    ));

    let meta_tablet = Arc::new(
        LsmTablet::new(
            TabletId::META,
            LsmBuilder::new(storage.clone())
                .l0_max_size(256)
                .run_target_size(65536)
                .block_size(4096)
                .build()
                .await?,
            Box::new(meta_proxy.clone()),
            Box::new(tablets.clone()),
        )
        .await?,
    );
    meta_tablet.create_keyspace(KeyspaceId::META).await?;
    // TODO: remove when keyspace sync from meta works
    meta_tablet
        .create_keyspace(KeyspaceId(ColoGroupId(1), 1))
        .await?;
    meta_tablet
        .create_keyspace(KeyspaceId(ColoGroupId(1), 2))
        .await?;
    let meta = MetaImpl::new(meta_tablet.clone());

    {
        let mut m = tablets.m.lock().unwrap();
        m.insert(TabletId::META, meta_tablet);
    }
    meta.add_tablet(TabletId::META).await?;

    for i in 0..(n_tablets - 1) {
        let tablet_id = TabletId(ShardId(1), (i + 2) as u64);
        let tablet = LsmTablet::new(
            tablet_id,
            LsmBuilder::new(storage.clone()).build().await?,
            Box::new(meta_proxy.clone()),
            Box::new(tablets.clone()),
        )
        .await?;
        // TODO: remove when keyspace sync from meta works
        tablet
            .create_keyspace(KeyspaceId(ColoGroupId(1), 1))
            .await?;
        tablet
            .create_keyspace(KeyspaceId(ColoGroupId(1), 2))
            .await?;

        let mut m = tablets.m.lock().unwrap();
        m.insert(tablet_id, Arc::new(tablet));
        meta.add_tablet(tablet_id).await?;
    }

    meta_proxy.put(meta);

    Ok(Frontend::new(
        Box::new(meta_proxy.clone()),
        MetaSynced::new(meta_proxy.clone()),
        Box::new(tablets),
    ))
}

pub(crate) fn single_byte_splits(n: usize) -> Vec<Bound<Vec<u8>>> {
    if n > 255 {
        panic!("can't do single_byte_splits with n > 255");
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push(Bound::Before(vec![i as u8]));
    }
    out
}

pub(crate) fn assert_roundtrip<E: Encode + Decode + Debug + Eq>(e: &E) -> anyhow::Result<()> {
    let encoded = encode(e);
    let decoded = E::decode(&encoded)?;
    assert_eq!(e, &decoded);
    Ok(())
}

macro_rules! obsidian_test_suite {
    ($make:expr) => {
        mod obsidian_test_suite {
            use std::collections::BTreeMap;

            use crate::obsidian::Obsidian;
            use crate::range::Bound;
            use crate::range::Range;
            use crate::types::ColoGroupId;
            use crate::types::Direction;
            use crate::types::KeyspaceId;
            use crate::types::Mutation;
            use crate::types::Record;
            use crate::types::Timestamp;

            #[tokio::test]
            async fn test_2pc() -> anyhow::Result<()> {
                let colo_group_id = ColoGroupId(1);
                let keyspace_id = KeyspaceId(colo_group_id, 1);

                let obs = $make(2).await?;
                obs.create_colo_group(colo_group_id, vec![Bound::Before(vec![2])])
                    .await?;
                obs.create_keyspace(keyspace_id).await?;

                let key1 = vec![1];
                let key2 = vec![2];

                let write_ts = obs
                    .write(
                        vec![],
                        BTreeMap::from([
                            ((keyspace_id, key1.clone()), Mutation::Put(vec![1, 2, 3])),
                            ((keyspace_id, key2.clone()), Mutation::Put(vec![4, 5, 6])),
                        ]),
                    )
                    .await?;

                assert_eq!(
                    obs.get(write_ts, &(keyspace_id, key1)).await?.map(|record| record.value),
                    Some(vec![1, 2, 3])
                );
                assert_eq!(
                    obs.get(write_ts, &(keyspace_id, key2)).await?.map(|record| record.value),
                    Some(vec![4, 5, 6])
                );

                Ok(())
            }

            #[tokio::test]
            async fn test_scan_page() {
                let _ = pretty_env_logger::try_init();

                async fn inner() -> anyhow::Result<()> {
                    let colo_group_id = ColoGroupId(1);
                    let keyspace_id = KeyspaceId(colo_group_id, 1);

                    let obs = $make(3).await?;
                    obs.create_colo_group(
                        colo_group_id,
                        vec![Bound::Before(vec![2]), Bound::Before(vec![3])],
                    )
                    .await?;

                    let writes: [(Vec<u8>, _); 12] = [
                        //          ts=0123456789
                        (vec![1, 0], b" o  o    o"),
                        (vec![1, 1], b"   o     o"),
                        (vec![1, 2], b"   o x    "),
                        (vec![1, 3], b"   oxo    "),
                        (vec![2, 0], b"    o   o "),
                        (vec![2, 1], b"     o  o "),
                        (vec![2, 2], b" o x  o  o"),
                        (vec![3, 0], b"  o oxo  o"),
                        (vec![3, 1], b"  o  oo o "),
                        (vec![3, 2], b" xoxoxoxox"),
                        (vec![3, 3], b"        o "),
                        (vec![3, 4], b" ooooooooo"),
                    ];

                    let mut timestamps = vec![Timestamp(0)];
                    for ts_idx in 1..writes[0].1.len() {
                        let mut mutations = BTreeMap::new();
                        for (key, versions) in &writes {
                            let mutation = match versions[ts_idx] {
                                b'o' => Mutation::Put(format!("{:?} {}", key, ts_idx).into()),
                                b'x' => Mutation::Delete,
                                _ => continue,
                            };

                            mutations.insert((keyspace_id, key.clone()), mutation);
                        }

                        if mutations.is_empty() {
                            timestamps.push(timestamps.last().cloned().unwrap_or(Timestamp(0)));
                            continue;
                        }

                        let ts = obs.write(vec![], mutations).await?;
                        timestamps.push(ts);
                    }

                    async fn check<O: Obsidian>(
                        obs: &O,
                        timestamps: &[Timestamp],
                        ts_idx: usize,
                        range: Range<&[u8]>,
                        expected: Vec<(Vec<u8>, usize)>,
                    ) -> anyhow::Result<()> {
                        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
                        for direction in [Direction::Asc, Direction::Desc] {
                            for page_size in 1..=expected.len() {
                                let mut maybe_cursor = Some(range.to_vec());
                                let mut results = vec![];
                                while let Some(cursor) = maybe_cursor {
                                    let (page, continue_cursor) = obs
                                        .scan_page(
                                            timestamps[ts_idx],
                                            keyspace_id,
                                            cursor.borrow(),
                                            direction,
                                            page_size,
                                        )
                                        .await?;

                                    assert!(page.len() <= page_size);
                                    results.extend(page);
                                    assert_ne!(continue_cursor, Some(cursor));
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
                                        .map(|(key, ts_idx)| Record {
                                            key: (keyspace_id, key.clone()),
                                            ts: timestamps[ts_idx],
                                            value: format!("{:?} {}", key, ts_idx).into(),
                                        })
                                        .collect::<Vec<_>>(),
                                    "scan_page(ts={:?}, /*keyspace_id*/, /*cursor*/, direction={:?}, page_size={})",
                                    timestamps[ts_idx],
                                    direction,
                                    page_size,
                                );
                            }
                        }

                        Ok(())
                    }

                    check(
                        &obs,
                        &timestamps,
                        5,
                        Range {
                            lower: Bound::Before(&[1, 1]),
                            upper: Bound::After(&[2, 0]),
                        },
                        vec![(vec![1, 1], 3), (vec![1, 3], 5), (vec![2, 0], 4)],
                    )
                    .await?;

                    check(
                        &obs,
                        &timestamps,
                        4,
                        Range::all(),
                        vec![
                            (vec![1, 0], 4),
                            (vec![1, 1], 3),
                            (vec![1, 2], 3),
                            // [1,3] got deleted at 4
                            (vec![2, 0], 4),
                            // [2,1] doesn't exist yet
                            // [2,2] got deleted at 3
                            (vec![3, 0], 4),
                            (vec![3, 1], 2),
                            (vec![3, 2], 4),
                            // [3,3] doesn't exist yet
                            (vec![3, 4], 4),
                        ],
                    )
                    .await?;

                    Ok(())
                }

                if let Err(e) = inner().await {
                    panic!("{:?}", e);
                }
            }
        }
    }
}

pub(crate) use obsidian_test_suite;
