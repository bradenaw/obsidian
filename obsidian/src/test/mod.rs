mod meta_proxy;
mod mem_journal;
mod shards;
mod mem_wal;
mod mem_wals;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;

use crate::coordinator::Coordinator;
use crate::gateway::Gateway;
use crate::lsm::Manifest;
use crate::meta::MetaImpl;
use crate::meta::MetaSynced;
pub(crate) use mem_wal::MemWal;
pub(crate) use mem_wals::MemWals;
pub(crate) use mem_journal::MemJournal;
use crate::runtime::Meta;
use crate::runtime::Shards;
use crate::runtime::Tablet;
use crate::storage::CachedStorage;
use crate::storage::MemStorage;
use crate::test::meta_proxy::MetaProxy;
use crate::test::shards::TestShards;
use crate::util::encode;
use crate::util::Decode;
use crate::util::Encode;
use crate::Bound;
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
use crate::TabletId;
use crate::Timestamp;
use crate::TxOutcome;
use crate::Txid;

#[async_trait]
impl<T: Tablet + ?Sized> Tablet for Arc<T> {
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

    async fn manifest(&self) -> anyhow::Result<Manifest> {
        T::manifest(self).await
    }

    async fn wait_mostly_hydrated(&self) -> anyhow::Result<()> {
        T::wait_mostly_hydrated(self).await
    }

    async fn catchup(&self) -> anyhow::Result<()> {
        T::catchup(self).await
    }

    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>> {
        T::find_split(self).await
    }
}

pub(crate) struct ObsidianForTest {
    pub gateway: Gateway,
    pub coordinator: Coordinator<Arc<dyn Tablet>>,
    pub meta: Arc<MetaImpl<Arc<dyn Tablet>>>,
}

impl ObsidianForTest {
    pub(crate) async fn new(n_shards: usize) -> anyhow::Result<Self> {
        if n_shards < 1 {
            return Err(anyhow!("need at least one shard to host the meta tablet"));
        }

        let meta_proxy = Arc::new(MetaProxy::new());
        let storage = Arc::new(CachedStorage::new(
            MemStorage::new(),
            64, // page_size
            4,  // stripe_size_pages
            4,  // n_stripes
        ));

        let shards = Arc::new(TestShards::new(storage.clone(), meta_proxy.clone()));

        let mut shard_ids = vec![];
        for _ in 0..n_shards {
            shard_ids.push(shards.create_shard().await?);
        }

        let meta_tablet = shards.tablet(TabletId::META)?;
        let meta = Arc::new(MetaImpl::new(meta_tablet));

        let coordinator = Coordinator::new(
            Arc::clone(&meta),
            Arc::new(Arc::clone(&shards)) as Arc<dyn Shards>,
        );

        for shard_id in shard_ids {
            meta.add_shard(shard_id).await?;
        }

        meta_proxy.put(Arc::clone(&meta));

        let gateway = Gateway::new(
            Box::new(meta_proxy.clone()),
            MetaSynced::new(meta_proxy),
            Box::new(shards),
        );

        Ok(Self {
            gateway,
            meta,
            coordinator,
        })
    }
}

#[async_trait]
impl Tablet for Box<dyn Tablet> {
    async fn get(&self, ts: Timestamp, key: &Key) -> Result<Option<Record>, InternalError> {
        Ok(self.deref().get(ts, key).await?)
    }

    async fn get_latest(&self, key: Key) -> Result<(Timestamp, Option<Record>), InternalError> {
        Ok(self.deref().get_latest(key).await?)
    }

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> Result<Timestamp, InternalError> {
        Ok(self.deref().latest_snapshot(keys).await?)
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<Range<Vec<u8>>>), InternalError> {
        Ok(self
            .deref()
            .scan_page(ts, keyspace_id, range, direction, limit)
            .await?)
    }

    async fn history_page(
        &self,
        key: Key,
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Revision>, Option<HistoryRange>), InternalError> {
        Ok(self
            .deref()
            .history_page(key, range, direction, limit)
            .await?)
    }

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        Ok(self.deref().write(preconds, muts).await?)
    }

    async fn prepare(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        Ok(self.deref().prepare(txid, preconds, muts).await?)
    }

    async fn try_commit(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<TxOutcome> {
        Ok(self
            .deref()
            .try_commit(txid, ts, precond_keys, mut_keys)
            .await?)
    }

    async fn try_abort(&self, txid: Txid) -> anyhow::Result<TxOutcome> {
        Ok(self.deref().try_abort(txid).await?)
    }

    async fn wait(&self, txid: Txid) -> Result<TxOutcome, InternalError> {
        Ok(self.deref().wait(txid).await?)
    }

    async fn cleanup_committed(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        Ok(self
            .deref()
            .cleanup_committed(txid, ts, precond_keys, mut_keys)
            .await?)
    }

    async fn wait_meta_sync(&self, ts: Timestamp) -> anyhow::Result<()> {
        Ok(self.deref().wait_meta_sync(ts).await?)
    }

    async fn manifest(&self) -> anyhow::Result<Manifest> {
        self.deref().manifest().await
    }

    async fn wait_mostly_hydrated(&self) -> anyhow::Result<()> {
        self.deref().wait_mostly_hydrated().await
    }

    async fn catchup(&self) -> anyhow::Result<()> {
        self.deref().catchup().await
    }

    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>> {
        self.deref().find_split().await
    }
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

pub(crate) fn assert_roundtrip_pb<T, U>(x: T) -> anyhow::Result<()>
where
    T: Debug + Clone + Eq + TryFrom<U, Error = anyhow::Error>,
    U: prost::Message + From<T> + Default,
{
    let encoded = U::from(x.clone()).encode_to_vec();
    let decoded: T = U::decode(&encoded[..])?.try_into()?;

    assert_eq!(x, decoded);

    Ok(())
}

macro_rules! obsidian_test_suite {
    ($make:expr) => {
        mod obsidian_test_suite {
            use std::collections::BTreeMap;

            use crate::Obsidian;
            use crate::Bound;
            use crate::Range;
            use crate::ColoGroupId;
            use crate::Direction;
            use crate::KeyspaceId;
            use crate::Mutation;
            use crate::Record;
            use crate::Timestamp;

            #[tokio::test]
            async fn test_2pc() -> anyhow::Result<()> {
                let _ = pretty_env_logger::try_init();

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
                    obs.create_keyspace(keyspace_id).await?;

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
