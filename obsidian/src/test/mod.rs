mod mem_file_reader;
mod mem_file_writer;
mod mem_journal;
mod mem_journals;
mod mem_storage;
mod meta_proxy;
mod test_nodes;

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;

use crate::election::Proposal;
use crate::gateway::Gateway;
use crate::lsm::Lsm;
use crate::lsm::LsmOptions;
use crate::meta::MetaImpl;
use crate::meta::MetaSynced;
use crate::meta::MetaSyncedSnapshot;
use crate::runtime::Journals;
use crate::runtime::Meta;
use crate::runtime::Storage;
use crate::storage::CachedStorage;
use crate::supervisor::Supervisor;
use crate::tablet::TabletJournalWriter;
pub(crate) use crate::test::mem_file_reader::MemFileReader;
pub(crate) use crate::test::mem_file_writer::MemFileWriter;
pub(crate) use crate::test::mem_journal::MemJournal;
pub(crate) use crate::test::mem_journals::MemJournals;
pub(crate) use crate::test::mem_storage::MemStorage;
use crate::test::meta_proxy::MetaProxy;
use crate::test::test_nodes::TestNodes;
use crate::util::encode;
use crate::util::Decode;
use crate::util::Encode;
use crate::Bound;
use crate::JournalEntry;
use crate::ShardId;
use crate::TabletJournalEntry;

pub(crate) struct ObsidianForTest {
    pub gateway: Gateway,
    pub meta: Arc<dyn Meta>,
    pub meta_synced: Arc<MetaSynced>,
    pub supervisor: Supervisor,

    nodes: TestNodes,
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

        let journals = Arc::new(MemJournals::new()) as Arc<dyn Journals<Proposal<JournalEntry>>>;

        let meta_tablet = crate::tablet::MetaTablet::new(
            Lsm::empty(
                LsmOptions::default(),
                Arc::clone(&storage) as Arc<dyn Storage>,
            )
            .await?,
            Arc::new(NoopJournalWriter {}),
        );
        let meta: Arc<dyn Meta> = Arc::new(MetaImpl::new(meta_tablet));
        meta_proxy.put(Arc::clone(&meta) as Arc<dyn Meta>);

        let nodes = TestNodes::new(
            Arc::clone(&storage) as Arc<dyn Storage>,
            Arc::clone(&meta) as Arc<dyn Meta>,
            journals,
        );

        for i in 0..n_shards {
            let shard_id = ShardId((2 + i) as u32);
            meta.add_shard(shard_id).await?;
            nodes.create_node().await?;
        }

        let meta_synced = Arc::new(MetaSynced::new(Arc::clone(&meta)));

        let supervisor = Supervisor::new(
            Arc::clone(&meta) as Arc<dyn Meta>,
            Arc::clone(&meta_synced),
            nodes.shards(),
        );

        let gateway = Gateway::new(
            Box::new(meta_proxy.clone()),
            MetaSynced::new(meta_proxy),
            nodes.shards(),
        );

        // JANK: Need to wait for everything to come to life.
        tokio::time::sleep(Duration::from_millis(500)).await;

        Ok(Self {
            gateway,
            meta,
            meta_synced,
            supervisor,
            nodes,
        })
    }

    pub async fn latest_meta_snapshot(&self) -> anyhow::Result<MetaSyncedSnapshot> {
        self.meta_synced
            .wait(self.meta.latest_snapshot().await?)
            .await?;
        Ok(self.meta_synced.snapshot())
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

struct NoopJournalWriter {}

#[async_trait]
impl TabletJournalWriter for NoopJournalWriter {
    async fn append(&self, _entry: TabletJournalEntry) -> anyhow::Result<()> {
        Ok(())
    }
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
