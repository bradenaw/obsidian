mod mem_file_reader;
mod mem_file_writer;
mod mem_journal;
mod mem_journals;
mod mem_storage;
pub(crate) mod suite;
mod test_nodes;

use std::fmt::Debug;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;

use crate::election::Proposal;
use crate::gateway::Gateway;
use crate::meta::MetaSynced;
use crate::meta::MetaSyncedSnapshot;
use crate::runtime;
use crate::runtime::Journals;
use crate::runtime::Shards as _;
use crate::runtime::Storage;
use crate::storage::CachedStorage;
use crate::tablet::TabletJournalWriter;
pub(crate) use crate::test::mem_file_reader::MemFileReader;
pub(crate) use crate::test::mem_file_writer::MemFileWriter;
pub(crate) use crate::test::mem_journal::MemJournal;
pub(crate) use crate::test::mem_journals::MemJournals;
pub(crate) use crate::test::mem_storage::MemStorage;
pub(crate) use crate::test::suite::obsidian_test_suite;
use crate::test::test_nodes::TestNodes;
use crate::util::encode;
use crate::util::Decode;
use crate::util::Encode;
use crate::util::Retry;
use crate::Bound;
use crate::JournalEntry;
use crate::ShardId;
use crate::TabletJournalEntry;

pub(crate) struct ObsidianForTest {
    pub gateway: Gateway,
    pub meta: Arc<dyn runtime::Meta>,
    pub meta_synced: Arc<MetaSynced>,
    pub supervisor: Arc<dyn runtime::Supervisor>,

    nodes: TestNodes,
}

impl ObsidianForTest {
    pub(crate) async fn new(n_shards: usize) -> anyhow::Result<Self> {
        if n_shards < 1 {
            return Err(anyhow!("need at least one shard to host the meta tablet"));
        }

        let storage = Arc::new(CachedStorage::new(
            MemStorage::new(),
            64, // page_size
            4,  // stripe_size_pages
            4,  // n_stripes
        ));

        let journals = Arc::new(MemJournals::new()) as Arc<dyn Journals<Proposal<JournalEntry>>>;

        let nodes = TestNodes::new(Arc::clone(&storage) as Arc<dyn Storage>, journals);

        for _ in 0..n_shards {
            nodes.create_node().await?;
        }

        let meta = nodes.discovery().meta();

        // Wait for a meta to get elected.
        Retry::new()
            .indefinitely(&async || meta.latest_snapshot().await)
            .await;

        let shard_ids: Vec<_> = (0..n_shards).map(|i| ShardId((i + 1) as u32)).collect();

        for shard_id in &shard_ids {
            meta.add_shard(*shard_id).await?;
        }

        // Wait for the supervisor to assign shards and for replicas to finish leader election.
        let meta_ts = meta.latest_snapshot().await?;
        for shard_id in &shard_ids {
            Retry::new()
                .indefinitely(&async || {
                    let shard = nodes.discovery().shard(*shard_id)?;
                    shard.wait_meta_sync(meta_ts).await?;
                    Ok::<(), anyhow::Error>(())
                })
                .await;
        }

        let gateway = Gateway::new(
            Arc::clone(&meta),
            MetaSynced::new(Arc::clone(&meta)),
            nodes.shards(),
        );

        let meta_synced = Arc::new(MetaSynced::new(nodes.discovery().meta()));
        Ok(Self {
            gateway,
            meta,
            meta_synced,
            supervisor: nodes.discovery().supervisor(),
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
