mod grpc_bridge;
mod grpc_in_process_node_builder;
mod in_process_node_builder;
pub(crate) mod obsidian_suite;
pub(crate) mod tablet_suite;
mod test_node_builder;
mod test_nodes;

use std::fmt::Debug;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use obsidian_external::mem::MemJournals;
use obsidian_external::mem::MemStorage;
use obsidian_util::encode;
use obsidian_util::Decode;
use obsidian_util::Encode;
use obsidian_util::Retry;

use crate::election::Proposal;
use crate::gateway::Gateway;
use crate::meta::MetaSynced;
use crate::meta::MetaSyncedSnapshot;
use crate::runtime;
use obsidian_external::Journals;
use crate::runtime::Shards as _;
use crate::tablet::TabletJournalWriter;
pub(crate) use crate::test::grpc_bridge::node_grpc_bridge;
pub(crate) use crate::test::grpc_bridge::obsidian_grpc_bridge;
pub(crate) use crate::test::grpc_bridge::GrpcBridge;
pub(crate) use crate::test::grpc_in_process_node_builder::GrpcInProcessNodeBuilder;
pub(crate) use crate::test::in_process_node_builder::InProcessNodeBuilder;
pub(crate) use crate::test::obsidian_suite::obsidian_test_suite;
pub(crate) use crate::test::tablet_suite::tablet_test_suite;
pub(crate) use crate::test::test_node_builder::TestNodeBuilder;
pub(crate) use crate::test::test_nodes::TestNodes;
use crate::Bound;
use crate::JournalEntry;
use crate::Obsidian;
use crate::ShardId;
use crate::TabletJournalEntry;

pub(crate) struct ObsidianForTestBuilder {
    n_shards: usize,
    node_builder: Option<Box<dyn TestNodeBuilder>>,
}

impl ObsidianForTestBuilder {
    pub fn new() -> ObsidianForTestBuilder {
        ObsidianForTestBuilder {
            n_shards: 1,
            node_builder: None,
        }
    }

    pub fn n_shards(mut self, n_shards: usize) -> Self {
        self.n_shards = n_shards;
        self
    }

    pub fn node_builder(mut self, node_builder: Box<dyn TestNodeBuilder>) -> Self {
        self.node_builder = Some(node_builder);
        self
    }

    pub async fn build(self) -> anyhow::Result<ObsidianForTest> {
        if self.n_shards < 1 {
            return Err(anyhow!("need at least one shard"));
        }

        let nodes = TestNodes::new(self.node_builder.unwrap_or_else(|| {
            let storage = Arc::new(MemStorage::new());
            let journals =
                Arc::new(MemJournals::new()) as Arc<dyn Journals<Proposal<JournalEntry>>>;
            Box::new(InProcessNodeBuilder::new(storage, journals))
        }));

        log::info!("making nodes");

        for _ in 0..self.n_shards {
            nodes.create_node().await?;
        }

        log::info!("making nodes -> done");

        let meta = nodes.discovery().meta();

        log::info!("waiting for meta election");
        // Wait for a meta to get elected.
        Retry::new()
            .indefinitely(&async || meta.latest_snapshot().await)
            .await;
        log::info!("waiting for meta election -> done");

        let shard_ids: Vec<_> = (0..self.n_shards)
            .map(|i| ShardId((i + 1) as u32))
            .collect();

        for shard_id in &shard_ids {
            meta.add_shard(*shard_id).await?;
        }

        log::info!("waiting for shard assignment and leader election");
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
        log::info!("waiting for shard assignment and leader election -> done");

        let gateway = Gateway::new(
            Arc::clone(&meta),
            MetaSynced::new(Arc::clone(&meta)),
            nodes.shards(),
        );

        let meta_synced = Arc::new(MetaSynced::new(nodes.discovery().meta()));
        Ok(ObsidianForTest {
            gateway: Arc::new(gateway),
            meta,
            meta_synced,
            supervisor: nodes.discovery().supervisor(),
            nodes,
        })
    }
}

pub(crate) struct ObsidianForTest {
    pub gateway: Arc<dyn Obsidian>,
    pub meta: Arc<dyn runtime::Meta>,
    pub meta_synced: Arc<MetaSynced>,
    pub supervisor: Arc<dyn runtime::Supervisor>,
    pub nodes: TestNodes,
}

impl ObsidianForTest {
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
