use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use obsidian_external::Journals;
use obsidian_external::Storage;
use obsidian_lsm::LsmOptions;

use crate::election::Proposal;
use crate::meta::MetaSynced;
use crate::runtime;
use crate::storage::CachedStorage;
use crate::test::TestNodeBuilder;
use crate::JournalEntry;
use crate::NodeId;

pub(crate) struct InProcessNodeBuilder {
    storage: Arc<dyn Storage>,
    journals: Arc<dyn Journals<Proposal<JournalEntry>>>,
    next_fake_port: AtomicU64,
}

impl InProcessNodeBuilder {
    pub fn new(
        storage: Arc<dyn Storage>,
        journals: Arc<dyn Journals<Proposal<JournalEntry>>>,
    ) -> Self {
        Self {
            storage,
            journals,
            next_fake_port: AtomicU64::new(1),
        }
    }
}

#[async_trait]
impl TestNodeBuilder for InProcessNodeBuilder {
    async fn build(
        &self,
        nodes: Arc<dyn runtime::Nodes>,
        meta: Arc<dyn runtime::Meta>,
        shards: Arc<dyn runtime::Shards>,
    ) -> anyhow::Result<Arc<dyn runtime::Node>> {
        let node_id = NodeId::new(
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            // Just to make these a little easier to pick out in logs than reading the UUIDs.
            self.next_fake_port.fetch_add(1, Ordering::SeqCst) as u16,
        );

        let cached_storage = Arc::new(CachedStorage::new(
            Arc::clone(&self.storage),
            64, // page_size
            4,  // stripe_size_pages
            4,  // n_stripes
        ));

        let meta_synced = Arc::new(MetaSynced::new(Arc::clone(&meta)));

        Ok(Arc::new(crate::node::Node::new(
            node_id,
            LsmOptions {
                l0_max_size: 256,
                l1_max_size: 100_000,
                run_size_target: 32768,
                block_size_target: 4096,
            },
            nodes,
            cached_storage,
            meta,
            shards,
            meta_synced,
            Arc::clone(&self.journals),
        )) as Arc<dyn runtime::Node>)
    }
}
