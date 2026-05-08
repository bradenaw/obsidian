use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::sync::Arc;

use async_trait::async_trait;

use crate::election::Proposal;
use crate::meta::MetaSynced;
use crate::runtime;
use obsidian_external::Journals;
use obsidian_external::Storage;
use crate::storage::CachedStorage;
use crate::test::TestNodeBuilder;
use crate::JournalEntry;
use crate::NodeId;

pub(crate) struct InProcessNodeBuilder {
    storage: Arc<dyn Storage>,
    journals: Arc<dyn Journals<Proposal<JournalEntry>>>,
}

impl InProcessNodeBuilder {
    pub fn new(
        storage: Arc<dyn Storage>,
        journals: Arc<dyn Journals<Proposal<JournalEntry>>>,
    ) -> Self {
        Self { storage, journals }
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
        let node_id = NodeId::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 1);

        let cached_storage = Arc::new(CachedStorage::new(
            Arc::clone(&self.storage),
            64, // page_size
            4,  // stripe_size_pages
            4,  // n_stripes
        ));

        let meta_synced = Arc::new(MetaSynced::new(Arc::clone(&meta)));

        Ok(Arc::new(crate::node::Node::new(
            node_id,
            nodes,
            cached_storage,
            meta,
            shards,
            meta_synced,
            Arc::clone(&self.journals),
        )) as Arc<dyn runtime::Node>)
    }
}
