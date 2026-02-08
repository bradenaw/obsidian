use std::sync::Arc;

use async_trait::async_trait;

use crate::meta::MetaSynced;
use crate::meta::MetaSyncedSnapshot;
use crate::meta::MetaWatcher;
use crate::meta::SyncType;
use crate::runtime::Meta;
use crate::util::Retry;
use crate::util::WithBackground;
use crate::NodeId;

pub(crate) struct Node(WithBackground<NodeInner>);

struct NodeInner {
    node_id: NodeId,
    meta: Arc<dyn Meta>,
}

impl Node {
    pub async fn new(
        node_id: NodeId,
        meta: Arc<dyn Meta>,
        meta_synced: Arc<MetaSynced>,
    ) -> anyhow::Result<Self> {
        meta.add_node(node_id.clone()).await?;

        let inner = Arc::new(NodeInner { node_id, meta });
        let node = Node(WithBackground::new(Arc::clone(&inner)));

        meta_synced.subscribe2(&node.0);

        Ok(node)
    }
}

#[async_trait]
impl MetaWatcher for NodeInner {
    async fn sync_meta(&self, sync_type: SyncType, snapshot: MetaSyncedSnapshot) {
        Retry::new()
            .indefinitely(&async || -> anyhow::Result<()> {
                self.try_sync_meta(sync_type.clone(), snapshot.clone())
                    .await
            })
            .await;
    }
}

impl NodeInner {
    async fn try_sync_meta(
        &self,
        sync_type: SyncType,
        snapshot: MetaSyncedSnapshot,
    ) -> anyhow::Result<()> {
        todo!();
    }
}
