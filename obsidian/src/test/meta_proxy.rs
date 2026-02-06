use std::ops::Deref;
use std::sync::Arc;

use anyhow::anyhow;
use arc_atomic::AtomicArc;
use async_trait::async_trait;

use crate::NodeId;
use crate::runtime::Meta;
use crate::Bound;
use crate::ColoGroupId;
use crate::KeyspaceId;
use crate::Range;
use crate::Record;
use crate::Revision;
use crate::ShardId;
use crate::TabletId;
use crate::Timestamp;

pub(super) struct MetaProxy<T> {
    inner: AtomicArc<Option<T>>,
}

impl<T> MetaProxy<T> {
    pub fn new() -> Self {
        Self {
            inner: AtomicArc::new(Arc::new(None)),
        }
    }

    pub fn put(&self, t: T) {
        self.inner.store(Arc::new(Some(t)))
    }
}

#[async_trait]
impl<T: Meta> Meta for Arc<MetaProxy<T>> {
    async fn add_shard(&self, shard_id: ShardId) -> anyhow::Result<()> {
        let inner = self.inner.load();
        if let Some(inner) = inner.deref() {
            return T::add_shard(inner, shard_id).await;
        }
        Err(anyhow!("MetaProxy not filled yet"))
    }

        async fn add_node(&self, node_id: NodeId) -> anyhow::Result<()> {
        let inner = self.inner.load();
        if let Some(inner) = inner.deref() {
            return T::add_node(inner, node_id).await;
        }
        Err(anyhow!("MetaProxy not filled yet"))
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
