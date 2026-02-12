use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::anyhow;
use arc_atomic::AtomicArc;
use async_trait::async_trait;

use crate::meta::MetaKey;
use crate::runtime::Meta;
use crate::Bound;
use crate::ColoGroupId;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Range;
use crate::Record;
use crate::Revision;
use crate::ShardId;
use crate::TabletId;
use crate::Timestamp;

pub(super) struct MetaProxy {
    inner: AtomicArc<Option<Arc<dyn Meta>>>,
}

impl MetaProxy {
    pub fn new() -> Self {
        Self {
            inner: AtomicArc::new(Arc::new(None)),
        }
    }

    pub fn put(&self, t: Arc<dyn Meta>) {
        self.inner.store(Arc::new(Some(t)))
    }
}

#[async_trait]
impl Meta for Arc<MetaProxy> {
    async fn add_shard(&self, shard_id: ShardId) -> anyhow::Result<()> {
        let inner = self.inner.load();
        if let Some(inner) = inner.deref() {
            return Meta::add_shard(inner, shard_id).await;
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
            return Meta::create_colo_group(inner, colo_group_id, initial_splits).await;
        }
        Err(anyhow!("MetaProxy not filled yet"))
    }

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        let inner = self.inner.load();
        if let Some(inner) = inner.deref() {
            return Meta::create_keyspace(inner, keyspace_id).await;
        }
        Err(anyhow!("MetaProxy not filled yet"))
    }

    async fn latest_snapshot(&self) -> anyhow::Result<Timestamp> {
        let inner = self.inner.load();
        if let Some(inner) = inner.deref() {
            return Meta::latest_snapshot(inner).await;
        }
        Err(anyhow!("MetaProxy not filled yet"))
    }

    async fn wait_for_newer(&self, ts: Timestamp) -> anyhow::Result<()> {
        let inner = self.inner.load();
        if let Some(inner) = inner.deref() {
            return Meta::wait_for_newer(inner, ts).await;
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
            return Meta::scan_page(inner, ts, range).await;
        }
        Err(anyhow!("MetaProxy not filled yet"))
    }

    async fn sync(&self, ts: Timestamp) -> anyhow::Result<(Vec<Revision>, Timestamp)> {
        let inner = self.inner.load();
        if let Some(inner) = inner.deref() {
            return Meta::sync(inner, ts).await;
        }
        Err(anyhow!("MetaProxy not filled yet"))
    }

    async fn tablet_ids(&self, ts: Timestamp) -> anyhow::Result<Vec<TabletId>> {
        let inner = self.inner.load();
        if let Some(inner) = inner.deref() {
            return Meta::tablet_ids(inner, ts).await;
        }
        Err(anyhow!("MetaProxy not filled yet"))
    }

    async fn write(
        &self,
        snapshot_ts: Timestamp,
        muts: HashMap<MetaKey, Mutation>,
    ) -> anyhow::Result<Timestamp> {
        let inner = self.inner.load();
        if let Some(inner) = inner.deref() {
            return Meta::write(inner, snapshot_ts, muts).await;
        }
        Err(anyhow!("MetaProxy not filled yet"))
    }
}
