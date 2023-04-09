use async_trait::async_trait;

use crate::tablet::Tablet;
use crate::types::ColoGroupId;
use crate::types::KeyspaceId;

#[async_trait]
pub(crate) trait Meta {
    async fn create_colo_group(&self, splits: Vec<Vec<u8>>) -> anyhow::Result<ColoGroupId>;
    async fn create_keyspace(&self, colo_group_id: ColoGroupId) -> anyhow::Result<KeyspaceId>;
}

struct MetaImpl<T> {
    tablet: T,
}

#[async_trait]
impl<T: Tablet + Sync> Meta for MetaImpl<T> {
    async fn create_colo_group(&self, splits: Vec<Vec<u8>>) -> anyhow::Result<ColoGroupId> {
        todo!();
    }

    async fn create_keyspace(&self, colo_group_id: ColoGroupId) -> anyhow::Result<KeyspaceId> {
        todo!();
    }
}

impl<T: Tablet> MetaImpl<T> {
    fn new(tablet: T) -> Self {
        Self { tablet }
    }
}
