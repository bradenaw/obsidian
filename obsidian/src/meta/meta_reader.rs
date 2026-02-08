use anyhow::anyhow;
use async_trait::async_trait;
use futures::Stream;
use futures::TryStreamExt;

use crate::meta::MetaKey;
use crate::meta::MetaValue;
use crate::meta::TabletMetadata;
use crate::meta::TransferMetadata;
use crate::util::hexlify;
use crate::ColoGroupId;
use crate::Direction;
use crate::KeyspaceId;
use crate::Range;
use crate::ShardId;
use crate::TabletId;
use crate::TransferId;

#[async_trait]
pub(crate) trait MetaReader {
    async fn get_raw(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>>;

    fn scan_raw(
        &self,
        range: Range<Vec<u8>>,
        direction: Direction,
    ) -> Box<dyn Stream<Item = anyhow::Result<(Vec<u8>, Vec<u8>)>> + Unpin + Send + '_>;

    async fn get<V: MetaValue>(&self, meta_key: &MetaKey) -> anyhow::Result<Option<V>> {
        if let Some(value) = self.get_raw(&meta_key.encode()).await? {
            return Ok(Some(V::decode(&value[..])?));
        }
        Ok(None)
    }

    async fn exists(&self, meta_key: &MetaKey) -> anyhow::Result<bool> {
        Ok(self.get_raw(&meta_key.encode()).await?.is_some())
    }

    async fn empty(&self) -> anyhow::Result<bool> {
        Ok(!(self.exists(&MetaKey::Sync).await?))
    }

    async fn shard_ids(&self) -> anyhow::Result<Vec<ShardId>> {
        let mut out = vec![];
        let mut s = self.scan_raw(MetaKey::shards(), Direction::Asc);
        while let Some((key, _)) = s.try_next().await? {
            if let MetaKey::Shard(shard_id) = MetaKey::decode(&key[..])? {
                out.push(shard_id);
            } else {
                return Err(anyhow!("invalid shard key {}", hexlify(&key)));
            }
        }
        Ok(out)
    }

    async fn shard_exists(&self, shard_id: ShardId) -> anyhow::Result<bool> {
        self.exists(&MetaKey::Shard(shard_id)).await
    }

    async fn next_tablet_id(&self, shard_id: ShardId) -> anyhow::Result<TabletId> {
        let max_existing = self
            .shard_tablet_ids(shard_id)
            .await?
            .iter()
            .map(|tablet_id| tablet_id.1)
            .max()
            .unwrap_or(0);

        Ok(TabletId(shard_id, max_existing + 1))
    }

    async fn shard_tablet_ids(&self, shard_id: ShardId) -> anyhow::Result<Vec<TabletId>> {
        // TODO: Actually just scan the prefix.
        Ok(self
            .tablet_ids()
            .await?
            .into_iter()
            .filter(|tablet_id| tablet_id.0 == shard_id)
            .collect())
    }

    async fn colo_group_exists(&self, colo_group_id: ColoGroupId) -> anyhow::Result<bool> {
        self.exists(&MetaKey::ColoGroup(colo_group_id)).await
    }

    async fn tablet_ids(&self) -> anyhow::Result<Vec<TabletId>> {
        let mut out = vec![];
        let mut s = self.scan_raw(MetaKey::tablets(), Direction::Asc);
        while let Some((key, _)) = s.try_next().await? {
            if let MetaKey::Tablet(tablet_id) = MetaKey::decode(&key[..])? {
                out.push(tablet_id);
            } else {
                return Err(anyhow!("invalid tablet key {}", hexlify(&key)));
            }
        }
        Ok(out)
    }

    async fn keyspace_ids(&self) -> anyhow::Result<Vec<KeyspaceId>> {
        let mut out = vec![];
        let mut s = self.scan_raw(MetaKey::keyspaces(), Direction::Asc);
        while let Some((key, _)) = s.try_next().await? {
            if let MetaKey::Keyspace(keyspace_id) = MetaKey::decode(&key[..])? {
                out.push(keyspace_id);
            } else {
                return Err(anyhow!("invalid tablet key {}", hexlify(&key)));
            }
        }
        Ok(out)
    }

    async fn tablet_metadata(&self, tablet_id: TabletId) -> anyhow::Result<TabletMetadata>
    where
        Self: Sized,
    {
        self.get::<TabletMetadata>(&MetaKey::Tablet(tablet_id))
            .await?
            .ok_or_else(|| anyhow!("{:?} not found", tablet_id))
    }

    async fn transfer_metadata(&self, transfer_id: TransferId) -> anyhow::Result<TransferMetadata>
    where
        Self: Sized,
    {
        self.get::<TransferMetadata>(&MetaKey::Transfer(transfer_id))
            .await?
            .ok_or_else(|| anyhow!("{:?} not found", transfer_id))
    }
}
