use anyhow::anyhow;
use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;

use crate::meta::MetaKey;
use crate::meta::MetaValue;
use crate::meta::ShardMetadata;
use crate::meta::TabletMetadata;
use crate::meta::TransferMetadata;
use crate::ColoGroupId;
use crate::Direction;
use crate::KeyspaceId;
use crate::NodeId;
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

    async fn get(&self, meta_key: &MetaKey) -> anyhow::Result<Option<MetaValue>> {
        if let Some(value) = self.get_raw(&meta_key.encode()).await? {
            return Ok(Some(MetaValue::decode(&value[..])?));
        }
        Ok(None)
    }

    fn scan(
        &self,
        range: Range<Vec<u8>>,
        direction: Direction,
    ) -> Box<dyn Stream<Item = anyhow::Result<(MetaKey, MetaValue)>> + Unpin + Send + '_> {
        Box::new(self.scan_raw(range, direction).map(|result| {
            result.and_then(|(k, v)| Ok((MetaKey::decode(&k)?, MetaValue::decode(&v)?)))
        }))
    }

    fn scan_keys(
        &self,
        range: Range<Vec<u8>>,
        direction: Direction,
    ) -> Box<dyn Stream<Item = anyhow::Result<MetaKey>> + Unpin + Send + '_> {
        Box::new(
            self.scan_raw(range, direction)
                .map(|result| result.and_then(|(k, _)| MetaKey::decode(&k))),
        )
    }

    async fn exists(&self, meta_key: &MetaKey) -> anyhow::Result<bool> {
        Ok(self.get_raw(&meta_key.encode()).await?.is_some())
    }

    async fn empty(&self) -> anyhow::Result<bool> {
        Ok(!(self.exists(&MetaKey::Sync).await?))
    }

    async fn shard_ids(&self) -> anyhow::Result<Vec<ShardId>> {
        let mut out = vec![];
        let mut s = self.scan_keys(MetaKey::shards(), Direction::Asc);
        while let Some(key) = s.try_next().await? {
            if let MetaKey::Shard(shard_id) = key {
                out.push(shard_id);
            } else {
                return Err(anyhow!("invalid shard key {:?}", key));
            }
        }
        Ok(out)
    }

    async fn shard_metadata(&self, shard_id: ShardId) -> anyhow::Result<ShardMetadata> {
        match self
            .get(&MetaKey::Shard(shard_id))
            .await?
            .ok_or_else(|| anyhow!("{:?} not found", shard_id))?
        {
            MetaValue::ShardMetadata(shard_metadata) => Ok(shard_metadata),
            other => Err(anyhow!(
                "unexpected type for {:?} metadata: {:?}",
                shard_id,
                other
            )),
        }
    }

    async fn node_exists(&self, node_id: NodeId) -> anyhow::Result<bool> {
        self.exists(&MetaKey::Node(node_id)).await
    }

    fn node_ids(&self) -> Box<dyn Stream<Item = anyhow::Result<NodeId>> + Unpin + Send + '_> {
        Box::new(
            self.scan(MetaKey::nodes(), Direction::Asc)
                .map(|result| match result {
                    Ok((MetaKey::Node(node_id), _)) => Ok(node_id),
                    Ok((meta_key, _)) => Err(anyhow!(
                        "unexpected meta key {:?}: expected MetaKey::Node",
                        meta_key
                    )),
                    Err(e) => Err(e),
                }),
        )
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
        let mut s = self.scan_keys(MetaKey::tablets(), Direction::Asc);
        while let Some(key) = s.try_next().await? {
            if let MetaKey::Tablet(tablet_id) = key {
                out.push(tablet_id);
            } else {
                return Err(anyhow!("invalid tablet key {:?}", key));
            }
        }
        Ok(out)
    }

    async fn keyspace_ids(&self) -> anyhow::Result<Vec<KeyspaceId>> {
        let mut out = vec![];
        let mut s = self.scan_keys(MetaKey::keyspaces(), Direction::Asc);
        while let Some(key) = s.try_next().await? {
            if let MetaKey::Keyspace(keyspace_id) = key {
                out.push(keyspace_id);
            } else {
                return Err(anyhow!("invalid tablet key {:?}", key));
            }
        }
        Ok(out)
    }

    async fn tablet_metadata(&self, tablet_id: TabletId) -> anyhow::Result<TabletMetadata>
    where
        Self: Sized,
    {
        match self
            .get(&MetaKey::Tablet(tablet_id))
            .await?
            .ok_or_else(|| anyhow!("{:?} not found", tablet_id))?
        {
            MetaValue::TabletMetadata(tablet_metadata) => Ok(tablet_metadata),
            other => Err(anyhow!(
                "unexpected type for {:?} metadata: {:?}",
                tablet_id,
                other
            )),
        }
    }

    async fn transfer_metadata(&self, transfer_id: TransferId) -> anyhow::Result<TransferMetadata>
    where
        Self: Sized,
    {
        match self
            .get(&MetaKey::Transfer(transfer_id))
            .await?
            .ok_or_else(|| anyhow!("{:?} not found", transfer_id))?
        {
            MetaValue::TransferMetadata(transfer_metadata) => Ok(transfer_metadata),
            other => Err(anyhow!(
                "unexpected type for {:?} metadata: {:?}",
                transfer_id,
                other
            )),
        }
    }

    fn transfers(
        &self,
    ) -> Box<dyn Stream<Item = anyhow::Result<(TransferId, TransferMetadata)>> + Unpin + Send + '_>
    {
        Box::new(
            self.scan(MetaKey::transfers(), Direction::Asc)
                .map(|result| match result {
                    Ok((
                        MetaKey::Transfer(transfer_id),
                        MetaValue::TransferMetadata(transfer_metadata),
                    )) => Ok((transfer_id, transfer_metadata)),
                    Ok((meta_key, meta_value)) => Err(anyhow!(
                        "wrong type for meta key/value, expected transfer: {:?} {:?}",
                        meta_key,
                        meta_value,
                    )),
                    Err(e) => Err(e),
                }),
        )
    }
}
