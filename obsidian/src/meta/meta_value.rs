use std::fmt::Debug;

use anyhow::anyhow;
use prost::Message;

use crate::meta::MetaSync;
use crate::meta::ShardMetadata;
use crate::meta::TabletMetadata;
use crate::meta::TransferMetadata;
use crate::pb;

#[derive(Clone, Debug)]
pub(crate) enum MetaValue {
    Empty,
    MetaSync(MetaSync),
    ShardMetadata(ShardMetadata),
    TabletMetadata(TabletMetadata),
    TransferMetadata(TransferMetadata),
}

impl MetaValue {
    pub fn decode(buf: &[u8]) -> anyhow::Result<Self> {
        MetaValue::try_from(pb::internal::MetaValue::decode(buf)?)
    }

    pub fn encode(self) -> Vec<u8> {
        pb::internal::MetaValue::from(self).encode_to_vec()
    }
}

impl From<MetaValue> for pb::internal::MetaValue {
    fn from(value: MetaValue) -> pb::internal::MetaValue {
        pb::internal::MetaValue {
            value_type: Some(match value {
                MetaValue::Empty => pb::internal::meta_value::ValueType::Empty(()),
                MetaValue::MetaSync(meta_tx) => {
                    pb::internal::meta_value::ValueType::MetaSync(meta_tx.into())
                }
                MetaValue::ShardMetadata(shard_metadata) => {
                    pb::internal::meta_value::ValueType::ShardMetadata(shard_metadata.into())
                }
                MetaValue::TabletMetadata(tablet_metadata) => {
                    pb::internal::meta_value::ValueType::TabletMetadata(tablet_metadata.into())
                }
                MetaValue::TransferMetadata(transfer_metadata) => {
                    pb::internal::meta_value::ValueType::TransferMetadata(transfer_metadata.into())
                }
            }),
        }
    }
}

impl TryFrom<pb::internal::MetaValue> for MetaValue {
    type Error = anyhow::Error;

    fn try_from(value_pb: pb::internal::MetaValue) -> anyhow::Result<Self> {
        Ok(
            match value_pb
                .value_type
                .ok_or_else(|| anyhow!("missing value_type"))?
            {
                pb::internal::meta_value::ValueType::Empty(()) => MetaValue::Empty,
                pb::internal::meta_value::ValueType::MetaSync(meta_tx_pb) => {
                    MetaValue::MetaSync(MetaSync::try_from(meta_tx_pb)?)
                }
                pb::internal::meta_value::ValueType::ShardMetadata(shard_metadata_pb) => {
                    MetaValue::ShardMetadata(ShardMetadata::try_from(shard_metadata_pb)?)
                }
                pb::internal::meta_value::ValueType::TabletMetadata(tablet_metadata_pb) => {
                    MetaValue::TabletMetadata(TabletMetadata::try_from(tablet_metadata_pb)?)
                }
                pb::internal::meta_value::ValueType::TransferMetadata(tablet_metadata_pb) => {
                    MetaValue::TransferMetadata(TransferMetadata::try_from(tablet_metadata_pb)?)
                }
            },
        )
    }
}
