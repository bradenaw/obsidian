use std::collections::BTreeSet;
use std::collections::HashSet;

use anyhow::anyhow;

use crate::meta::MetaKey;
use crate::pb;
use crate::KeyspaceId;

#[derive(Clone, Debug)]
pub(crate) struct MetaTx {
    pub keys: HashSet<MetaKey>,
}

impl TryFrom<pb::internal::MetaTx> for MetaTx {
    type Error = anyhow::Error;

    fn try_from(value_pb: pb::internal::MetaTx) -> Result<Self, Self::Error> {
        Ok(Self {
            keys: BTreeSet::try_from(
                value_pb
                    .keys
                    .ok_or_else(|| anyhow!("MetaTx missing field keys"))?,
            )?
            .into_iter()
            .map(|(_, key)| MetaKey::decode(&key))
            .collect::<anyhow::Result<HashSet<_>>>()?,
        })
    }
}

impl From<MetaTx> for pb::internal::MetaTx {
    fn from(value: MetaTx) -> Self {
        Self {
            keys: Some(pb::internal::CompressedKeySet::from(
                value
                    .keys
                    .iter()
                    .map(|meta_key| (KeyspaceId::META, meta_key.encode()))
                    .collect::<BTreeSet<_>>(),
            )),
        }
    }
}
