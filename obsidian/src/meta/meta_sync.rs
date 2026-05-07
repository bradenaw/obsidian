use std::collections::BTreeSet;
use std::collections::HashSet;

use anyhow::anyhow;
use obsidian_pb as pb;

use crate::meta::MetaKey;
use crate::util::key_set_from_proto;
use crate::util::key_set_to_proto;
use crate::KeyspaceId;

#[derive(Clone, Debug)]
pub(crate) struct MetaSync {
    pub keys: HashSet<MetaKey>,
}

impl TryFrom<pb::internal::MetaSync> for MetaSync {
    type Error = anyhow::Error;

    fn try_from(value_pb: pb::internal::MetaSync) -> Result<Self, Self::Error> {
        Ok(Self {
            keys: key_set_from_proto(
                value_pb
                    .keys
                    .ok_or_else(|| anyhow!("MetaSync missing field keys"))?,
            )?
            .into_iter()
            .map(|(_, key)| MetaKey::decode(&key))
            .collect::<anyhow::Result<HashSet<_>>>()?,
        })
    }
}

impl From<MetaSync> for pb::internal::MetaSync {
    fn from(value: MetaSync) -> Self {
        Self {
            keys: Some(key_set_to_proto(
                value
                    .keys
                    .iter()
                    .map(|meta_key| (KeyspaceId::META, meta_key.encode()))
                    .collect::<BTreeSet<_>>(),
            )),
        }
    }
}
