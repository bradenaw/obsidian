use obsidian_pb as pb;
use prost::Message;

use crate::meta::MetaValue;
use crate::Mutation;

#[derive(Debug, Clone)]
pub(crate) enum MetaMutation {
    Put(MetaValue),
    Delete,
}

impl MetaMutation {
    pub(super) fn into_mutation(self) -> Mutation {
        match self {
            MetaMutation::Put(meta_value) => {
                Mutation::Put(pb::internal::MetaValue::from(meta_value).encode_to_vec())
            }
            MetaMutation::Delete => Mutation::Delete,
        }
    }
}
