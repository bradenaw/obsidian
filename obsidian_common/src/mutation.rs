use std::fmt::Debug;

use anyhow::anyhow;
use obsidian_pb as pb;

#[derive(Clone, Debug)]
pub enum Mutation {
    Put(Vec<u8>),
    Delete,
}

impl Mutation {
    pub fn len(&self) -> usize {
        match self {
            Mutation::Put(v) => v.len(),
            Mutation::Delete => 0,
        }
    }
}

impl TryFrom<pb::Mutation> for Mutation {
    type Error = anyhow::Error;

    fn try_from(value: pb::Mutation) -> Result<Self, Self::Error> {
        Ok(match value.mutation_type {
            Some(pb::mutation::MutationType::Put(value)) => Mutation::Put(value),
            Some(pb::mutation::MutationType::Delete(())) => Mutation::Delete,
            None => return Err(anyhow!("missing mutation_type")),
        })
    }
}
impl From<Mutation> for pb::Mutation {
    fn from(value: Mutation) -> Self {
        pb::Mutation {
            mutation_type: Some(match value {
                Mutation::Put(value) => pb::mutation::MutationType::Put(value),
                Mutation::Delete => pb::mutation::MutationType::Delete(()),
            }),
        }
    }
}
