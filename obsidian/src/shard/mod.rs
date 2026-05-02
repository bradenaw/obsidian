//! A shard is the unit of replication and routing. Key ranges are assigned to tablets, which are
//! assigned to shards, which are in turn assigned to nodes.

mod shard;

pub(crate) use shard::Shard;
pub(crate) use shard::ShardJournalWriter;
