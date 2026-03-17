#![allow(dead_code)]
#![feature(coroutines)]
#![feature(iter_from_coroutine)]
#![feature(thread_id_value)]

mod election;
mod gateway;
mod grpc;
mod lsm;
mod meta;
mod node;
mod obsidian;
mod replica;
mod router;
mod rtest;
mod runtime;
mod shard;
mod shards;
mod storage;
mod supervisor;
mod tablet;
mod tuple_encoding;
mod types;
mod util;

mod pb {
    mod obsidian {
        tonic::include_proto!("obsidian");
    }
    pub(crate) mod internal {
        tonic::include_proto!("obsidian_internal");
    }

    pub use crate::pb::obsidian::bound;
    pub use crate::pb::obsidian::get_result;
    pub use crate::pb::obsidian::mutation;
    pub use crate::pb::obsidian::obsidian_client;
    pub use crate::pb::obsidian::obsidian_server;
    pub use crate::pb::obsidian::precondition;
    pub use crate::pb::obsidian::Bound;
    pub use crate::pb::obsidian::CreateColoGroupReq;
    pub use crate::pb::obsidian::CreateKeyspaceReq;
    pub use crate::pb::obsidian::Direction;
    pub use crate::pb::obsidian::GetLatestReq;
    pub use crate::pb::obsidian::GetLatestResp;
    pub use crate::pb::obsidian::GetReq;
    pub use crate::pb::obsidian::GetResp;
    pub use crate::pb::obsidian::GetResult;
    pub use crate::pb::obsidian::Key;
    pub use crate::pb::obsidian::KeyspaceId;
    pub use crate::pb::obsidian::Mutation;
    pub use crate::pb::obsidian::Precondition;
    pub use crate::pb::obsidian::Range;
    pub use crate::pb::obsidian::Record;
    pub use crate::pb::obsidian::ScanReq;
    pub use crate::pb::obsidian::ScanResp;
    pub use crate::pb::obsidian::WriteReq;
    pub use crate::pb::obsidian::WriteResp;
}

#[cfg(test)]
mod test;

pub(crate) use crate::obsidian::Obsidian;
pub(crate) use crate::obsidian::ObsidianExt;
pub(crate) use crate::types::Bound;
pub(crate) use crate::types::ColoGroupId;
pub(crate) use crate::types::Direction;
pub(crate) use crate::types::HistoryRange;
pub(crate) use crate::types::InternalError;
pub(crate) use crate::types::JournalEntry;
pub(crate) use crate::types::JournalSeq;
pub(crate) use crate::types::Key;
pub(crate) use crate::types::KeyOrBound;
pub(crate) use crate::types::KeyspaceId;
pub(crate) use crate::types::Mutation;
pub(crate) use crate::types::NodeId;
pub(crate) use crate::types::Precondition;
pub(crate) use crate::types::Range;
pub(crate) use crate::types::RangeMap;
pub(crate) use crate::types::RangeSet;
pub(crate) use crate::types::Record;
pub(crate) use crate::types::Revision;
pub(crate) use crate::types::RevisionValue;
pub(crate) use crate::types::ShardId;
pub(crate) use crate::types::TabletId;
pub(crate) use crate::types::TabletJournalEntry;
pub(crate) use crate::types::Timestamp;
pub(crate) use crate::types::TransferId;
pub(crate) use crate::types::TxOutcome;
pub(crate) use crate::types::Txid;
pub(crate) use crate::types::WriteError;
