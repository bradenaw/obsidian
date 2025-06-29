#![allow(dead_code)]
#![feature(assert_matches)]
#![feature(coroutines)]

mod grpc;
mod lsm;
mod meta;
mod meta_synced;
mod obsidian;
mod range;
mod router;
mod rtest;
mod storage;
mod tablet;
mod tuple_encoding;
mod types;
mod util;
mod wal;

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
