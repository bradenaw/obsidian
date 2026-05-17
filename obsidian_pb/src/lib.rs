//! obsidian_pb holds the compiled protocol buffers from `proto/`.

mod obsidian {
    tonic::include_proto!("obsidian");
}
pub mod external {
    tonic::include_proto!("obsidian_external");
}
pub mod internal {
    tonic::include_proto!("obsidian_internal");
}

pub use crate::obsidian::bound;
pub use crate::obsidian::get_result;
pub use crate::obsidian::mutation;
pub use crate::obsidian::obsidian_client;
pub use crate::obsidian::obsidian_server;
pub use crate::obsidian::precondition;
pub use crate::obsidian::revision_value;
pub use crate::obsidian::Bound;
pub use crate::obsidian::CreateColoGroupReq;
pub use crate::obsidian::CreateKeyspaceReq;
pub use crate::obsidian::Direction;
pub use crate::obsidian::GetLatestReq;
pub use crate::obsidian::GetLatestResp;
pub use crate::obsidian::GetReq;
pub use crate::obsidian::GetResp;
pub use crate::obsidian::GetResult;
pub use crate::obsidian::Key;
pub use crate::obsidian::KeyMutation;
pub use crate::obsidian::KeyspaceId;
pub use crate::obsidian::LatestSnapshotResp;
pub use crate::obsidian::Mutation;
pub use crate::obsidian::Precondition;
pub use crate::obsidian::Range;
pub use crate::obsidian::Record;
pub use crate::obsidian::Revision;
pub use crate::obsidian::RevisionValue;
pub use crate::obsidian::ScanReq;
pub use crate::obsidian::ScanResp;
pub use crate::obsidian::WriteReq;
pub use crate::obsidian::WriteResp;
