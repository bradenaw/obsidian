#![allow(dead_code)]
#![feature(async_fn_traits)]
#![feature(coroutines)]
#![feature(unboxed_closures)]
#![feature(iter_from_coroutine)]
#![feature(thread_id_value)]

mod discovery;
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
mod storage;
mod supervisor;
mod tablet;
mod tuple_encoding;
mod util;

#[cfg(test)]
mod test;

pub(crate) use obsidian_common::Bound;
pub(crate) use obsidian_common::ColoGroupId;
pub(crate) use obsidian_common::Direction;
pub(crate) use obsidian_common::HistoryRange;
pub(crate) use obsidian_common::InternalError;
pub(crate) use obsidian_common::JournalEntry;
pub(crate) use obsidian_common::JournalSeq;
pub(crate) use obsidian_common::Key;
pub(crate) use obsidian_common::KeyOrBound;
pub(crate) use obsidian_common::KeyspaceId;
pub(crate) use obsidian_common::KeyspaceManifest;
pub(crate) use obsidian_common::LevelManifest;
pub(crate) use obsidian_common::Manifest;
pub(crate) use obsidian_common::Mutation;
pub(crate) use obsidian_common::NodeId;
pub(crate) use obsidian_common::Precondition;
pub(crate) use obsidian_common::Range;
pub(crate) use obsidian_common::RangeMap;
pub(crate) use obsidian_common::RangeSet;
pub(crate) use obsidian_common::Record;
pub(crate) use obsidian_common::Revision;
pub(crate) use obsidian_common::RevisionValue;
pub(crate) use obsidian_common::RunId;
pub(crate) use obsidian_common::RunManifest;
pub(crate) use obsidian_common::ShardId;
pub(crate) use obsidian_common::TabletId;
pub(crate) use obsidian_common::TabletJournalEntry;
pub(crate) use obsidian_common::Timestamp;
pub(crate) use obsidian_common::TransferId;
pub(crate) use obsidian_common::TxOutcome;
pub(crate) use obsidian_common::Txid;
pub(crate) use obsidian_common::WriteError;

pub(crate) use crate::obsidian::Obsidian;
pub(crate) use crate::obsidian::ObsidianExt;
