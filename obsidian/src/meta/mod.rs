//! Meta holds the global metadata that Obsidian stores about itself: routing, keyspace metadata,
//! tablet state, etc.
//!
//! This information is needed by every node in the system, so it also allows syncing and holding
//! in-memory copies.
//!
//! Meta resides in KeyspaceId::META, which is hardcoded to be owned by TabletId::META.

mod meta;
mod meta_key;
mod meta_mutation;
mod meta_reader;
mod meta_state;
mod meta_subscriber;
mod meta_sync;
mod meta_synced;
mod meta_value;
mod shard_metadata;
mod tablet_metadata;
mod tablet_state;
mod transfer_metadata;
mod transfer_state;

pub(crate) use meta::Meta;
#[allow(unused_imports)]
pub(crate) use meta::MetaSnapshot;
pub(crate) use meta_key::MetaKey;
pub(crate) use meta_mutation::MetaMutation;
pub(crate) use meta_reader::MetaReader;
pub(crate) use meta_state::MetaState;
pub(crate) use meta_subscriber::MetaSubscriber;
pub(crate) use meta_sync::MetaSync;
pub(crate) use meta_synced::MetaSynced;
pub(crate) use meta_synced::MetaSyncedSnapshot;
pub(crate) use meta_synced::SyncType;
pub(crate) use meta_value::MetaValue;
pub(crate) use shard_metadata::ShardMetadata;
pub(crate) use tablet_metadata::TabletMetadata;
pub(crate) use tablet_state::TabletState;
pub(crate) use transfer_metadata::TransferMetadata;
pub(crate) use transfer_state::TransferState;
