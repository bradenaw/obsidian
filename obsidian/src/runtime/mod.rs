//! The runtime module's purpose is to abstract away all of the parts of the system that do not run
//! inside this process in a real environment, but tests can use these traits to do something
//! different.
//!
//! For example, the Tablet trait is what the real tablet implements, and also what all code
//! talking to a tablet uses. In reality, the caller will have a network client and the server will
//! be wrapped in a network server. In some tests, these two parts can just talk directly to each
//! other without any network in the middle, which simplifies debugging because callstacks get
//! preserved and also speeds up the tests.
//!
//! Some of the traits here are also third-party dependencies that are expected to be provided for
//! the system, like Storage.

mod journal;
mod journals;
mod meta;
mod node;
mod nodes;
mod shard;
mod shards;
mod storage;
mod supervisor;
mod tablet;

pub(crate) use journal::Journal;
pub(crate) use journals::Journals;
pub(crate) use meta::Meta;
pub(crate) use node::Node;
pub(crate) use node::ReplicaState;
pub(crate) use nodes::Nodes;
pub(crate) use shard::Shard;
pub(crate) use shards::Shards;
pub(crate) use storage::FileReader;
pub(crate) use storage::FileWriter;
pub(crate) use storage::Storage;
pub(crate) use supervisor::Supervisor;
pub(crate) use tablet::Tablet;
