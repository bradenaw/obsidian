//! obsidian_external holds the interfaces and implementations for Obsidian's external
//! dependencies: blob storage and journals.

mod consul_node_discovery;
mod file_reader;
mod file_writer;
mod journal;
mod journals;
pub mod mem;
mod node_discovery;
mod s3_storage;
mod storage;

pub use consul_node_discovery::ConsulNodeDiscovery;
pub use file_reader::FileReader;
pub use file_writer::FileWriter;
pub use journal::Journal;
pub use journals::Journals;
pub use node_discovery::NodeDiscovery;
pub use s3_storage::S3Storage;
pub use storage::FileName;
pub use storage::Storage;
