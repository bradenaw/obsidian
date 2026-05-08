//! obsidian_external holds the interfaces and implementations for Obsidian's external
//! dependencies: blob storage and journals.

mod file_reader;
mod file_writer;
mod journal;
mod journals;
pub mod mem;
mod storage;

pub use file_reader::FileReader;
pub use file_writer::FileWriter;
pub use journal::Journal;
pub use journals::Journals;
pub use storage::FileName;
pub use storage::Storage;
