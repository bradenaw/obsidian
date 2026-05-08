//! LSM is the log-structured merge tree.

#![feature(thread_id_value)]

mod compactor;
mod index;
mod lsm;
mod memtable;
mod preload;
mod run;
#[cfg(test)]
mod tests;

pub use crate::lsm::Lsm;
pub use crate::lsm::LsmOptions;
pub use crate::preload::Preloaded;
pub use crate::preload::Preloader;
