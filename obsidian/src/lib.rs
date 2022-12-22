#![allow(dead_code)]
#![feature(generators)]
#![feature(is_sorted)]
#![feature(map_first_last)]
#![feature(build_hasher_simple_hash_one)]

mod lock_mgr;
mod lsm;
mod memtable;
mod meta;
mod obsidian;
mod range;
mod sequencer;
mod storage;
mod tablet;
mod types;
mod util;
mod wal;
