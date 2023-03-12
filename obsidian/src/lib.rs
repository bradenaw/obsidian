#![allow(dead_code)]
#![feature(assert_matches)]
#![feature(build_hasher_simple_hash_one)]
#![feature(generators)]
#![feature(is_sorted)]
#![feature(map_first_last)]

mod lock_mgr;
mod lsm;
mod lsm_block;
mod lsm_run;
mod lsm_util;
mod memtable;
mod obsidian;
mod range;
mod router;
mod sequencer;
mod storage;
mod tablet;
mod types;
mod util;
mod wal;
