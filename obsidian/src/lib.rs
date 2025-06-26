#![allow(dead_code)]
#![feature(assert_matches)]
#![feature(coroutines)]

mod lock_mgr;
mod lsm;
mod lsm_block;
mod lsm_run;
mod lsm_util;
mod memtable;
mod meta;
mod meta_synced;
mod obsidian;
mod range;
mod router;
mod rtest;
mod sequencer;
mod storage;
mod tablet;
mod tuple_encoding;
mod types;
mod util;
mod wal;

mod pb {
    include!(concat!(env!("OUT_DIR"), "/obsidian.rs"));
}

#[cfg(test)]
mod test;
