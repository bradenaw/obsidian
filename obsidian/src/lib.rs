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
    mod obsidian {
        include!(concat!(env!("OUT_DIR"), "/obsidian.rs"));
    }
    pub(crate) mod internal {
        include!(concat!(env!("OUT_DIR"), "/obsidian_internal.rs"));
    }

    pub use obsidian::bound;
    pub use obsidian::Bound;
    pub use obsidian::KeyspaceId;
    pub use obsidian::Range;
}

#[cfg(test)]
mod test;
