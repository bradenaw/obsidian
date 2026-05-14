//! Tablets hold onto a key range. They are homed to a single shard, so rebalancing is done by
//! transferring the key range to a new tablet and disposing of the old tablet.
//!
//! There are two special kinds of tablets in addition to the data tablets that hold userland data.
//! The [`MetaTablet`] is on the Meta shard and permanently holds the meta colo group.
//! A [`ShardMetaTablet`] exists on every shard and holds onto the range of keys in the shard meta
//! colo group that begin with the shard's ID.

mod active_tablet;
mod data_tablet;
mod frozen_tablet;
mod hydrating_tablet;
mod journaled_lsm;
mod key_locks;
mod meta_tablet;
mod read_only_lsm;
mod scan_locks;
mod sequencer;
mod shard_meta_tablet;
mod tablet_inner;
mod tablet_journal_writer;
mod tests;

pub(crate) use data_tablet::DataTablet;
pub(crate) use meta_tablet::MetaTablet;
pub(crate) use shard_meta_tablet::ShardMetaTablet;
pub(crate) use tablet_journal_writer::TabletJournalWriter;
