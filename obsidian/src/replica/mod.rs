//! Replica is a node holding a logical shard. Of the replicas for a shard, one is elected the
//! leader, which serves writes and latest reads, along with snapshot reads. The others are
//! follower replicas, which are capable only of serving snapshot reads because their
//! representation of the shard is a little behind the leader's.

mod recovery;
mod replica;

pub(crate) use replica::Replica;
