use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::anyhow;
use async_trait::async_trait;

use crate::election::Follower;
use crate::election::Leader;
use crate::election::Participant;
use crate::election::ParticipantState;
use crate::lsm::Manifest;
use crate::replica::shard_journal::ShardEntry;
use crate::replica::shard_journal::ShardJournal;
use crate::runtime;
use crate::runtime::Shard as _;
use crate::shard::Shard;
use crate::Bound;
use crate::Direction;
use crate::HistoryRange;
use crate::InternalError;
use crate::Key;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Precondition;
use crate::Range;
use crate::Record;
use crate::Revision;
use crate::ShardId;
use crate::TabletId;
use crate::Timestamp;
use crate::TxOutcome;
use crate::Txid;
use crate::WalSeq;

struct Replica {
    shard_id: ShardId,
    participant: Arc<Participant<ShardEntry, LeaderReplica, FollowerReplica>>,

    tablets: RwLock<HashMap<TabletId, Arc<dyn runtime::Tablet>>>,
}

struct ReplicaInner {
    shard_journal: ShardJournal,
}

struct LeaderReplica {
    inner: ReplicaInner,
    shard: Shard,
}

#[async_trait]
impl Leader<ShardEntry, FollowerReplica> for LeaderReplica {
    async fn process(&self, seq: WalSeq, entry: ShardEntry) {
        self.inner.shard_journal.process_entry(seq, entry);
    }

    async fn demote(self) -> FollowerReplica {
        todo!();
    }
}

struct FollowerReplica {
    inner: ReplicaInner,
}

#[async_trait]
impl Follower<ShardEntry, LeaderReplica> for FollowerReplica {
    async fn process(&self, seq: WalSeq, entry: ShardEntry) {
        self.inner.shard_journal.process_entry(seq, entry);
    }

    async fn promote(self) -> LeaderReplica {
        // XXX: Make sure the readers have actually seen everything.
        todo!();
    }
}

#[async_trait]
impl runtime::Shard for Replica {
    fn id(&self) -> ShardId {
        self.shard_id
    }

    fn tablet(&self, tablet_id: TabletId) -> anyhow::Result<Arc<dyn runtime::Tablet>> {
        {
            let tablets = self.tablets.read().unwrap();
            if let Some(tablet) = tablets.get(&tablet_id) {
                return Ok(Arc::clone(&tablet));
            }
        }

        {
            let mut tablets = self.tablets.write().unwrap();
            Ok(Arc::clone(tablets.entry(tablet_id).or_insert_with(|| {
                Arc::new(ReplicaTablet {
                    tablet_id: tablet_id,
                    participant: Arc::clone(&self.participant),
                }) as Arc<dyn runtime::Tablet>
            })))
        }
    }

    async fn wait_meta_sync(&self, ts: Timestamp) -> anyhow::Result<()> {
        todo!();
    }
}

struct ReplicaTablet {
    tablet_id: TabletId,
    participant: Arc<Participant<ShardEntry, LeaderReplica, FollowerReplica>>,
}

impl ReplicaTablet {
    async fn get_inner(&self, ts: Timestamp, key: Key) -> Result<Option<Record>, InternalError> {
        let tablet_id = self.tablet_id;
        self.participant
            .with_state(&async move |participant_state: ParticipantState<
                LeaderReplica,
                FollowerReplica,
            >| {
                if let ParticipantState::Leader(leader) = participant_state {
                    // TODO: Get rid of this clone.
                    let key2 = key.clone();
                    return leader.shard.tablet(tablet_id)?.get(ts, &key2).await;
                }
                Err(anyhow!("not currently leader").into())
            })
            .await
    }
}

#[async_trait]
impl runtime::Tablet for ReplicaTablet {
    async fn get(&self, ts: Timestamp, key: &Key) -> Result<Option<Record>, InternalError> {
        // TODO: Get rid of this clone.
        self.get_inner(ts, key.clone()).await
    }

    async fn get_latest(&self, key: Key) -> Result<(Timestamp, Option<Record>), InternalError> {
        todo!();
    }

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> Result<Timestamp, InternalError> {
        todo!();
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<Range<Vec<u8>>>), InternalError> {
        todo!();
    }

    async fn history_page(
        &self,
        key: Key,
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Revision>, Option<HistoryRange>), InternalError> {
        todo!();
    }

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        todo!();
    }

    async fn prepare(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        todo!();
    }

    async fn try_commit(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<TxOutcome> {
        todo!();
    }

    async fn try_abort(&self, txid: Txid) -> anyhow::Result<TxOutcome> {
        todo!();
    }

    async fn wait(&self, txid: Txid) -> Result<TxOutcome, InternalError> {
        todo!();
    }

    async fn cleanup_committed(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        todo!();
    }

    async fn wait_meta_sync(&self, ts: Timestamp) -> anyhow::Result<()> {
        todo!();
    }

    async fn manifest(&self) -> anyhow::Result<Manifest> {
        todo!();
    }

    async fn wait_mostly_hydrated(&self) -> anyhow::Result<()> {
        todo!();
    }

    async fn catchup(&self) -> anyhow::Result<()> {
        todo!();
    }

    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>> {
        todo!();
    }
}
