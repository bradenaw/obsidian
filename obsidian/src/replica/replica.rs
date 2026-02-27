use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

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
    async fn get_inner(&self, ts: Timestamp, key: &Key) -> Result<Option<Record>, InternalError> {
        let tablet_id = self.tablet_id;
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader.shard.tablet(tablet_id)?.get(ts, key).await;
                }
                Err(InternalError::NotLeader(tablet_id))
            })
            .await
    }

    async fn if_leader<F, T>(&self, f: F) -> Result<T, InternalError>
    where
        F: AsyncFnOnce(Arc<dyn runtime::Tablet>) -> Result<T, InternalError>,
        T: Send + 'static,
    {
        let tablet_id = self.tablet_id;
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    let tablet = leader.shard.tablet(tablet_id)?;
                    return f(tablet).await;
                }
                Err(InternalError::NotLeader(tablet_id))
            })
            .await
    }

    async fn get_latest_inner(
        &self,
        key: Key,
    ) -> Result<(Timestamp, Option<Record>), InternalError> {
        self.if_leader(async move |tablet: Arc<dyn runtime::Tablet>| tablet.get_latest(key).await)
            .await
    }
}

#[async_trait]
impl runtime::Tablet for ReplicaTablet {
    async fn get(&self, ts: Timestamp, key: &Key) -> Result<Option<Record>, InternalError> {
        self.get_inner(ts, key).await
    }

    async fn get_latest(&self, key: Key) -> Result<(Timestamp, Option<Record>), InternalError> {
        let tablet_id = self.tablet_id;
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader.shard.tablet(tablet_id)?.get_latest(key).await;
                }
                Err(InternalError::NotLeader(tablet_id))
            })
            .await
    }

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> Result<Timestamp, InternalError> {
        let tablet_id = self.tablet_id;
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader.shard.tablet(tablet_id)?.latest_snapshot(keys).await;
                }
                Err(InternalError::NotLeader(tablet_id))
            })
            .await
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<Range<Vec<u8>>>), InternalError> {
        let tablet_id = self.tablet_id;
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader
                        .shard
                        .tablet(tablet_id)?
                        .scan_page(ts, keyspace_id, range, direction, limit)
                        .await;
                }
                Err(InternalError::NotLeader(tablet_id))
            })
            .await
    }

    async fn history_page(
        &self,
        key: Key,
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Revision>, Option<HistoryRange>), InternalError> {
        let tablet_id = self.tablet_id;
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader
                        .shard
                        .tablet(tablet_id)?
                        .history_page(key, range, direction, limit)
                        .await;
                }
                Err(InternalError::NotLeader(tablet_id))
            })
            .await
    }

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        let tablet_id = self.tablet_id;
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader.shard.tablet(tablet_id)?.write(preconds, muts).await;
                }
                Err(InternalError::NotLeader(tablet_id))
            })
            .await
    }

    async fn prepare(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        let tablet_id = self.tablet_id;
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader
                        .shard
                        .tablet(tablet_id)?
                        .prepare(txid, preconds, muts)
                        .await;
                }
                Err(InternalError::NotLeader(tablet_id))
            })
            .await
    }

    async fn try_commit(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<TxOutcome> {
        let tablet_id = self.tablet_id;
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader
                        .shard
                        .tablet(tablet_id)?
                        .try_commit(txid, ts, precond_keys, mut_keys)
                        .await;
                }
                Err(InternalError::NotLeader(tablet_id).into())
            })
            .await
    }

    async fn try_abort(&self, txid: Txid) -> anyhow::Result<TxOutcome> {
        let tablet_id = self.tablet_id;
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader.shard.tablet(tablet_id)?.try_abort(txid).await;
                }
                Err(InternalError::NotLeader(tablet_id).into())
            })
            .await
    }

    async fn wait(&self, txid: Txid) -> Result<TxOutcome, InternalError> {
        let tablet_id = self.tablet_id;
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader.shard.tablet(tablet_id)?.wait(txid).await;
                }
                Err(InternalError::NotLeader(tablet_id).into())
            })
            .await
    }

    async fn cleanup_committed(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        let tablet_id = self.tablet_id;
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader
                        .shard
                        .tablet(tablet_id)?
                        .cleanup_committed(txid, ts, precond_keys, mut_keys)
                        .await;
                }
                Err(InternalError::NotLeader(tablet_id).into())
            })
            .await
    }

    async fn wait_meta_sync(&self, ts: Timestamp) -> anyhow::Result<()> {
        let tablet_id = self.tablet_id;
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader.shard.tablet(tablet_id)?.wait_meta_sync(ts).await;
                }
                Err(InternalError::NotLeader(tablet_id).into())
            })
            .await
    }

    async fn manifest(&self) -> anyhow::Result<Manifest> {
        let tablet_id = self.tablet_id;
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader.shard.tablet(tablet_id)?.manifest().await;
                }
                Err(InternalError::NotLeader(tablet_id).into())
            })
            .await
    }

    async fn wait_mostly_hydrated(&self) -> anyhow::Result<()> {
        let tablet_id = self.tablet_id;
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader.shard.tablet(tablet_id)?.wait_mostly_hydrated().await;
                }
                Err(InternalError::NotLeader(tablet_id).into())
            })
            .await
    }

    async fn catchup(&self) -> anyhow::Result<()> {
        let tablet_id = self.tablet_id;
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader.shard.tablet(tablet_id)?.catchup().await;
                }
                Err(InternalError::NotLeader(tablet_id).into())
            })
            .await
    }

    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>> {
        let tablet_id = self.tablet_id;
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader.shard.tablet(tablet_id)?.find_split().await;
                }
                Err(InternalError::NotLeader(tablet_id).into())
            })
            .await
    }
}
