use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;

use crate::election::Follower;
use crate::election::FollowerBuilder;
use crate::election::JournalWriter;
use crate::election::Leader;
use crate::election::Participant;
use crate::election::ParticipantState;
use crate::election::Proposal;
use crate::lsm::LsmOptions;
use crate::lsm::Manifest;
use crate::replica::recovery::ShardRecovery;
use crate::runtime;
use crate::runtime::Journal;
use crate::runtime::Meta;
use crate::runtime::Shard as _;
use crate::runtime::Shards;
use crate::runtime::Storage;
use crate::shard::Shard;
use crate::shard::ShardJournalWriter;
use crate::Bound;
use crate::Direction;
use crate::HistoryRange;
use crate::InternalError;
use crate::JournalEntry;
use crate::JournalSeq;
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

const LEASE_DURATION: Duration = Duration::from_millis(10_000);

pub(crate) struct Replica {
    shard_id: ShardId,
    participant: Arc<Participant<JournalEntry, LeaderReplica, FollowerReplica>>,

    tablets: RwLock<HashMap<TabletId, Arc<dyn runtime::Tablet>>>,
}

impl Replica {
    pub fn new(
        name: String,
        shard_id: ShardId,
        lsm_options: LsmOptions,
        storage: Arc<dyn Storage>,
        meta: Arc<dyn Meta>,
        shards: Arc<dyn Shards>,
        journal: Arc<dyn Journal<Proposal<JournalEntry>>>,
    ) -> Replica {
        Replica {
            shard_id,
            participant: Arc::new(Participant::new(
                name,
                journal,
                ReplicaOptions {
                    shard_id,
                    lsm_options,
                    storage,
                    meta,
                    shards,
                },
                LEASE_DURATION,
            )),
            tablets: RwLock::new(HashMap::new()),
        }
    }

    pub fn became_leader_at_subscribe(&self) -> (Option<JournalSeq>, impl Future<Output = ()>) {
        self.participant.became_leader_at_subscribe()
    }
}

#[derive(Clone)]
struct ReplicaOptions {
    shard_id: ShardId,
    lsm_options: LsmOptions,
    storage: Arc<dyn Storage>,
    meta: Arc<dyn Meta>,
    shards: Arc<dyn Shards>,
}

impl FollowerBuilder<JournalEntry, FollowerReplica> for ReplicaOptions {
    fn build(&self) -> FollowerReplica {
        FollowerReplica {
            options: self.clone(),
            recovery: Mutex::new(Some(ShardRecovery::empty(
                self.lsm_options.clone(),
                Arc::clone(&self.storage),
            ))),
        }
    }
}

struct LeaderReplica {
    options: ReplicaOptions,
    last_persisted_manifests: HashMap<TabletId, Manifest>,
    shard: Shard,
}

#[async_trait]
impl Leader<JournalEntry, FollowerReplica> for LeaderReplica {
    async fn demote(self) -> anyhow::Result<FollowerReplica> {
        Ok(FollowerReplica {
            recovery: Mutex::new(Some(ShardRecovery::from_manifests(
                self.options.lsm_options.clone(),
                Arc::clone(&self.options.storage),
                self.last_persisted_manifests,
            ))),
            options: self.options,
        })
    }
}

struct FollowerReplica {
    options: ReplicaOptions,
    recovery: Mutex<Option<ShardRecovery>>,
}

#[async_trait]
impl Follower<JournalEntry, LeaderReplica> for FollowerReplica {
    async fn process(&self, seq: JournalSeq, entry: JournalEntry) {
        self.recovery
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .process(seq, entry);
    }

    async fn promote(self, journal: JournalWriter<JournalEntry>) -> anyhow::Result<LeaderReplica> {
        let recovery = self.recovery.lock().unwrap().take().unwrap();
        let lsms = recovery.wait().await?;

        let manifests = lsms
            .iter()
            .map(|(tablet_id, lsm)| (*tablet_id, lsm.manifest()))
            .collect();

        let shard = Shard::new(
            self.options.shard_id,
            Arc::clone(&self.options.storage),
            Arc::clone(&self.options.meta),
            Arc::clone(&self.options.shards),
            self.options.lsm_options.clone(),
            lsms,
            Arc::new(journal),
        )
        .await?;

        // TODO: intercept written manifests into last_persisted_manifests

        Ok(LeaderReplica {
            options: self.options,
            last_persisted_manifests: manifests,
            shard,
        })
    }
}

#[async_trait]
impl runtime::Shard for Replica {
    fn id(&self) -> ShardId {
        self.shard_id
    }

    fn tablet(&self, tablet_id: TabletId) -> anyhow::Result<Arc<dyn runtime::Tablet>> {
        if tablet_id.0 != self.shard_id {
            return Err(anyhow!(
                "{:?} not a tablet of {:?}",
                tablet_id,
                self.shard_id
            ));
        }

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
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader.shard.wait_meta_sync(ts).await;
                }
                Err(InternalError::NotLeader(self.shard_id).into())
            })
            .await
    }

    async fn tx_try_commit(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<TxOutcome> {
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader
                        .shard
                        .tx_try_commit(txid, ts, precond_keys, mut_keys)
                        .await;
                }
                Err(InternalError::NotLeader(self.shard_id).into())
            })
            .await
    }

    async fn tx_try_abort(&self, txid: Txid) -> anyhow::Result<TxOutcome> {
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader.shard.tx_try_abort(txid).await;
                }
                Err(InternalError::NotLeader(self.shard_id).into())
            })
            .await
    }

    async fn tx_wait(&self, txid: Txid) -> Result<TxOutcome, InternalError> {
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader.shard.tx_wait(txid).await;
                }
                Err(InternalError::NotLeader(self.shard_id).into())
            })
            .await
    }
}

struct ReplicaTablet {
    tablet_id: TabletId,
    participant: Arc<Participant<JournalEntry, LeaderReplica, FollowerReplica>>,
}

#[async_trait]
impl runtime::Tablet for ReplicaTablet {
    async fn get_multi(
        &self,
        ts: Timestamp,
        keys: BTreeSet<Key>,
    ) -> Result<BTreeMap<Key, Record>, InternalError> {
        let tablet_id = self.tablet_id;
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader.shard.tablet(tablet_id)?.get_multi(ts, keys).await;
                }
                Err(InternalError::NotLeader(tablet_id.0))
            })
            .await
    }

    async fn get_latest_multi(
        &self,
        keys: BTreeSet<Key>,
    ) -> Result<(Timestamp, BTreeMap<Key, Record>), InternalError> {
        let tablet_id = self.tablet_id;
        self.participant
            .with_state(async move |participant_state| {
                if let ParticipantState::Leader(leader) = participant_state {
                    return leader.shard.tablet(tablet_id)?.get_latest_multi(keys).await;
                }
                Err(InternalError::NotLeader(tablet_id.0))
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
                Err(InternalError::NotLeader(tablet_id.0))
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
                Err(InternalError::NotLeader(tablet_id.0))
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
                Err(InternalError::NotLeader(tablet_id.0))
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
                Err(InternalError::NotLeader(tablet_id.0))
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
                Err(InternalError::NotLeader(tablet_id.0))
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
                Err(InternalError::NotLeader(tablet_id.0).into())
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
                Err(InternalError::NotLeader(tablet_id.0).into())
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
                Err(InternalError::NotLeader(tablet_id.0).into())
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
                Err(InternalError::NotLeader(tablet_id.0).into())
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
                Err(InternalError::NotLeader(tablet_id.0).into())
            })
            .await
    }
}

#[async_trait]
impl ShardJournalWriter for JournalWriter<JournalEntry> {
    async fn append(&self, entry: JournalEntry) -> anyhow::Result<()> {
        JournalWriter::append(self, entry).await
    }
}
