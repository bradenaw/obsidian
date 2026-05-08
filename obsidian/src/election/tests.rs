use std::iter;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_stream::try_stream;
use async_trait::async_trait;
use futures::future;
use futures::future::Either;
use futures::Stream;
use futures::StreamExt;
use obsidian_external::mem::MemJournal;
use tokio::sync::Notify;

use crate::election::Follower;
use crate::election::FollowerBuilder;
use crate::election::JournalWriter;
use crate::election::Leader;
use crate::election::Participant;
use crate::election::ParticipantState;
use crate::election::Proposal;
use crate::election::ProposalType;
use obsidian_external::Journal;
use crate::JournalSeq;

#[derive(Clone)]
struct TestEntry {}

#[tokio::test]
async fn test_election() -> anyhow::Result<()> {
    let _ = pretty_env_logger::try_init();

    let lease_duration = Duration::from_millis(100);

    let mut replica_group = TestReplicaGroup::new(lease_duration);

    replica_group.add_replica();
    replica_group.add_replica();
    replica_group.add_replica();

    let first_leader_id = {
        let first_leader = replica_group.leader().await;
        first_leader.journal_view.pause_tail();
        first_leader.id
    };

    tokio::time::sleep(lease_duration * 2).await;

    // Because the leader can't observe its own Acquires, it will eventually stop making them, and
    // someone else will need to take over.

    let mut new_leader = false;
    for _ in 0..20 {
        let leader_id = replica_group.leader().await.id;
        if leader_id != first_leader_id {
            new_leader = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if !new_leader {
        panic!("no new leader elected");
    }

    Ok(())
}

struct TestReplicaGroup {
    journal: Arc<MemJournal<Proposal<TestEntry>>>,
    replicas: Vec<TestReplica>,
    lease_duration: Duration,
}

impl TestReplicaGroup {
    fn new(lease_duration: Duration) -> Self {
        Self {
            journal: Arc::new(MemJournal::new()),
            replicas: vec![],
            lease_duration,
        }
    }

    fn add_replica(&mut self) {
        let offset = self.replicas.len();
        let journal_view = Arc::new(TestJournal::new(
            offset,
            Arc::clone(&self.journal) as Arc<dyn Journal<Proposal<TestEntry>>>,
        ));
        self.replicas.push(TestReplica {
            id: offset,
            journal_view: Arc::clone(&journal_view),
            participant: Participant::new(
                format!("{}", offset),
                journal_view as Arc<dyn Journal<Proposal<TestEntry>>>,
                TestFollowerBuilder { id: offset },
                self.lease_duration,
            ),
        });
    }

    async fn leader(&self) -> &TestReplica {
        for _ in 0..5 {
            for replica in &self.replicas {
                if replica.is_leader().await.unwrap() {
                    return replica;
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        panic!("no leader");
    }
}

struct TestReplica {
    id: usize,
    journal_view: Arc<TestJournal>,
    participant: Participant<TestEntry, TestLeader, TestFollower>,
}

impl TestReplica {
    async fn entries(&self) -> anyhow::Result<Vec<TestEntry>> {
        self.participant
            .with_state(async move |participant_state| {
                Ok(match participant_state {
                    ParticipantState::Leader(leader) => leader.entries.lock().unwrap().clone(),
                    ParticipantState::Follower(follower) => {
                        follower.entries.lock().unwrap().clone()
                    }
                })
            })
            .await
    }

    async fn is_leader(&self) -> anyhow::Result<bool> {
        self.participant
            .with_state(async move |participant_state| {
                Ok(match participant_state {
                    ParticipantState::Leader(_) => true,
                    ParticipantState::Follower(_) => false,
                })
            })
            .await
    }
}

struct TestFollowerBuilder {
    id: usize,
}

impl TestFollowerBuilder {
    fn new(id: usize) -> Self {
        Self { id }
    }
}

impl FollowerBuilder<TestEntry, TestFollower> for TestFollowerBuilder {
    fn build(&self) -> TestFollower {
        TestFollower {
            id: self.id,
            entries: Mutex::new(vec![]),
        }
    }
}

struct TestLeader {
    id: usize,
    entries: Mutex<Vec<TestEntry>>,
}

#[async_trait]
impl Leader<TestEntry, TestFollower> for TestLeader {
    async fn demote(self) -> anyhow::Result<TestFollower> {
        Ok(TestFollower {
            id: self.id,
            entries: self.entries,
        })
    }
}

struct TestFollower {
    id: usize,
    entries: Mutex<Vec<TestEntry>>,
}

#[async_trait]
impl Follower<TestEntry, TestLeader> for TestFollower {
    async fn process(&self, _seq: JournalSeq, entry: TestEntry) {
        let mut entries = self.entries.lock().unwrap();
        entries.push(entry);
    }

    async fn promote(self, _writer: JournalWriter<TestEntry>) -> anyhow::Result<TestLeader> {
        Ok(TestLeader {
            id: self.id,
            entries: self.entries,
        })
    }
}

struct TestJournal {
    inner: Arc<dyn Journal<Proposal<TestEntry>>>,
    offset: usize,
    paused: Mutex<Option<Arc<Notify>>>,
}

impl TestJournal {
    fn new(offset: usize, inner: Arc<dyn Journal<Proposal<TestEntry>>>) -> Self {
        Self {
            offset,
            inner,
            paused: Mutex::new(None),
        }
    }

    fn pause_tail(&self) {
        let mut paused = self.paused.lock().unwrap();
        *paused = Some(Arc::new(Notify::new()));
    }

    fn unpause_tail(&self) {
        let mut paused = self.paused.lock().unwrap();
        if let Some(unpause) = paused.deref() {
            unpause.notify_waiters();
        }
        *paused = None;
    }
}

#[async_trait]
impl Journal<Proposal<TestEntry>> for TestJournal {
    async fn append(&self, proposal: Proposal<TestEntry>) -> anyhow::Result<JournalSeq> {
        let seq = self.inner.append(proposal.clone()).await?;
        log::info!(
            "{} {:<8} {:<4} {}",
            iter::repeat_n(" ", 40 * self.offset).collect::<String>(),
            "append",
            seq.0,
            match proposal.proposal_type {
                ProposalType::Acquire { .. } => "Acquire",
                ProposalType::Relinquish => "Relinquish",
                ProposalType::Append(_) => "Append",
                ProposalType::Heartbeat => "Heartbeat",
            }
        );
        Ok(seq)
    }

    fn tail(
        &self,
        first: JournalSeq,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<(JournalSeq, Proposal<TestEntry>)>> + Send + '_>>
    {
        let mut stream = self.inner.tail(first);

        Box::pin(try_stream! {
            while let Some((seq, proposal)) = stream.next().await.transpose()? {
                log::info!(
                    "{} {:<8} {:<4} {}",
                    iter::repeat_n(" ", 40 * self.offset).collect::<String>(),
                    "tail",
                    seq.0,
                    match proposal.proposal_type {
                        ProposalType::Acquire { .. } => "Acquire",
                        ProposalType::Relinquish => "Relinquish",
                        ProposalType::Append(_) => "Append",
                        ProposalType::Heartbeat => "Heartbeat",
                    }
                );

                {
                    let paused = self.paused.lock().unwrap();
                    if let Some(unpause) = paused.deref() {
                        Either::Left(Arc::clone(unpause).notified_owned())
                    } else {
                        Either::Right(future::ready(()))
                    }
                }.await;

                yield (seq, proposal);
            }
        })
    }

    async fn latest(&self) -> anyhow::Result<JournalSeq> {
        self.inner.latest().await
    }

    async fn oldest_available(&self) -> anyhow::Result<JournalSeq> {
        self.inner.oldest_available().await
    }

    async fn trim(&self, before: JournalSeq) -> anyhow::Result<()> {
        self.inner.trim(before).await
    }
}
