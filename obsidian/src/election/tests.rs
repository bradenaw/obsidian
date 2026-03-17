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
use tokio::sync::Notify;

use crate::election::Follower;
use crate::election::FollowerInit;
use crate::election::JournalWriter;
use crate::election::Leader;
use crate::election::Participant;
use crate::election::ParticipantBuilder;
use crate::election::ParticipantState;
use crate::election::Proposal;
use crate::election::ProposalType;
use crate::runtime::Journal;
use crate::test::MemJournal;
use crate::JournalSeq;

#[derive(Clone)]
struct TestEntry {}

#[tokio::test]
async fn test_election() -> anyhow::Result<()> {
    let _ = pretty_env_logger::try_init();

    let builder = ParticipantBuilder::new()
        .lease_duration(Duration::from_millis(1000))
        .heartbeat_interval(Duration::from_millis(500))
        .renew_interval(Duration::from_millis(100))
        .lease_grace_period(Duration::from_millis(200));

    let mut replica_group = TestReplicaGroup::new(builder);

    replica_group.add_replica();
    replica_group.add_replica();
    replica_group.add_replica();

    let first_leader_id = {
        let first_leader = replica_group.leader().await;
        first_leader.journal_view.pause_tail();
        first_leader.id
    };

    // Because the leader can't observe its own Acquires, it will eventually stop making them, and
    // someone else will need to take over.

    for _ in 0..10 {
        let leader_id = replica_group.leader().await.id;
        if leader_id != first_leader_id {
            continue;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    Ok(())
}

struct TestReplicaGroup {
    journal: Arc<MemJournal<Proposal<TestEntry>>>,
    replicas: Vec<TestReplica>,
    builder: ParticipantBuilder,
}

impl TestReplicaGroup {
    fn new(builder: ParticipantBuilder) -> Self {
        Self {
            journal: Arc::new(MemJournal::new()),
            replicas: vec![],
            builder,
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
            participant: self.builder.build(
                journal_view as Arc<dyn Journal<Proposal<TestEntry>>>,
                TestFollowerInit { id: offset },
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
    journal_view: Arc<TestJournal<Arc<dyn Journal<Proposal<TestEntry>>>>>,
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

struct TestFollowerInit {
    id: usize,
}

impl TestFollowerInit {
    fn new(id: usize) -> Self {
        Self { id }
    }
}

impl FollowerInit<TestEntry, TestFollower> for TestFollowerInit {
    fn new_follower(&self) -> TestFollower {
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

struct TestJournal<J> {
    inner: J,
    offset: usize,
    paused: Mutex<Option<Arc<Notify>>>,
}

impl<J> TestJournal<J>
where
    J: Journal<Proposal<TestEntry>>,
{
    fn new(offset: usize, inner: J) -> Self {
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
impl<J> Journal<Proposal<TestEntry>> for TestJournal<J>
where
    J: Journal<Proposal<TestEntry>>,
{
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

#[async_trait]
impl<E> Journal<E> for Arc<dyn Journal<E>>
where
    E: Send + 'static,
{
    async fn append(&self, entry: E) -> anyhow::Result<JournalSeq> {
        Journal::append(self.deref(), entry).await
    }

    fn tail(
        &self,
        first: JournalSeq,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<(JournalSeq, E)>> + Send + '_>> {
        Journal::tail(self.deref(), first)
    }

    async fn latest(&self) -> anyhow::Result<JournalSeq> {
        Journal::latest(self.deref()).await
    }

    async fn oldest_available(&self) -> anyhow::Result<JournalSeq> {
        Journal::oldest_available(self.deref()).await
    }

    async fn trim(&self, before: JournalSeq) -> anyhow::Result<()> {
        Journal::trim(self.deref(), before).await
    }
}
