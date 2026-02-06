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
use crate::election::Leader;
use crate::election::Participant;
use crate::election::ParticipantBuilder;
use crate::election::Proposal;
use crate::election::ProposalType;
use crate::runtime::Journal;
use crate::test::MemJournal;
use crate::WalSeq;

#[derive(Clone)]
struct TestEntry {}

#[tokio::test]
async fn test_election() -> anyhow::Result<()> {
    let _ = pretty_env_logger::try_init();

    let builder = ParticipantBuilder::new()
        .lease_duration(Duration::from_millis(100))
        .heartbeat_interval(Duration::from_millis(50))
        .renew_interval(Duration::from_millis(10));

    let mut replica_group = TestReplicaGroup::new(builder);

    replica_group.add_replica();
    replica_group.add_replica();
    replica_group.add_replica();

    let first_leader_id = {
        let first_leader = replica_group.leader().await;
        first_leader.journal_view.pause_tail();
        first_leader.replica.participant_id()
    };

    // Because the leader can't observe its own Acquires, it will eventually stop making them, and
    // someone else will need to take over.

    for _ in 0..10 {
        let leader_id = replica_group.leader().await.replica.participant_id();
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

        let replica_inner = Arc::new(Mutex::new(ReplicaInner {
            leader: false,
            processed_entries: vec![],
        }));

        self.replicas.push(TestReplica {
            journal_view: Arc::clone(&journal_view),
            inner: Arc::clone(&replica_inner),
            replica: self.builder.build(
                journal_view as Arc<dyn Journal<Proposal<TestEntry>>>,
                TestFollower::new(offset, replica_inner),
            ),
        });
    }

    async fn leader(&self) -> &TestReplica {
        for _ in 0..5 {
            for replica in &self.replicas {
                let inner = replica.inner.lock().unwrap();
                if inner.leader {
                    return replica;
                }
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("no leader");
    }
}

struct TestReplica {
    journal_view: Arc<TestJournal<Arc<dyn Journal<Proposal<TestEntry>>>>>,
    inner: Arc<Mutex<ReplicaInner>>,
    replica: Participant<TestEntry, TestLeader, TestFollower>,
}

struct ReplicaInner {
    leader: bool,
    processed_entries: Vec<TestEntry>,
}

struct TestLeader {
    id: usize,
    inner: Arc<Mutex<ReplicaInner>>,
}

#[async_trait]
impl Leader<TestEntry, TestFollower> for TestLeader {
    async fn process(&self, entry: TestEntry) {
        let mut inner = self.inner.lock().unwrap();
        inner.processed_entries.push(entry);
    }

    async fn demote(self) -> TestFollower {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.leader = false;
        }

        TestFollower {
            id: self.id,
            inner: self.inner,
        }
    }
}

struct TestFollower {
    id: usize,
    inner: Arc<Mutex<ReplicaInner>>,
}

impl TestFollower {
    fn new(id: usize, inner: Arc<Mutex<ReplicaInner>>) -> Self {
        Self { id, inner }
    }
}

#[async_trait]
impl Follower<TestEntry, TestLeader> for TestFollower {
    async fn process(&self, entry: TestEntry) {
        let mut inner = self.inner.lock().unwrap();
        inner.processed_entries.push(entry);
    }

    async fn promote(self) -> TestLeader {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.leader = true;
        }

        TestLeader {
            id: self.id,
            inner: self.inner,
        }
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
    async fn append(&self, proposal: Proposal<TestEntry>) -> anyhow::Result<WalSeq> {
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

    fn read(
        &self,
        _first: WalSeq,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<(WalSeq, Proposal<TestEntry>)>> + Send + '_>>
    {
        todo!();
    }

    fn tail(
        &self,
        first: WalSeq,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<(WalSeq, Proposal<TestEntry>)>> + Send + '_>>
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

    async fn oldest_available(&self) -> anyhow::Result<WalSeq> {
        self.inner.oldest_available().await
    }

    async fn trim(&self, before: WalSeq) -> anyhow::Result<()> {
        self.inner.trim(before).await
    }
}

#[async_trait]
impl<E> Journal<E> for Arc<dyn Journal<E>>
where
    E: Send + 'static,
{
    async fn append(&self, entry: E) -> anyhow::Result<WalSeq> {
        Journal::append(self.deref(), entry).await
    }

    fn read(
        &self,
        first: WalSeq,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<(WalSeq, E)>> + Send + '_>> {
        Journal::read(self.deref(), first)
    }

    fn tail(
        &self,
        first: WalSeq,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<(WalSeq, E)>> + Send + '_>> {
        Journal::tail(self.deref(), first)
    }

    async fn oldest_available(&self) -> anyhow::Result<WalSeq> {
        Journal::oldest_available(self.deref()).await
    }

    async fn trim(&self, before: WalSeq) -> anyhow::Result<()> {
        Journal::trim(self.deref(), before).await
    }
}
