use std::pin::Pin;
use std::time::Duration;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;

use crate::replica::Follower;
use crate::replica::Leader;
use crate::replica::Proposal;
use crate::replica::Replica;
use crate::runtime::Journal;
use crate::test::MemJournal;
use crate::WalSeq;

struct TestLeader {
    id: usize,
}

impl TestLeader {
    fn new(id: usize) -> Self {
        Self { id }
    }
}

#[async_trait]
impl Leader<TestFollower> for TestLeader {
    async fn process(&self, entry: super::Entry) {
        todo!()
    }

    async fn demote(self) -> TestFollower {
        TestFollower { id: self.id }
    }
}

struct TestFollower {
    id: usize,
}

impl TestFollower {
    fn new(id: usize) -> Self {
        Self { id }
    }
}

#[async_trait]
impl Follower<TestLeader> for TestFollower {
    async fn process(&self, entry: super::Entry) {
        todo!()
    }

    async fn promote(self) -> TestLeader {
        TestLeader { id: self.id }
    }
}

struct LoggingJournal<J> {
    inner: J,
}

impl<J> LoggingJournal<J>
where
    J: Journal<Proposal>,
{
    fn new(inner: J) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<J> Journal<Proposal> for LoggingJournal<J>
where
    J: Journal<Proposal>,
{
    async fn append(&self, proposal: Proposal) -> anyhow::Result<WalSeq> {
        let seq = self.inner.append(proposal.clone()).await?;
        log::info!(
            "Journal append {:?} {:?} {:?} {}",
            seq,
            proposal.replica_id,
            proposal.timestamp,
            match proposal.proposal_type {
                crate::replica::ProposalType::Acquire { .. } => "Acquire",
                crate::replica::ProposalType::Relinquish => "Relinquish",
                crate::replica::ProposalType::Append(_) => "Append",
                crate::replica::ProposalType::Heartbeat => "Heartbeat",
            }
        );
        Ok(seq)
    }

    fn read(
        &self,
        first: WalSeq,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<(WalSeq, Proposal)>> + Send + '_>> {
        self.inner.read(first)
    }

    fn tail(
        &self,
        first: WalSeq,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<(WalSeq, Proposal)>> + Send + '_>> {
        self.inner.tail(first)
    }

    async fn oldest_available(&self) -> anyhow::Result<WalSeq> {
        self.inner.oldest_available().await
    }

    async fn trim(&self, before: WalSeq) -> anyhow::Result<()> {
        self.inner.trim(before).await
    }
}

#[tokio::test]
async fn test_election() -> anyhow::Result<()> {
    let _ = pretty_env_logger::try_init();
    let journal = Arc::new(LoggingJournal::new(MemJournal::new())) as Arc<dyn Journal<Proposal>>;
    let replicas = [
        Replica::new(Arc::clone(&journal), TestFollower::new(1)),
        Replica::new(Arc::clone(&journal), TestFollower::new(2)),
        Replica::new(Arc::clone(&journal), TestFollower::new(3)),
    ];

    tokio::time::sleep(Duration::from_secs(4)).await;
    Ok(())
}
