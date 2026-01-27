use std::sync::Arc;

use crate::replica::Follower;
use crate::replica::Leader;
use crate::replica::Proposal;
use crate::replica::Replica;
use crate::runtime::Journal;
use crate::test::MemJournal;

struct TestLeader {
    id: usize,
}

impl TestLeader {
    fn new(id: usize) -> Self {
        Self { id }
    }
}

impl Leader<TestFollower> for TestLeader {
    fn process(&self, entry: super::Entry) {
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

impl Follower<TestLeader> for TestFollower {
    fn process(&self, entry: super::Entry) {
        todo!()
    }

    async fn promote(self) -> TestLeader {
        TestLeader { id: self.id }
    }
}

struct Entry {}

#[tokio::test]
async fn test_election() -> anyhow::Result<()> {
    let journal = Arc::new(MemJournal::new()) as Arc<dyn Journal<Proposal>>;
    let replicas = [
        Replica::new(Arc::clone(&journal), TestFollower::new(1)),
        Replica::new(Arc::clone(&journal), TestFollower::new(2)),
        Replica::new(Arc::clone(&journal), TestFollower::new(3)),
    ];
    Ok(())
}
