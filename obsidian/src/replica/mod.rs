#![cfg(test)]
mod tests;

use std::cmp::max;
use std::future::Future;
use std::ops::Deref;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_stream::stream;
use async_stream::try_stream;
use async_trait::async_trait;
use futures::future::OptionFuture;
use futures::Stream;
use futures::StreamExt;
use rand::Rng;
use tokio::select;
use tokio::sync::RwLock;
use tokio::time::interval;
use tokio::time::sleep_until;
use tokio::time::Instant;
use tokio::time::Sleep;
use tokio_stream::wrappers::IntervalStream;
use uuid::Uuid;

use crate::runtime::Journal;
use crate::util::WithBackground;
use crate::Timestamp;
use crate::WalSeq;

/// A Replica is a participant in a replica group, which all hold copies of the same logical data.
/// Replicas join the group and elect a leader. The leader executes writes and has the most
/// up-to-date copy of the data. The followers also have a copy of this data, but at any given time
/// likely will not have the most recent few writes. Followers thus can serve stale reads
/// (timestamps that are older than some amount, probably tens to hundreds of milliseconds at
/// minimum), but cannot serve get_latest and friends.
///
/// Replicas transition back and forth between leaders and followers. If the leader departs
/// (intentionally or accidentally), the followers will elect a new leader.
///
/// For any of this to work, we need to guarantee that there is at most one leader at any point in
/// time. "Point in time" is defined along two dimensions:
///
/// - Logical, that is, that segments of the journal have at most one leader. This prevents
///   split-braining where different replicas apply a different set of writes from each other. This
///   is absolutely guaranteed by the rules of proposal acceptance below. Writes are guaranteed not
///   to split brain.
/// - Real-time. We also need to guarantee that there is only one replica behaving as a leader for
///   the purpose of reads at any given time, otherwise we get split-brains where writes that have
///   been applied to one leader are not visible on another replica claiming to serve latest reads.
///   This is resolved with timing assumptions, which assumes some (loose) amount of clock
///   synchronization for correctness. This is on the order of seconds, which is generally an
///   achievable guarantee to provide, but it's still worth noting that if the clocks in the system
///   get far out of sync the system may not behave correctly.
pub struct Replica<Leader, Follower>(WithBackground<ReplicaInner<Leader, Follower>>);

struct ReplicaInner<Leader, Follower> {
    replica_id: ReplicaId,
    journal: Arc<dyn Journal<Proposal>>,

    campaign_splay: Duration,
    heartbeat_interval: Duration,
    renew_interval: Duration,
    lease_duration: Duration,

    // This is Option just for the convenience of take() when promoting/demoting.
    state: RwLock<Option<InnerReplicaState<Leader, Follower>>>,
    // The timestamp of the write of the last seen Acquire by self, stored as the duration since
    // self.epoch in nanoseconds.
    last_confirmation: AtomicInstant,
}

enum InnerReplicaState<Leader, Follower> {
    Leader {
        leader: Leader,
        lease_end: Timestamp,
    },
    Follower(Follower),
}

impl<Leader, Follower> InnerReplicaState<Leader, Follower> {
    fn as_replica_state(&self) -> ReplicaState<'_, Leader, Follower> {
        match self {
            InnerReplicaState::Leader { leader, .. } => ReplicaState::Leader(&leader),
            InnerReplicaState::Follower(follower) => ReplicaState::Follower(&follower),
        }
    }
}

pub enum ReplicaState<'a, Leader, Follower> {
    Leader(&'a Leader),
    Follower(&'a Follower),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaId(Uuid);

impl ReplicaId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Clone)]
pub struct Proposal {
    replica_id: ReplicaId,
    // Timestamps are not necessarily ordered the same way as WalSeqs, since the leader may submit
    // proposals concurrently that can be accepted by the journal in any order.
    timestamp: Timestamp,
    proposal_type: ProposalType,
}

#[derive(Clone)]
enum ProposalType {
    // Acquires are only accepted if their timestamp is greater than the last non-relinquished
    // lease_end.
    Acquire { lease_end: Timestamp },
    Relinquish,
    // Appends are only accepted if they're made by the current leader.
    Append(Entry),
    // Heartbeats are always accepted since they have no effect.
    Heartbeat,
}

#[derive(Clone)]
pub struct Entry {}

#[async_trait]
pub trait Leader<Follower> {
    async fn process(&self, entry: Entry);
    async fn demote(self) -> Follower;
}

#[async_trait]
pub trait Follower<Leader> {
    async fn process(&self, entry: Entry);
    async fn promote(self) -> Leader;
}

impl<TLeader, TFollower> Replica<TLeader, TFollower>
where
    TLeader: Leader<TFollower> + Send + Sync + 'static,
    TFollower: Follower<TLeader> + Send + Sync + 'static,
{
    pub fn new(journal: Arc<dyn Journal<Proposal>>, follower: TFollower) -> Self {
        let inner = WithBackground::new(Arc::new(ReplicaInner {
            replica_id: ReplicaId::new(),
            journal,
            campaign_splay: Duration::from_millis(2000),
            heartbeat_interval: Duration::from_millis(1000),
            renew_interval: Duration::from_millis(10000),
            lease_duration: Duration::from_millis(10000),
            state: RwLock::new(Some(InnerReplicaState::Follower(follower))),
            last_confirmation: AtomicInstant::new(),
        }));
        inner.spawn(async |replica| {
            // XXX: Do something with this error.
            replica.background_process().await.unwrap();
        });
        Self(inner)
    }

    pub fn replica_id(&self) -> ReplicaId {
        self.0.replica_id
    }

    pub async fn try_append(&self, entry: Entry) -> anyhow::Result<()> {
        let state = self.0.state.read().await;
        if let Some(InnerReplicaState::Leader { lease_end, .. }) = state.deref() {
            let ts = Timestamp::now();
            if ts > *lease_end {
                return Err(anyhow!("cannot append: lease expired"));
            }
            self.0
                .journal
                .append(Proposal {
                    replica_id: self.0.replica_id,
                    timestamp: ts,
                    proposal_type: ProposalType::Append(entry),
                })
                .await?;
            // XXX: Actually  make sure the proposal is accepted.
        }

        Err(anyhow!("cannot append: not currently leader"))
    }

    pub async fn with_state<F, Fut, T>(&self, f: F) -> anyhow::Result<T>
    where
        F: FnOnce(ReplicaState<TLeader, TFollower>) -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<T>> + Send,
        T: Send + 'static,
    {
        let state = self.0.state.read().await;

        // TODO: select! against a future that fills if we need to promote/demote
        let out = f(state
            .as_ref()
            .ok_or_else(|| anyhow!("no replica state present"))?
            .as_replica_state())
        .await?;

        if matches!(state.deref(), Some(InnerReplicaState::Leader { .. })) {
            let last_confirmation = self.0.last_confirmation.load();

            // TODO: Quantify the slop factor somewhere.
            if Instant::now().duration_since(last_confirmation) > self.0.lease_duration / 2 {
                return Err(anyhow!("lease expired before operation completed"));
            }
        }

        Ok(out)
    }
}

impl<TLeader, TFollower> ReplicaInner<TLeader, TFollower>
where
    TLeader: Leader<TFollower> + Send + Sync + 'static,
    TFollower: Follower<TLeader> + Send + Sync + 'static,
{
    fn next_timestamp(&self) -> Timestamp {
        Timestamp::now()
    }

    fn propose_at(&self, ts: Timestamp, proposal_type: ProposalType) {
        tokio::spawn({
            let journal = Arc::clone(&self.journal);
            let replica_id = self.replica_id;

            async move {
                let _ = journal
                    .append(Proposal {
                        replica_id: replica_id,
                        timestamp: ts,
                        proposal_type,
                    })
                    .await;
            }
        });
    }

    async fn maybe_promote(&self, lease_end: Timestamp) {
        {
            let maybe_state = self.state.read().await;
            if matches!(maybe_state.deref(), Some(InnerReplicaState::Leader { .. })) {
                return;
            }
        }

        let mut maybe_state = self.state.write().await;
        if matches!(maybe_state.deref(), Some(InnerReplicaState::Leader { .. })) {
            return;
        }

        let state = maybe_state.take().unwrap();
        let leader = match state {
            InnerReplicaState::Leader { .. } => unreachable!(),
            InnerReplicaState::Follower(follower) => follower.promote().await,
        };
        *maybe_state = Some(InnerReplicaState::Leader { leader, lease_end });
    }

    async fn maybe_demote(&self) {
        {
            let maybe_state = self.state.read().await;
            if matches!(maybe_state.deref(), Some(InnerReplicaState::Follower(_))) {
                return;
            }
        }

        let mut maybe_state = self.state.write().await;
        if matches!(maybe_state.deref(), Some(InnerReplicaState::Follower(_))) {
            return;
        }

        let state = maybe_state.take().unwrap();
        let follower = match state {
            InnerReplicaState::Leader { leader, .. } => leader.demote().await,
            InnerReplicaState::Follower(_) => unreachable!(),
        };
        *maybe_state = Some(InnerReplicaState::Follower(follower));
    }

    async fn process(&self, entry: Entry) {
        let maybe_state = self.state.read().await;
        let state = maybe_state.as_ref().unwrap();
        match state {
            InnerReplicaState::Leader { leader, .. } => leader.process(entry).await,
            InnerReplicaState::Follower(follower) => follower.process(entry).await,
        }
    }

    async fn background_process(&self) -> anyhow::Result<()> {
        let mut accepted = accepted_proposals(
            self.journal
                .tail(self.journal.oldest_available().await?)
                .boxed(),
        )
        .boxed();

        let mut next_renew: Option<Instant> = None;
        let mut next_campaign: Option<Instant> = None;
        let mut heartbeat_ticker = Box::pin(jittered_ticker(self.heartbeat_interval));
        // True if we've published a Heartbeat that we haven't observed in the stream yet.
        let mut pending_heartbeat = false;
        // Some if we've published an Acquire message that we haven't observed in the stream yet.
        let mut pending_acquire: Option<Instant> = None;

        let mut current_lease = None;

        let mut last_ts = Timestamp::now();

        loop {
            select! {
                next = StreamExt::next(&mut accepted) => {
                    let (_seq, proposal) = next
                        .transpose()?
                        .ok_or_else(|| anyhow!("replication stream ended"))?;

                    match proposal.proposal_type {
                        ProposalType::Acquire{lease_end, ..} => {
                            current_lease = Some((proposal.replica_id, lease_end));
                            next_campaign = None;

                            if proposal.replica_id == self.replica_id {
                                next_renew = Some(Instant::now() + self.renew_interval);

                                self.last_confirmation.store(
                                    pending_acquire
                                        .ok_or_else(|| {
                                            anyhow!("received acquire that was not pending")
                                        })?,
                                );
                                pending_acquire = None;
                                self.maybe_promote(lease_end).await;
                                // XXX: Advance lease_end!
                            } else {
                                self.maybe_demote().await;
                            }
                        },
                        ProposalType::Relinquish => {
                            current_lease = None;
                            if proposal.replica_id == self.replica_id {
                                self.maybe_demote().await;
                            }
                        },
                        ProposalType::Append(entry) => {
                            self.process(entry).await;
                        },
                        ProposalType::Heartbeat => {
                            if proposal.replica_id == self.replica_id {
                                pending_heartbeat = false;

                                // Receiving a heartbeat of our own means we're as close to 'now'
                                // in the journal as we can be, since we only ever have one in
                                // flight.
                                let current_lease_expired = match current_lease {
                                    Some((_, lease_end)) => Timestamp::now() > lease_end,
                                    None => true,
                                };
                                if next_campaign.is_none() && current_lease_expired {
                                    // eh, we may as well just go for it
                                    let wait_time =
                                        rand::thread_rng().gen_range(Duration::ZERO..self.campaign_splay);
                                    next_campaign = Some(Instant::now() + wait_time);
                                }
                            }
                        },
                    }
                },
                _ = StreamExt::next(&mut heartbeat_ticker), if !pending_heartbeat => {
                    let ts = Timestamp::now_after(last_ts);
                    last_ts = ts;
                    self.propose_at(ts, ProposalType::Heartbeat);
                    pending_heartbeat = true;
                },
                _ = maybe_sleep_until(max(next_campaign, next_renew)), if pending_acquire.is_none() => {
                    let ts = Timestamp::now_after(last_ts);
                    last_ts = ts;
                    let lease_end = Timestamp::from_nanos(
                        ts.as_nanos() + (self.lease_duration.as_nanos() as u64),
                    );
                    pending_acquire = Some(Instant::now());
                    self.propose_at(ts, ProposalType::Acquire{ lease_end });
                },
            }
        }
    }
}

fn duration_until(ts: Timestamp) -> Duration {
    ts.saturating_duration_since(Timestamp::now())
}

fn jittered_ticker(x: Duration) -> impl Stream<Item = ()> {
    let mut next = Instant::now();
    stream! {
        loop {
            next = next + rand::thread_rng().gen_range(x / 2..x * 3/2);
            yield ();
            sleep_until(next).await;
        }
    }
}

fn ticker(x: Duration) -> IntervalStream {
    let mut s = interval(x);
    s.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    IntervalStream::new(s)
}

fn maybe_sleep_until(x: Option<Instant>) -> OptionFuture<Sleep> {
    OptionFuture::from(x.map(|instant| sleep_until(instant)))
}

fn accepted_proposals<S>(mut proposals: S) -> impl Stream<Item = anyhow::Result<(WalSeq, Proposal)>>
where
    S: Stream<Item = anyhow::Result<(WalSeq, Proposal)>> + Send + Unpin,
{
    try_stream! {
        // XXX: If we start anywhere in the middle we might accept an acquire that shouldn't have
        // been... Probably means we just need to guarantee that a trim always happens at a
        // position with a successful acquire in it.

        let mut current_leader = None;

        while let Some((seq, proposal)) = proposals.next().await.transpose()? {
            if let ProposalType::Acquire { lease_end: new_lease_end, ..} = proposal.proposal_type {
                let accept_acquire = match current_leader {
                    Some((leader_replica_id, current_lease_end)) => {
                        // Accept if it's either a renewal by the previous leader, or if it's a new
                        // lease term after the previous one expired.
                        proposal.replica_id == leader_replica_id
                            || proposal.timestamp > current_lease_end
                    },
                    None => true,
                };

                if accept_acquire {
                    log::info!("{:?} is leader for {:?} - {:?}", proposal.replica_id, proposal.timestamp, new_lease_end);
                    current_leader = Some((proposal.replica_id, new_lease_end));
                } else {
                    log::info!("acquire at {:?} {:?} by {:?} rejected", seq, proposal.timestamp, proposal.replica_id);
                    continue;
                }
            }

            // If this entry wasn't proposed by the current leader, or there is no leader, skip it.
            if !matches!(proposal.proposal_type, ProposalType::Heartbeat)
                && current_leader
                    .map(|(leader_replica_id, _)| proposal.replica_id != leader_replica_id)
                    .unwrap_or(true)
            {
                log::info!("proposal at {:?} {:?} by {:?} rejected", seq, proposal.timestamp, proposal.replica_id);
                continue;
            }

            // TODO: Make sure the timestamp is below the end of the lease term. That shouldn't
            // ever happen because the leader shouldn't ever make a proposal like that.

            if let ProposalType::Relinquish = proposal.proposal_type {
                current_leader = None;
            }

            yield (seq, proposal);
        }
    }
}

struct AtomicInstant {
    epoch: Instant,
    elapsed: AtomicI64,
}

impl AtomicInstant {
    fn new() -> Self {
        Self {
            epoch: Instant::now(),
            elapsed: AtomicI64::new(0),
        }
    }

    fn load(&self) -> Instant {
        let x = self.elapsed.load(Ordering::SeqCst);
        if x >= 0 {
            self.epoch
                .checked_add(Duration::from_nanos(x as u64))
                .unwrap()
        } else {
            self.epoch
                .checked_sub(Duration::from_nanos(-x as u64))
                .unwrap()
        }
    }

    fn store(&self, x: Instant) {
        if let Some(elapsed) = x.checked_duration_since(self.epoch) {
            self.elapsed
                .store(elapsed.as_nanos() as i64, Ordering::SeqCst);
        } else {
            let elapsed = self.epoch.duration_since(x);
            self.elapsed
                .store(elapsed.as_nanos() as i64, Ordering::SeqCst);
        }
    }
}
