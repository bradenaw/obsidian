mod shareable_revokable;

use std::cmp::max;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_stream::try_stream;
use futures::future::OptionFuture;
use futures::Stream;
use futures::StreamExt;
use rand::Rng;
use tokio::select;
use tokio::time::interval;
use tokio::time::sleep_until;
use tokio::time::Instant;
use tokio::time::Sleep;
use tokio_stream::wrappers::IntervalStream;
use uuid::Uuid;

use crate::replica::shareable_revokable::ShareableRevokable;
use crate::runtime::Journal;
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
struct Replica<Leader, Follower> {
    replica_id: ReplicaId,
    journal: Arc<dyn Journal<Proposal>>,
    state: ShareableRevokable<ReplicaState<Leader, Follower>>,
    campaign_splay: Duration,
    heartbeat_interval: Duration,
    renew_interval: Duration,
    recency_window: Duration,
    lease_duration: Duration,
}

enum ReplicaState<Leader, Follower> {
    Leader {
        leader: Leader,
        last_confirmation: tokio::time::Instant,
    },
    Follower(Follower),
}

#[derive(Eq, PartialEq, Clone, Copy)]
struct ReplicaId(Uuid);

struct Proposal {
    replica_id: ReplicaId,
    // Timestamps are not necessarily ordered the same way as WalSeqs, since the leader may submit
    // proposals concurrently that can be accepted by the journal in any order.
    timestamp: Timestamp,
    proposal_type: ProposalType,
}

enum ProposalType {
    // Acquires are only accepted if their timestamp is greater than the last non-relinquished
    // lease_end.
    Acquire {
        // No further writes will happen with a lower timestamp, so any
        safe_read: Timestamp,
        lease_end: Timestamp,
    },
    Relinquish,
    // Appends are only accepted if they're made by the current leader.
    Append(Entry),
    // Heartbeats are always accepted since they have no effect.
    Heartbeat,
}

struct Entry {}

trait Leader<Follower> {
    fn process(&self, entry: Entry);
    fn demote(self) -> Follower;
}

trait Follower<Leader> {
    fn process(&self, entry: Entry);
    async fn promote(self) -> Leader;
}

impl<TLeader, TFollower> Replica<TLeader, TFollower>
where
    TLeader: Leader<TFollower> + Send + Sync + 'static,
    TFollower: Follower<TLeader> + Send + Sync + 'static,
{
    async fn with_state<F, Fut, T>(&self, f: F) -> anyhow::Result<T>
    where
        F: FnOnce(&ReplicaState<TLeader, TFollower>) -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<T>> + Send,
        T: Send + 'static,
    {
        let lease_duration = self.lease_duration;
        self.state
            .share(move |state| async move {
                if let ReplicaState::Leader {
                    ref last_confirmation,
                    ..
                } = state
                {
                    if Instant::now().duration_since(*last_confirmation) > lease_duration / 2 {
                        return Err(anyhow!(""));
                    }
                }
                f(state).await
            })
            .await?
    }

    fn propose_at(&self, ts: Timestamp, proposal_type: ProposalType) {
        todo!();
        //self.journal
        //    .append(Proposal {
        //        replica_id: self.replica_id,
        //        timestamp: Timestamp::now(), // order
        //        proposal_type,
        //    })
    }

    fn next_timestamp(&self) -> Timestamp {
        todo!();
    }

    async fn promote_or_renew(&self, confirmation: Instant) -> anyhow::Result<()> {
        self.state
            .revoke_and_modify(|state: &mut ReplicaState<TLeader, TFollower>| async {
                match state {
                    ReplicaState::Leader {
                        ref mut last_confirmation,
                        ..
                    } => {
                        *last_confirmation = confirmation;
                    }
                    ReplicaState::Follower(follower) => {
                        let leader = follower.promote().await;
                        *state = ReplicaState::Leader {
                            last_confirmation: confirmation,
                            leader,
                        };
                    }
                }
            })
            .await;
        Ok(())
    }

    async fn process(&self) -> anyhow::Result<()> {
        let mut accepted = accepted_proposals(
            self.journal
                .read(self.journal.oldest_available().await?)
                .boxed(),
        )
        .boxed();

        // TODO: next expire
        let mut next_renew: Option<tokio::time::Instant> = None;
        let mut next_campaign: Option<tokio::time::Instant> = None;
        let mut heartbeat_ticker = ticker(self.heartbeat_interval);
        // True if we've published a heartbeat that we haven't observed in the stream yet.
        let mut pending_heartbeat = false;
        let mut pending_acquire = None;

        let mut current_lease = None;

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
                                next_renew = Some(tokio::time::Instant::now() + self.renew_interval);
                                // TODO: possibly promote self
                            }
                        },
                        ProposalType::Relinquish => {
                            current_lease = None;
                        },
                        ProposalType::Append(entry) => {},
                        ProposalType::Heartbeat => {
                            if proposal.replica_id == self.replica_id {
                                pending_heartbeat = false;

                                // Receiving a heartbeat of our own means we're as close to 'now'
                                // in the journal as we can be.
                                let current_lease_expired = match current_lease {
                                    Some((_, lease_end)) => Timestamp::now() > lease_end,
                                    None => true,
                                };
                                if next_campaign.is_none() && current_lease_expired {
                                    let wait_time =
                                        rand::thread_rng().gen_range(Duration::ZERO..self.campaign_splay);
                                    next_campaign = Some(tokio::time::Instant::now() + wait_time);
                                }
                            }
                        },
                    }
                },
                _ = StreamExt::next(&mut heartbeat_ticker), if !pending_heartbeat => {
                    self.propose_at(self.next_timestamp(), ProposalType::Heartbeat);
                    pending_heartbeat = true;
                },
                _ = maybe_sleep_until(max(next_campaign, next_renew)), if pending_acquire.is_none() => {
                    let ts = self.next_timestamp();
                    let lease_end = Timestamp::from_nanos(ts.as_nanos() + (self.lease_duration.as_nanos() as u64));
                    pending_acquire = Some(tokio::time::Instant::now());
                    self.propose_at(ts, ProposalType::Acquire{ safe_read: todo!(), lease_end });
                },
            }
        }
    }
}

fn duration_until(ts: Timestamp) -> Duration {
    ts.saturating_duration_since(Timestamp::now())
}

fn ticker(x: Duration) -> IntervalStream {
    let mut s = interval(x);
    s.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    IntervalStream::new(s)
}

fn maybe_sleep_until(x: Option<tokio::time::Instant>) -> OptionFuture<Sleep> {
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
                    current_leader = Some((proposal.replica_id, new_lease_end));
                } else {
                    continue;
                }
            }

            // If this entry wasn't proposed by the current leader, or there is no leader, skip it.
            if !matches!(proposal.proposal_type, ProposalType::Heartbeat)
                && current_leader
                    .map(|(leader_replica_id, _)| proposal.replica_id != leader_replica_id)
                    .unwrap_or(true)
            {
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
