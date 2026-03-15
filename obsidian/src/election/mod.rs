mod atomic_instant;
mod seq_waiters;
#[cfg(test)]
mod tests;

use std::ops::Deref;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use anyhow::anyhow;
use async_stream::stream;
use async_stream::try_stream;
use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;
use rand::Rng;
use tokio::select;
use tokio::sync::Notify;
use tokio::sync::RwLock as AsyncRwLock;
use tokio::time::interval;
use tokio::time::sleep_until;
use tokio::time::Instant;
use tokio_stream::wrappers::IntervalStream;
use uuid::Uuid;

use crate::election::atomic_instant::AtomicInstant;
use crate::election::seq_waiters::SeqWaiters;
use crate::runtime::Journal;
use crate::util::AtomicTimestamp;
use crate::util::Retry;
use crate::util::WithBackground;
use crate::Timestamp;
use crate::WalSeq;

/// A Participant is a member of a replica group. The purpose of election is to select a single
/// leader, who then has the right to append to a journal and serve reads without consulting any
/// other participants.
///
/// Participants transition back and forth between leaders and followers. If the leader departs
/// (intentionally or accidentally), the remaining participants will elect a new leader.
///
/// For any of this to work, we need to guarantee that there is at most one leader at any point in
/// time. "Point in time" is defined along two dimensions:
///
/// - Logical, that is, that segments of the journal have at most one leader. This prevents
///   split-braining where different participants apply a different set of writes from each other.
///   This is absolutely guaranteed by the rules of proposal acceptance below. Writes are
///   guaranteed not to split brain.
/// - Real-time. We also need to guarantee that there is only one participant behaving as a leader
///   for the purpose of reads at any given time, otherwise we get split-brains where writes that
///   have been applied to one leader are not visible on another participant claiming to serve
///   latest reads. This is resolved with timing assumptions, which assumes some (loose) amount of
///   clock synchronization for correctness. This is on the order of seconds, which is generally an
///   achievable guarantee to provide, but it's still worth noting that if the clocks in the system
///   get far out of sync the system may not behave correctly.
pub struct Participant<TEntry, TLeader, TFollower>(
    WithBackground<ParticipantInner<TEntry, TLeader, TFollower>>,
);

pub struct ParticipantBuilder {
    campaign_splay: Duration,
    heartbeat_interval: Duration,
    renew_interval: Duration,
    lease_duration: Duration,
    lease_grace_period: Duration,
}

impl ParticipantBuilder {
    pub fn new() -> Self {
        Self {
            campaign_splay: Duration::from_millis(2000),
            heartbeat_interval: Duration::from_millis(1000),
            renew_interval: Duration::from_millis(10000),
            lease_duration: Duration::from_millis(10000),
            lease_grace_period: Duration::from_millis(5000),
        }
    }

    pub fn campaign_splay(mut self, x: Duration) -> Self {
        self.campaign_splay = x;
        self
    }

    pub fn heartbeat_interval(mut self, x: Duration) -> Self {
        self.heartbeat_interval = x;
        self
    }

    pub fn renew_interval(mut self, x: Duration) -> Self {
        self.renew_interval = x;
        self
    }

    pub fn lease_duration(mut self, x: Duration) -> Self {
        self.lease_duration = x;
        self
    }

    pub fn lease_grace_period(mut self, x: Duration) -> Self {
        self.lease_grace_period = x;
        self
    }

    pub fn build<TEntry, TLeader, TFollower, TFollowerInit>(
        &self,
        journal: Arc<dyn Journal<Proposal<TEntry>>>,
        init: TFollowerInit,
    ) -> Participant<TEntry, TLeader, TFollower>
    where
        TEntry: Send + Sync + 'static,
        TLeader: Leader<TEntry, TFollower> + Send + Sync + 'static,
        TFollower: Follower<TEntry, TLeader> + Send + Sync + 'static,
        TFollowerInit: FollowerInit<TEntry, TFollower> + Send + Sync + 'static,
    {
        let inner = WithBackground::new(Arc::new(ParticipantInner {
            journal,
            campaign_splay: self.campaign_splay,
            heartbeat_interval: self.heartbeat_interval,
            renew_interval: self.renew_interval,
            lease_duration: self.lease_duration,
            lease_grace_period: self.lease_grace_period,
            state: AsyncRwLock::new(Some(InnerParticipantState::Follower(init.new_follower()))),
            state_change_request: Notify::new(),
            last_confirmation: AtomicInstant::new(),
            accepted_seqs: Arc::new(SeqWaiters::new()),
        }));

        inner.spawn(async move |participant| {
            Retry::new()
                .indefinitely(&|| async {
                    let participant_id = ParticipantId::new();
                    participant.background_process(participant_id).await?;
                    participant.state_change_request.notify_waiters();
                    {
                        let mut state = participant.state.write().await;
                        *state = Some(InnerParticipantState::Follower(init.new_follower()));
                    }
                    Err::<(), anyhow::Error>(anyhow!("background_process terminated"))
                })
                .await;
        });

        Participant(inner)
    }
}

struct ParticipantInner<TEntry, TLeader, TFollower> {
    journal: Arc<dyn Journal<Proposal<TEntry>>>,
    accepted_seqs: Arc<SeqWaiters>,

    campaign_splay: Duration,
    heartbeat_interval: Duration,
    renew_interval: Duration,
    lease_duration: Duration,
    lease_grace_period: Duration,

    // This is Option just for the convenience of take() when promoting/demoting.
    state: AsyncRwLock<Option<InnerParticipantState<TLeader, TFollower>>>,
    // Notified when the participant needs to be promoted or demoted, so that with_state can early
    // exit.
    state_change_request: Notify,
    // The instant of the start of the write of the last seen Acquire by us. Since the Acquire
    // takes some amount of time, it's important that we start counting from the _start_ of this
    // operation rather than the end, so that our idea of expiration is a lower bound.
    last_confirmation: AtomicInstant,
}

enum InnerParticipantState<TLeader, TFollower> {
    Leader {
        leader: TLeader,
        lease_end: Arc<AtomicTimestamp>,
    },
    Follower(TFollower),
}

impl<TLeader, TFollower> InnerParticipantState<TLeader, TFollower> {
    fn as_participant_state(&self) -> ParticipantState<'_, TLeader, TFollower> {
        match self {
            InnerParticipantState::Leader { leader, .. } => ParticipantState::Leader(&leader),
            InnerParticipantState::Follower(follower) => ParticipantState::Follower(&follower),
        }
    }
}

pub enum ParticipantState<'a, TLeader, TFollower> {
    Leader(&'a TLeader),
    Follower(&'a TFollower),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParticipantId(Uuid);

impl ParticipantId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Clone)]
pub struct Proposal<TEntry> {
    participant_id: ParticipantId,
    // Timestamps are not necessarily ordered the same way as WalSeqs, since the leader may submit
    // proposals concurrently that can be committed by the journal in any order.
    timestamp: Timestamp,
    proposal_type: ProposalType<TEntry>,
}

#[derive(Clone)]
enum ProposalType<TEntry> {
    // Acquires are only accepted if their timestamp is greater than the last non-relinquished
    // lease_end.
    Acquire { lease_end: Timestamp },
    Relinquish,
    // Appends are only accepted if they're made by the current leader.
    Append(TEntry),
    // Heartbeats are always accepted since they have no effect.
    Heartbeat,
}

pub trait FollowerInit<TEntry, TFollower> {
    fn new_follower(&self) -> TFollower;
}

#[async_trait]
pub trait Leader<TEntry, TFollower> {
    async fn demote(self) -> anyhow::Result<TFollower>;
}

#[async_trait]
pub trait Follower<TEntry, TLeader> {
    async fn process(&self, seq: WalSeq, entry: TEntry);
    async fn promote(self, writer: JournalWriter<TEntry>) -> anyhow::Result<TLeader>;
}

pub struct JournalWriter<TEntry> {
    participant_id: ParticipantId,
    journal: Arc<dyn Journal<Proposal<TEntry>>>,
    lease_end: Weak<AtomicTimestamp>,
    accepted_seqs: Arc<SeqWaiters>,
}

impl<TEntry> JournalWriter<TEntry>
where
    TEntry: Send + Sync + 'static,
{
    /// Append an entry to the log.
    pub async fn append(&self, entry: TEntry) -> anyhow::Result<()> {
        let lease_end = {
            if let Some(lease_end) = self.lease_end.upgrade() {
                lease_end.load()
            } else {
                return Err(anyhow!("cannot append: lease expired"));
            }
        };

        let ts = Timestamp::now();
        if ts > lease_end {
            return Err(anyhow!("cannot append: lease expired"));
        }

        let wait = self.accepted_seqs.register();
        let seq = self
            .journal
            .append(Proposal {
                participant_id: self.participant_id,
                timestamp: ts,
                proposal_type: ProposalType::Append(entry),
            })
            .await
            .unwrap(); // TODO: We do need to relinquish and change our participant ID etc, but
                       // unwrap is a little ungraceful.
        let accepted = wait.wait(seq).await;
        if !accepted {
            return Err(anyhow!("entry not accepted"));
        }

        return Ok(());
    }
}

impl<TEntry, TLeader, TFollower> Participant<TEntry, TLeader, TFollower>
where
    TEntry: Send + Sync + 'static,
    TLeader: Leader<TEntry, TFollower> + Send + Sync + 'static,
    TFollower: Follower<TEntry, TLeader> + Send + Sync + 'static,
{
    pub async fn with_state<F, T, E>(&self, f: F) -> Result<T, E>
    where
        F: AsyncFnOnce(ParticipantState<TLeader, TFollower>) -> Result<T, E>,
        T: Send + 'static,
        E: From<anyhow::Error> + Send + 'static,
    {
        let state_change_request = self.0.state_change_request.notified();
        let state = self.0.state.read().await;

        let out = select! {
            out =
                f(state
                .as_ref()
                .ok_or_else(|| anyhow!("no participant state present"))?
                .as_participant_state()) => {
                out?
            },
            _ = state_change_request => {
                return Err(anyhow!("aborted: participant state change requested").into());
            },
        };

        if matches!(state.deref(), Some(InnerParticipantState::Leader { .. })) {
            let last_confirmation = self.0.last_confirmation.load();

            if Instant::now().duration_since(last_confirmation)
                > self.0.lease_duration - self.0.lease_grace_period
            {
                return Err(anyhow!("lease expired before operation completed").into());
            }
        }

        Ok(out)
    }
}

impl<TEntry, TLeader, TFollower> ParticipantInner<TEntry, TLeader, TFollower>
where
    TEntry: Send + Sync + 'static,
    TLeader: Leader<TEntry, TFollower> + Send + Sync + 'static,
    TFollower: Follower<TEntry, TLeader> + Send + Sync + 'static,
{
    fn next_timestamp(&self) -> Timestamp {
        Timestamp::now()
    }

    fn propose_at(
        &self,
        participant_id: ParticipantId,
        ts: Timestamp,
        proposal_type: ProposalType<TEntry>,
    ) {
        tokio::spawn({
            let journal = Arc::clone(&self.journal);

            async move {
                let _ = journal
                    .append(Proposal {
                        participant_id,
                        timestamp: ts,
                        proposal_type,
                    })
                    .await;
            }
        });
    }

    async fn promote_or_extend(
        &self,
        participant_id: ParticipantId,
        new_lease_end: Timestamp,
    ) -> anyhow::Result<()> {
        {
            let maybe_state = self.state.read().await;
            if let Some(InnerParticipantState::Leader { lease_end, .. }) = maybe_state.deref() {
                lease_end.store(new_lease_end);
                return Ok(());
            }
        }

        self.state_change_request.notify_waiters();

        let mut maybe_state = self.state.write().await;
        if let Some(InnerParticipantState::Leader { lease_end, .. }) = maybe_state.deref() {
            lease_end.store(new_lease_end);
            return Ok(());
        }

        let state = maybe_state.take().unwrap();
        let lease_end = Arc::new(AtomicTimestamp::new(new_lease_end));
        let leader = match state {
            InnerParticipantState::Leader { .. } => unreachable!(),
            InnerParticipantState::Follower(follower) => {
                let journal_writer = JournalWriter {
                    participant_id,
                    journal: Arc::clone(&self.journal),
                    lease_end: Arc::downgrade(&lease_end),
                    accepted_seqs: Arc::clone(&self.accepted_seqs),
                };
                follower.promote(journal_writer).await?
            }
        };
        *maybe_state = Some(InnerParticipantState::Leader { leader, lease_end });

        Ok(())
    }

    async fn demote_if_leader(&self) -> anyhow::Result<()> {
        {
            let maybe_state = self.state.read().await;
            if matches!(
                maybe_state.deref(),
                Some(InnerParticipantState::Follower(_))
            ) {
                return Ok(());
            }
        }

        self.state_change_request.notify_waiters();

        let mut maybe_state = self.state.write().await;
        if matches!(
            maybe_state.deref(),
            Some(InnerParticipantState::Follower(_))
        ) {
            return Ok(());
        }

        let state = maybe_state.take().unwrap();
        let follower = match state {
            InnerParticipantState::Leader { leader, .. } => leader.demote().await?,
            InnerParticipantState::Follower(_) => unreachable!(),
        };
        *maybe_state = Some(InnerParticipantState::Follower(follower));

        Ok(())
    }

    async fn process(&self, seq: WalSeq, entry: TEntry) {
        let maybe_state = self.state.read().await;
        let state = maybe_state.as_ref().unwrap();
        if let InnerParticipantState::Follower(follower) = state {
            follower.process(seq, entry).await;
        }
    }

    async fn background_process(&self, participant_id: ParticipantId) -> anyhow::Result<()> {
        let mut accepted = accepted_proposals(
            self.journal
                .tail(self.journal.oldest_available().await?)
                .boxed(),
        )
        .boxed();

        let mut renew_ticker = ticker(self.renew_interval);
        let mut heartbeat_ticker = Box::pin(jittered_ticker(self.heartbeat_interval));
        // True if we've published a Heartbeat that we haven't observed in the stream yet.
        let mut pending_heartbeat = false;
        // Some if we've published an Acquire message that we haven't observed in the stream yet.
        let mut pending_acquire: Option<Instant> = None;

        let mut current_lease = None;

        let mut last_ts = Timestamp::now();
        let mut try_acquire = false;

        loop {
            if try_acquire && pending_acquire.is_none() {
                let ts = Timestamp::now_after(last_ts);
                last_ts = ts;
                let lease_end =
                    Timestamp::from_nanos(ts.as_nanos() + (self.lease_duration.as_nanos() as u64));
                pending_acquire = Some(Instant::now());
                self.propose_at(participant_id, ts, ProposalType::Acquire { lease_end });
                try_acquire = false;
            }

            let self_leader = match current_lease {
                Some((current_participant_id, _)) => current_participant_id == participant_id,
                _ => false,
            };

            select! {
                next = StreamExt::next(&mut accepted) => {
                    let (seq, ratification) = next
                        .transpose()?
                        .ok_or_else(|| anyhow!("journal tail ended"))?;

                    self.accepted_seqs.observe(seq);

                    match ratification {
                        Ratification::Accepted(proposal) => match proposal.proposal_type {
                            ProposalType::Acquire{lease_end, ..} => {
                                current_lease = Some((proposal.participant_id, lease_end));
                                if proposal.participant_id == participant_id {
                                    self.last_confirmation.store(
                                        pending_acquire
                                            .ok_or_else(|| {
                                                anyhow!("received acquire that was not pending")
                                            })?,
                                    );
                                    pending_acquire = None;
                                    self.promote_or_extend(participant_id, lease_end).await?;
                                } else {
                                    self.demote_if_leader().await?;
                                }
                            },
                            ProposalType::Relinquish => {
                                current_lease = None;
                                if proposal.participant_id == participant_id {
                                    self.demote_if_leader().await?;
                                }
                            },
                            ProposalType::Append(entry) => {
                                self.process(seq, entry).await;
                            },
                            ProposalType::Heartbeat => {
                                if proposal.participant_id == participant_id {
                                    pending_heartbeat = false;

                                    // Receiving a heartbeat of our own means we're as close to
                                    // 'now' in the journal as we can be, since we only ever have
                                    // one in flight.
                                    let current_lease_expired = match current_lease {
                                        Some((_, lease_end)) => Timestamp::now() > lease_end,
                                        None => true,
                                    };
                                    if current_lease_expired && pending_acquire.is_none() {
                                        try_acquire = true;
                                    }
                                }
                            },
                        },
                        Ratification::Rejected(proposal) => match proposal.proposal_type {
                            ProposalType::Acquire{..} => {
                                if proposal.participant_id == participant_id {
                                    pending_acquire = None;
                                }
                            }
                            _ => {},
                        },
                    }
                },
                _ = StreamExt::next(&mut heartbeat_ticker), if !pending_heartbeat => {
                    let ts = Timestamp::now_after(last_ts);
                    last_ts = ts;
                    self.propose_at(participant_id, ts, ProposalType::Heartbeat);
                    pending_heartbeat = true;
                },
                _ = StreamExt::next(&mut renew_ticker),
                    if self_leader && pending_acquire.is_none() => {
                    try_acquire = true;
                },
                // TODO: maybe_demote() on expiry
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

async fn maybe_sleep_until(x: Option<Instant>) {
    match x {
        Some(instant) => sleep_until(instant).await,
        None => futures::future::pending().await,
    }
}

enum Ratification<Entry> {
    Accepted(Proposal<Entry>),
    Rejected(Proposal<Entry>),
}

fn accepted_proposals<S, Entry>(
    mut proposals: S,
) -> impl Stream<Item = anyhow::Result<(WalSeq, Ratification<Entry>)>>
where
    S: Stream<Item = anyhow::Result<(WalSeq, Proposal<Entry>)>> + Send + Unpin,
{
    try_stream! {
        // NOTE: If we start anywhere in the middle we might accept an acquire that shouldn't have
        // been. We need to guarantee that a trim always happens at a position with a successful
        // acquire in it.

        let mut current_leader = None;

        while let Some((seq, proposal)) = proposals.next().await.transpose()? {
            if let ProposalType::Acquire { lease_end: new_lease_end, ..} = proposal.proposal_type {
                let accept_acquire = match current_leader {
                    Some((leader_participant_id, current_lease_end)) => {
                        // Accept if it's either a renewal by the previous leader, or if it's a new
                        // lease term after the previous one expired.
                        proposal.participant_id == leader_participant_id
                            || proposal.timestamp > current_lease_end
                    },
                    None => true,
                };

                if accept_acquire {
                    log::info!(
                        "{:?} is leader for {:?} - {:?}",
                        proposal.participant_id,
                        proposal.timestamp,
                        new_lease_end,
                    );
                    current_leader = Some((proposal.participant_id, new_lease_end));
                } else {
                    log::info!(
                        "acquire at {:?} {:?} by {:?} rejected",
                        seq,
                        proposal.timestamp,
                        proposal.participant_id,
                    );
                    yield (seq, Ratification::Rejected(proposal));
                    continue;
                }
            }

            // If this entry wasn't proposed by the current leader, or there is no leader, skip it.
            if !matches!(proposal.proposal_type, ProposalType::Heartbeat)
                && current_leader
                    .map(|(leader_participant_id, _)| {
                        proposal.participant_id != leader_participant_id
                    })
                    .unwrap_or(true)
            {
                log::info!(
                    "proposal at {:?} {:?} by {:?} rejected",
                    seq,
                    proposal.timestamp,
                    proposal.participant_id,
                );
                yield (seq, Ratification::Rejected(proposal));
                continue;
            }

            // TODO: Make sure the timestamp is below the end of the lease term. That shouldn't
            // ever happen because the leader shouldn't ever make a proposal like that.

            if let ProposalType::Relinquish = proposal.proposal_type {
                current_leader = None;
            }

            yield (seq, Ratification::Accepted(proposal));
        }
    }
}
