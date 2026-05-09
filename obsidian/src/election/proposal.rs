use anyhow::anyhow;
use obsidian_common::uuid_from_proto;
use obsidian_common::uuid_to_proto;
use obsidian_pb as pb;
use obsidian_util::encode;
use obsidian_util::Decode;
use obsidian_util::Encode;
use prost::Message as _;

use crate::election::ParticipantId;
use crate::Timestamp;

#[derive(Clone)]
pub(crate) struct Proposal<TEntry> {
    pub(super) participant_id: ParticipantId,
    // Timestamps are not necessarily ordered the same way as JournalSeqs, since the leader may submit
    // proposals concurrently that can be committed by the journal in any order.
    pub(super) timestamp: Timestamp,
    pub(super) proposal_type: ProposalType<TEntry>,
}

impl<TEntry> From<Proposal<TEntry>> for pb::internal::Proposal
where
    TEntry: Encode,
{
    fn from(value: Proposal<TEntry>) -> Self {
        Self {
            participant_id: Some(uuid_to_proto(value.participant_id.0)),
            ts: value.timestamp.as_micros(),
            proposal_type: Some(match value.proposal_type {
                ProposalType::Acquire { lease_end } => {
                    pb::internal::proposal::ProposalType::Acquire(pb::internal::proposal::Acquire {
                        lease_end: lease_end.as_micros(),
                    })
                }
                ProposalType::Relinquish => pb::internal::proposal::ProposalType::Relinquish(()),
                ProposalType::Append(entry) => {
                    pb::internal::proposal::ProposalType::Append(pb::internal::proposal::Append {
                        entry: encode(&entry),
                    })
                }
                ProposalType::Heartbeat => pb::internal::proposal::ProposalType::Heartbeat(()),
            }),
        }
    }
}

impl<TEntry> TryFrom<pb::internal::Proposal> for Proposal<TEntry>
where
    TEntry: Decode,
{
    type Error = anyhow::Error;

    fn try_from(value: pb::internal::Proposal) -> Result<Self, Self::Error> {
        Ok(Self {
            participant_id: ParticipantId(uuid_from_proto(
                value
                    .participant_id
                    .ok_or_else(|| anyhow!("missing participant_id"))?,
            )),
            timestamp: Timestamp::from_micros(value.ts),
            proposal_type: match value
                .proposal_type
                .ok_or_else(|| anyhow!("missing proposal_type"))?
            {
                pb::internal::proposal::ProposalType::Acquire(acquire_pb) => {
                    ProposalType::Acquire {
                        lease_end: Timestamp::from_micros(acquire_pb.lease_end),
                    }
                }
                pb::internal::proposal::ProposalType::Relinquish(_) => ProposalType::Relinquish,
                pb::internal::proposal::ProposalType::Append(append_pb) => {
                    ProposalType::Append(TEntry::decode(&append_pb.entry)?)
                }
                pb::internal::proposal::ProposalType::Heartbeat(_) => ProposalType::Heartbeat,
            },
        })
    }
}

impl<TEntry> Encode for Proposal<TEntry>
where
    TEntry: Encode + Clone,
{
    fn encoded_size_estimate(&self) -> usize {
        0
    }

    fn encode(&self, w: &mut Vec<u8>) {
        let f = self.clone();
        pb::internal::Proposal::from(f)
            .encode(w)
            .expect("only fails if the buffer is too small")
    }
}

impl<TEntry> Decode for Proposal<TEntry>
where
    TEntry: Decode,
{
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        Proposal::try_from(pb::internal::Proposal::decode(b)?)
    }
}

#[derive(Clone)]
pub(super) enum ProposalType<TEntry> {
    // Acquires are only accepted if their timestamp is greater than the last non-relinquished
    // lease_end.
    Acquire { lease_end: Timestamp },
    Relinquish,
    // Appends are only accepted if they're made by the current leader.
    Append(TEntry),
    // Heartbeats are always accepted since they have no effect.
    Heartbeat,
}
