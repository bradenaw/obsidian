use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Debug;

use anyhow::anyhow;
use obsidian_pb as pb;
use thiserror::Error;

use crate::Key;
use crate::Record;
use crate::ShardId;
use crate::TabletId;
use crate::Txid;

#[derive(Error, Debug)]
pub enum InternalError {
    #[error("conflict")]
    Conflict(Txid),
    #[error("already committed")]
    AlreadyCommitted,
    #[error("already aborted")]
    AlreadyAborted,
    #[error("precondition failed")]
    PreconditionFailed,
    // Can happen on an attempt at a wait() if Tablet::cleanup_committed_outcomes already
    // cleaned everything up and removed the TxOutcome.
    #[error("TxOutcome missing")]
    TxOutcomeMissing,
    #[error("tablet not currently readable")]
    TabletNotReadable(TabletId),
    #[error("tablet not currently writable")]
    TabletNotWriteable(TabletId),
    #[error("tablet not currently hydrating")]
    TabletNotHydrating(TabletId),
    /// Gets with multiple keys may produce results too large to return. Instead, PartialGet is
    /// returned with some of the results, and the get can be repeated with the remaining keys.
    #[error("partial get")]
    PartialGet {
        /// Results for the requested keys, except for the keys in `remaining`.
        results: BTreeMap<Key, Record>,
        /// Keys that were not read in the course of the get.
        remaining: BTreeSet<Key>,
    },
    #[error("node not currently leader for {0:?}")]
    NotLeader(ShardId),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl TryFrom<pb::internal::InternalError> for InternalError {
    type Error = anyhow::Error;

    fn try_from(value: pb::internal::InternalError) -> Result<Self, Self::Error> {
        use pb::internal::internal_error::ErrorType;
        Ok(
            match value
                .error_type
                .ok_or_else(|| anyhow!("missing InternalError.error_type"))?
            {
                ErrorType::Conflict(conflict) => InternalError::Conflict(Txid::try_from(
                    conflict
                        .txid
                        .ok_or_else(|| anyhow!("missing Conflict.txid"))?,
                )?),
                ErrorType::AlreadyCommitted(_) => InternalError::AlreadyCommitted,
                ErrorType::AlreadyAborted(_) => InternalError::AlreadyAborted,
                ErrorType::PreconditionFailed(_) => InternalError::PreconditionFailed,
                ErrorType::TxOutcomeMissing(_) => InternalError::TxOutcomeMissing,
                ErrorType::TabletNotReadable(tablet_id_error) => {
                    InternalError::TabletNotReadable(parse_tablet_id_error(tablet_id_error)?)
                }
                ErrorType::TabletNotWriteable(tablet_id_error) => {
                    InternalError::TabletNotWriteable(parse_tablet_id_error(tablet_id_error)?)
                }
                ErrorType::TabletNotHydrating(tablet_id_error) => {
                    InternalError::TabletNotHydrating(parse_tablet_id_error(tablet_id_error)?)
                }
                ErrorType::NotLeader(shard_id_error) => {
                    InternalError::NotLeader(ShardId(shard_id_error.shard_id))
                }
                ErrorType::Other(msg) => InternalError::Other(anyhow::Error::msg(msg)),
            },
        )
    }
}

fn parse_tablet_id_error(
    value: pb::internal::internal_error::TabletIdError,
) -> anyhow::Result<TabletId> {
    TabletId::try_from(
        value
            .tablet_id
            .ok_or_else(|| anyhow!("missing TabletIdError.tablet_id"))?,
    )
}

impl TryFrom<InternalError> for pb::internal::InternalError {
    type Error = anyhow::Error;

    fn try_from(value: InternalError) -> Result<Self, Self::Error> {
        use pb::internal::internal_error::ErrorType;
        Ok(pb::internal::InternalError {
            error_type: Some(match value {
                InternalError::Conflict(txid) => {
                    ErrorType::Conflict(pb::internal::internal_error::Conflict {
                        txid: Some(txid.into()),
                    })
                }
                InternalError::AlreadyCommitted => ErrorType::AlreadyCommitted(()),
                InternalError::AlreadyAborted => ErrorType::AlreadyAborted(()),
                InternalError::PreconditionFailed => ErrorType::PreconditionFailed(()),
                InternalError::TxOutcomeMissing => ErrorType::TxOutcomeMissing(()),
                InternalError::TabletNotReadable(tablet_id) => {
                    ErrorType::TabletNotReadable(pb::internal::internal_error::TabletIdError {
                        tablet_id: Some(tablet_id.into()),
                    })
                }
                InternalError::TabletNotWriteable(tablet_id) => {
                    ErrorType::TabletNotWriteable(pb::internal::internal_error::TabletIdError {
                        tablet_id: Some(tablet_id.into()),
                    })
                }
                InternalError::TabletNotHydrating(tablet_id) => {
                    ErrorType::TabletNotHydrating(pb::internal::internal_error::TabletIdError {
                        tablet_id: Some(tablet_id.into()),
                    })
                }
                InternalError::PartialGet { .. } => {
                    return Err(anyhow!(
                        "InternalError::PartialGet is too large to fit into the InternalError proto, it should be getting communicated with the GetResult proto",
                    ));
                }
                InternalError::NotLeader(shard_id) => {
                    ErrorType::NotLeader(pb::internal::internal_error::ShardIdError {
                        shard_id: shard_id.0,
                    })
                }
                InternalError::Other(error) => ErrorType::Other(error.to_string()),
            }),
        })
    }
}
