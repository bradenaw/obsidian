use std::fmt::Debug;

use thiserror::Error;

use crate::TabletId;
use crate::Txid;

#[derive(Error, Debug)]
pub(crate) enum InternalError {
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
    #[error("node not currently leader for tablet {0:?}")]
    NotLeader(TabletId),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
