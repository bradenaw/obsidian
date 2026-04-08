use anyhow::anyhow;

use crate::pb;
use crate::Timestamp;

#[derive(Clone, Copy, Debug)]
pub(crate) enum TxOutcome {
    Committed(Timestamp),
    Aborted,
}

impl TryFrom<pb::internal::TxOutcome> for TxOutcome {
    type Error = anyhow::Error;

    fn try_from(value: pb::internal::TxOutcome) -> Result<Self, Self::Error> {
        Ok(
            match value
                .outcome
                .ok_or_else(|| anyhow!("missing TxOutcome.outcome"))?
            {
                pb::internal::tx_outcome::Outcome::Committed(ts) => {
                    TxOutcome::Committed(Timestamp::from_micros(ts))
                }
                pb::internal::tx_outcome::Outcome::Aborted(_) => TxOutcome::Aborted,
            },
        )
    }
}

impl From<TxOutcome> for pb::internal::TxOutcome {
    fn from(value: TxOutcome) -> Self {
        match value {
            TxOutcome::Committed(ts) => pb::internal::TxOutcome {
                outcome: Some(pb::internal::tx_outcome::Outcome::Committed(ts.as_micros())),
            },
            TxOutcome::Aborted => pb::internal::TxOutcome {
                outcome: Some(pb::internal::tx_outcome::Outcome::Aborted(())),
            },
        }
    }
}
