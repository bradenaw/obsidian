use crate::Timestamp;

#[derive(Clone, Copy, Debug)]
pub(crate) enum TxOutcome {
    Committed(Timestamp),
    Aborted,
}
