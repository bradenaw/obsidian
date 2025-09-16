use std::fmt::Debug;

use crate::Timestamp;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum HistoryRange {
    All,
    Until(Timestamp),
    Between(Timestamp, Timestamp),
    Since(Timestamp),
}

impl HistoryRange {
    pub(crate) fn as_min_max(&self) -> (Timestamp, Timestamp) {
        match self {
            HistoryRange::All => (Timestamp::ZERO, Timestamp::MAX),
            HistoryRange::Until(max) => (Timestamp::ZERO, *max),
            HistoryRange::Between(min, max) => (*min, *max),
            HistoryRange::Since(min) => (*min, Timestamp::MAX),
        }
    }

    pub(crate) fn contains(&self, ts: Timestamp) -> bool {
        let (min, max) = self.as_min_max();
        min <= ts && ts <= max
    }

    pub(crate) fn intersects(&self, min: Timestamp, max: Timestamp) -> bool {
        let (self_min, self_max) = self.as_min_max();
        !(self_max < min || self_min > max)
    }
}
