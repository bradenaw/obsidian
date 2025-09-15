use std::fmt::Debug;
use std::fmt::Display;
use std::time::Duration;
use std::time::SystemTime;

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Copy)]
pub struct Timestamp(pub(crate) u64);

impl Timestamp {
    pub const ZERO: Self = Timestamp(0);
    pub const MAX: Self = Timestamp(u64::MAX);

    pub fn now() -> Self {
        Timestamp::from_nanos(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("now before UNIX_EPOCH?")
                .as_nanos() as u64,
        )
    }

    pub fn now_after(other: Timestamp) -> Self {
        std::cmp::max(Timestamp(other.0 + 1), Self::now())
    }

    pub fn from_nanos(x: u64) -> Self {
        Timestamp(x)
    }

    pub fn as_nanos(&self) -> u64 {
        self.0
    }

    pub fn plus_one(&self) -> Timestamp {
        Timestamp(self.0 + 1)
    }

    pub fn minus_one(&self) -> Timestamp {
        Timestamp(self.0 - 1)
    }

    pub fn checked_duration_since(&self, earlier: Timestamp) -> Option<Duration> {
        self.0.checked_sub(earlier.0).map(Duration::from_nanos)
    }

    pub fn saturating_duration_since(&self, earlier: Timestamp) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Debug for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ts:")?;
        Display::fmt(self, f)
    }
}
