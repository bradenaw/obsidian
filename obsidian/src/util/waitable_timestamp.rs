use crate::util::waitable_ord::WaitableOrd;
use crate::Timestamp;

pub(crate) type WaitableTimestamp = WaitableOrd<Timestamp>;
