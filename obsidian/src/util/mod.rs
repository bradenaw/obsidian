mod atomic_instant;
mod atomic_timestamp;
mod background;
mod bytes;
mod compressed_key_set;
mod futures;
mod iterator_either;
mod merge_sorted;
mod ord_eq_by_first;
mod owned;
mod pause;
mod retry;
mod waitable_ord;
mod waitable_timestamp;
mod watchable;

use std::cmp::Ordering;

pub(crate) use atomic_instant::AtomicInstant;
pub(crate) use atomic_timestamp::AtomicTimestamp;
pub(crate) use background::spawn_owned;
pub(crate) use background::Background;
pub(crate) use background::OwnedJoinHandle;
pub(crate) use background::WithBackground;
pub(crate) use bytes::byte_width;
pub(crate) use bytes::encode;
pub(crate) use bytes::hexlify;
pub(crate) use bytes::longest_shared_prefix;
pub(crate) use bytes::longest_shared_prefix_len;
pub(crate) use bytes::shortest_between;
pub(crate) use bytes::Decode;
pub(crate) use bytes::Encode;
pub(crate) use futures::wait_all;
pub(crate) use iterator_either::IteratorEither;
pub(crate) use merge_sorted::merge_sorted;
#[allow(unused_imports)]
pub(crate) use merge_sorted::merge_sorted2;
pub(crate) use merge_sorted::merge_sorted_streams;
pub(crate) use ord_eq_by_first::OrdEqByFirst;
pub(crate) use owned::Owned;
pub(crate) use owned::WeakView;
pub(crate) use pause::Pause;
pub(crate) use retry::sleep_for_retry;
pub(crate) use retry::Retry;
pub(crate) use retry::RetryResult;
#[allow(unused_imports)]
pub(crate) use waitable_ord::WaitableOrd;
pub(crate) use waitable_timestamp::WaitableTimestamp;
#[allow(unused_imports)]
pub(crate) use watchable::Watchable;

pub(crate) fn binary_search_by_idx<K: Ord, F: Fn(usize) -> K>(
    n: usize,
    k: K,
    f: F,
) -> Result<usize, usize> {
    let mut lower = 0;
    let mut upper = n;
    while lower < upper {
        let mid = (lower + upper) / 2;
        let at_mid = f(mid);
        match k.cmp(&at_mid) {
            Ordering::Equal => return Ok(mid),
            Ordering::Less => upper = mid,
            Ordering::Greater => lower = mid + 1,
        }
    }
    Err(lower)
}
