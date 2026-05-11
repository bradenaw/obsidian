#![feature(async_fn_traits)]
#![feature(unboxed_closures)]

mod atomic_instant;
mod background;
mod binary_search_by_idx;
mod bytes;
mod iterator_either;
mod merge_sorted;
mod ord_eq_by_first;
mod owned;
mod retry;
mod state_machine;
mod time;
mod waitable_ord;
mod watchable;

pub use atomic_instant::AtomicInstant;
pub use background::spawn_owned;
pub use background::Background;
pub use background::OwnedJoinHandle;
pub use background::OwnedWithBackground;
pub use background::WithBackground;
pub use binary_search_by_idx::binary_search_by_idx;
pub use bytes::byte_width;
pub use bytes::encode;
pub use bytes::hexlify;
pub use bytes::longest_shared_prefix;
pub use bytes::longest_shared_prefix_len;
pub use bytes::shortest_between;
pub use bytes::Decode;
pub use bytes::Encode;
pub use iterator_either::IteratorEither;
pub use merge_sorted::merge_sorted;
pub use merge_sorted::merge_sorted2;
pub use merge_sorted::merge_sorted_streams;
pub use ord_eq_by_first::OrdEqByFirst;
pub use owned::Owned;
pub use owned::WeakView;
pub use retry::sleep_for_retry;
pub use retry::Retry;
pub use retry::RetryResult;
pub use state_machine::StateMachine;
pub use time::jittered_ticker;
pub use waitable_ord::WaitableOrd;
pub use watchable::Watchable;
