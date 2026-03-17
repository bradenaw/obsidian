use std::cmp::max;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;

use priority_queue::PriorityQueue;
use tokio::sync::oneshot;

use crate::JournalSeq;

/// Journal appenders want to wait to see that their journal entry is accepted.
///
/// This is complicated somewhat by the fact that we process the journal in another task, and that
/// appenders do not know what sequence number their entry is assigned until after the append
/// succeeds, by which point we may have already processed the entry.
///
/// In order to resolve this, waiters register themselves as waiting for some unknown sequence
/// number in the future. We hold onto all accepted sequence numbers we see that are higher than
/// the lowest of these registered waiters. Once the waiter finds out the actual sequence number
/// it's waiting for, we can swap to just looking for that exact one.
pub(super) struct SeqWaiters(Arc<Mutex<SeqWaitersInner>>);

struct SeqWaitersInner {
    next_id: usize,
    highest_seen: JournalSeq,

    // Map from waiter ID to the lower bound of the JournalSeq they're waiting for.
    waiting_unknown_above: PriorityQueue<usize, JournalSeq>,
    // Map from (waiter ID, seq) to a sender to notify them. Only contains waiters with a seq
    // higher than highest_seen.
    waiting_specific: BTreeMap<(JournalSeq, usize), oneshot::Sender<bool>>,
    // Only holds the seqs seen above the minimum seq contained in `waiting_unknown_above`. If
    // `waiting_unknown_above` is empty, so is `seen`.
    seen: BTreeSet<JournalSeq>,
}

impl SeqWaiters {
    pub fn new() -> SeqWaiters {
        SeqWaiters(Arc::new(Mutex::new(SeqWaitersInner {
            next_id: 0,
            highest_seen: JournalSeq(0),
            waiting_unknown_above: PriorityQueue::new(),
            waiting_specific: BTreeMap::new(),
            seen: BTreeSet::new(),
        })))
    }

    pub fn register(&self) -> SeqWaiter {
        let id = {
            let mut inner = self.0.lock().unwrap();
            let id = inner.next_id;
            inner.next_id += 1;
            let highest_seen = inner.highest_seen;
            inner.waiting_unknown_above.push(id, highest_seen);

            id
        };

        SeqWaiter {
            id,
            parent: Arc::clone(&self.0),
        }
    }

    pub fn observe(&self, seq: JournalSeq) {
        let mut inner = self.0.lock().unwrap();
        if !inner.waiting_unknown_above.is_empty() {
            inner.seen.insert(seq);
        }
        while let Some(entry) = inner.waiting_specific.first_entry() {
            if entry.key().0 > seq {
                break;
            }
            let matched = entry.key().0 == seq;
            let _ = entry.remove().send(matched);
        }
        inner.highest_seen = max(inner.highest_seen, seq);
    }
}

impl Default for SeqWaiters {
    fn default() -> Self {
        Self::new()
    }
}

impl SeqWaitersInner {
    fn remove_unknown_waiter(&mut self, id: usize) {
        self.waiting_unknown_above.remove(&id);
        if let Some(min_waiter) = self.waiting_unknown_above.peek().map(|(_, seq)| *seq) {
            while self
                .seen
                .first()
                .map(|min_seen_entry| *min_seen_entry < min_waiter)
                .unwrap_or(false)
            {
                self.seen.pop_first();
            }
        } else {
            self.seen.retain(|_| false);
        }
    }
}

pub(super) struct SeqWaiter {
    id: usize,
    parent: Arc<Mutex<SeqWaitersInner>>,
}

impl SeqWaiter {
    pub async fn wait(self, seq: JournalSeq) -> bool {
        let recv = {
            let mut inner = self.parent.lock().unwrap();
            let already_seen = inner.seen.contains(&seq);

            inner.remove_unknown_waiter(self.id);

            if already_seen {
                return true;
            }
            // We already saw a higher sequence number and didn't see the one we wanted. Since
            // observations are monotonic, this means we can't ever see it in the future.
            if inner.highest_seen > seq {
                return false;
            }

            let (send, recv) = oneshot::channel();

            inner.waiting_specific.insert((seq, self.id), send);
            recv
        };
        // This only returns an error when the sender is dropped, which we never do without
        // sending.
        recv.await.unwrap()
    }
}

impl Drop for SeqWaiter {
    fn drop(&mut self) {
        let mut inner = self.parent.lock().unwrap();
        inner.remove_unknown_waiter(self.id);
    }
}

#[cfg(test)]
mod tests {
    use crate::election::seq_waiters::SeqWaiters;
    use crate::JournalSeq;

    #[tokio::test]
    async fn test_seq_waiters() {
        let waiters = SeqWaiters::new();
        let w1 = waiters.register();
        let w3 = waiters.register();
        let w4 = waiters.register();
        let w7 = waiters.register();
        let w8 = waiters.register();

        waiters.observe(JournalSeq(1));
        waiters.observe(JournalSeq(4));
        assert_eq!(w1.wait(JournalSeq(1)).await, true);
        assert_eq!(w3.wait(JournalSeq(3)).await, false);
        assert_eq!(w4.wait(JournalSeq(4)).await, true);

        let w7_fut = w7.wait(JournalSeq(7));
        let w8_fut = w8.wait(JournalSeq(8));

        waiters.observe(JournalSeq(8));
        assert_eq!(w7_fut.await, false);
        assert_eq!(w8_fut.await, true);
    }
}
