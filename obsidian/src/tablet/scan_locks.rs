use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Mutex;

use tokio::sync::oneshot;

/// ScanLocks resolves one very specific race. For gets and writes, we can rely just on key locks
/// to serialize everything we might need. For scans specifically, there's a race between scan
/// and cleanup_pending_key. cleanup_pending_key upgrades a pending record into a real one by
/// removing and re-adding. This is non-transactional, so without ScanLocks it's possible for a
/// scan to see neither the pending record nor the real record.
///
/// e.g.
/// scan_page                                    cleanup_pending_key
/// ------------------------------------------------------------------------------------------------
/// scan real records
///                                              remove pending record
///                                              add real record
/// scan pending records
///
/// In order to resolve this, we have cleanup_pending_key add the real record first and then wait
/// for all in-progress scans to complete before removing the pending record. This guarantees that
/// the scans will all see at least one of the pair. (Some may see both, but that's fine.)
///
/// This notably delays the cleanup by, on average, half of a scan latency. During this time, even
/// all future reads of the key being cleaned up will be delayed. This could be mitigated by
/// ignoring conflicts for which a newer record for the same key is already present in the real
/// results.
pub(crate) struct ScanLocks {
    inner: Mutex<ScanLocksInner>,
}

impl ScanLocks {
    pub fn new() -> Self {
        ScanLocks {
            inner: Mutex::new(ScanLocksInner {
                next_seq: 0,
                scans: BTreeSet::new(),
                cleanups: BTreeMap::new(),
            }),
        }
    }

    pub fn scan(&self) -> ScanGuard<'_> {
        let mut inner = self.inner.lock().unwrap();
        let seq = inner.next_seq();
        inner.scans.insert(seq);

        ScanGuard { parent: self, seq }
    }

    pub fn cleanup(&self) -> CleanupGuard<'_> {
        let mut inner = self.inner.lock().unwrap();
        let seq = inner.next_seq();

        if inner.scans.is_empty() {
            return CleanupGuard {
                parent: self,
                seq,
                recv: None,
            };
        }

        let (send, recv) = oneshot::channel();
        inner.cleanups.insert(seq, send);
        CleanupGuard {
            parent: self,
            seq,
            recv: Some(recv),
        }
    }
}

struct ScanLocksInner {
    // Used to vend sequence numbers for operations so we can keep them in order.
    next_seq: usize,

    // Keys in both are sequence numbers.
    //
    // There are never any cleanups with a sequence number lower than the lowest scan, because that
    // means they need to be woken up already.
    scans: BTreeSet<usize>,
    cleanups: BTreeMap<usize, oneshot::Sender<()>>,
}

impl ScanLocksInner {
    fn next_seq(&mut self) -> usize {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }
}

pub(crate) struct ScanGuard<'a> {
    parent: &'a ScanLocks,
    seq: usize,
}

impl<'a> Drop for ScanGuard<'a> {
    fn drop(&mut self) {
        let mut inner = self.parent.inner.lock().unwrap();
        inner.scans.remove(&self.seq);
        let lowest_scan = inner.scans.first().copied().unwrap_or(usize::MAX);
        while let Some(entry) = inner.cleanups.first_entry() {
            let cleanup_seq = *entry.key();
            if cleanup_seq >= lowest_scan {
                break;
            }
            let cleanup_wake = entry.remove();
            let _ = cleanup_wake.send(());
        }
    }
}

pub(crate) struct CleanupGuard<'a> {
    parent: &'a ScanLocks,
    seq: usize,
    recv: Option<oneshot::Receiver<()>>,
}

impl<'a> CleanupGuard<'a> {
    pub async fn wait(mut self) {
        if let Some(recv) = self.recv.take() {
            let _ = recv.await;
        }
    }
}

impl<'a> Drop for CleanupGuard<'a> {
    fn drop(&mut self) {
        self.parent.inner.lock().unwrap().cleanups.remove(&self.seq);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::spawn;
    use tokio::sync::oneshot;

    use crate::tablet::scan_locks::ScanLocks;

    #[tokio::test]
    async fn test_no_scans() {
        let scan_locks = ScanLocks::new();
        let cleanup_guard = scan_locks.cleanup();
        cleanup_guard.wait().await;
    }

    #[tokio::test]
    async fn test_after_scan() {
        let scan_locks = ScanLocks::new();
        let cleanup_guard = scan_locks.cleanup();
        {
            scan_locks.scan();
        }
        cleanup_guard.wait().await;
    }

    #[tokio::test]
    async fn test_concurrent_scans() {
        let scan_locks = Arc::new(ScanLocks::new());
        let scan_guard_0 = scan_locks.scan();
        let scan_guard_1 = scan_locks.scan();

        let (send, recv) = oneshot::channel();
        let join_handle = spawn({
            let scan_locks = Arc::clone(&scan_locks);
            async move {
                let cleanup_guard = scan_locks.cleanup();
                let _ = send.send(());
                cleanup_guard.wait().await;
            }
        });

        recv.await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(scan_guard_0);
        drop(scan_guard_1);

        join_handle.await.unwrap();
    }
}
