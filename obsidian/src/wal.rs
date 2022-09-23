use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use async_stream::stream;
use futures::future;
use futures::Stream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub(crate) struct Wal<E> {
    inner: Arc<RwLock<WalInner<E>>>,

    reqs: mpsc::Sender<(E, oneshot::Sender<SeqNo>)>,
    highest_seqno: watch::Receiver<SeqNo>,
    process_handle: JoinHandle<()>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SeqNo(pub u64);

impl<E: Entry + Clone + Send + Sync + 'static> Wal<E> {
    pub fn new(buffer_size: u64, buffer_duration: Duration) -> Self {
        let (reqs_send, reqs_recv) = mpsc::channel(1);
        let (highest_seqno_send, highest_seqno_recv) = watch::channel(SeqNo(0));
        let inner = Arc::new(RwLock::new(WalInner::new()));

        let process_handle = tokio::spawn(Self::process(
            inner.clone(),
            buffer_size,
            buffer_duration,
            reqs_recv,
            highest_seqno_send,
        ));

        Self {
            inner,
            reqs: reqs_send,
            highest_seqno: highest_seqno_recv,
            process_handle,
        }
    }

    pub async fn append(&self, e: E) -> anyhow::Result<SeqNo> {
        let (done_send, done_recv) = oneshot::channel();
        self.reqs
            .send((e, done_send))
            .await
            .map_err(|_| anyhow::anyhow!("Wal processor gone missing"))?;
        let seqno = done_recv
            .await
            .map_err(|_| anyhow::anyhow!("Wal processor gone missing"))?;
        Ok(seqno)
    }

    pub async fn trim(&self, last: SeqNo) -> anyhow::Result<()> {
        let mut inner = self.inner.write().unwrap();
        while inner.offset <= last {
            inner.entries.pop_front();
            inner.offset = SeqNo(inner.offset.0 + 1);
        }
        Ok(())
    }

    pub fn stream(
        &self,
        first: SeqNo,
    ) -> impl Stream<Item = anyhow::Result<(SeqNo, E)>> + Send + '_ {
        stream! {
            let mut highest_seqno = self.highest_seqno.clone();
            let mut i = first;
            loop {
                loop {
                    let maybe_entry = {
                        let inner = self.inner.read().unwrap();
                        let (min_seqno, max_seqno_plus_one) = inner.seqno_range();
                        if i < min_seqno {
                            Err(anyhow::anyhow!("fell behind"))
                        } else {
                            if i >= max_seqno_plus_one {
                                // Run off the edge, so must wait for new ones to be written.
                                break;
                            }
                            Ok((i, inner.entry(i).unwrap().clone()))
                        }
                    };
                    let is_err = maybe_entry.is_err();
                    yield maybe_entry;
                    if is_err {
                        return;
                    }
                    i = SeqNo(i.0 + 1);
                }
                highest_seqno
                    .changed()
                    .await
                    .map_err(|_| anyhow::anyhow!("Wal processor gone missing"))?;
            }
        }
    }

    async fn process(
        inner: Arc<RwLock<WalInner<E>>>,
        max_buffer_size: u64,
        max_buffer_duration: Duration,
        mut reqs: mpsc::Receiver<(E, oneshot::Sender<SeqNo>)>,
        mut highest_seqno: watch::Sender<SeqNo>,
    ) {
        let mut timer = future::Either::Left(future::pending::<()>());
        let mut buffer = vec![];
        let mut buffer_size = 0u64;

        fn flush<E>(
            inner_lock: &RwLock<WalInner<E>>,
            buffer: &mut Vec<(E, oneshot::Sender<SeqNo>)>,
            buffer_size: &mut u64,
            highest_seqno: &mut watch::Sender<SeqNo>,
        ) {
            let last_seqno = {
                let mut inner = inner_lock.write().unwrap();
                let mut last_seqno: Option<SeqNo> = None;
                for (entry, sender) in buffer.drain(..) {
                    let seqno = SeqNo(inner.offset.0 + (inner.entries.len() as u64));
                    inner.entries.push_back(entry);
                    _ = sender.send(seqno);
                    last_seqno = Some(seqno);
                }
                last_seqno
            };
            if let Some(seqno) = last_seqno {
                _ = highest_seqno.send(seqno);
            }
            *buffer_size = 0;
        }

        loop {
            tokio::select! {
                Some((entry, sender)) = reqs.recv() => {
                    let entry_size = entry.size();
                    if !buffer.is_empty() && buffer_size + entry_size > max_buffer_size {
                        flush(&inner, &mut buffer, &mut buffer_size, &mut highest_seqno);
                    }
                    buffer_size += entry_size;
                    buffer.push((entry, sender));
                    if buffer_size >= max_buffer_size {
                        flush(&inner, &mut buffer, &mut buffer_size, &mut highest_seqno);
                    }
                    if buffer.len() == 1 {
                        timer = future::Either::Right(Box::pin(
                            tokio::time::sleep(max_buffer_duration),
                        ));
                    }
                }
                _ = &mut timer => {
                    flush(&inner, &mut buffer, &mut buffer_size, &mut highest_seqno);
                    timer = future::Either::Left(future::pending::<()>());
                }
            }
        }
    }
}

impl<E> Drop for Wal<E> {
    fn drop(&mut self) {
        self.process_handle.abort();
    }
}

pub(crate) trait Entry {
    fn size(&self) -> u64;
}

struct WalInner<E> {
    entries: VecDeque<E>,
    offset: SeqNo,
}

impl<E> WalInner<E> {
    fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            offset: SeqNo(0),
        }
    }

    fn entry(&self, seqno: SeqNo) -> Option<&E> {
        if seqno < self.offset {
            return None;
        }
        let idx = (seqno.0 - self.offset.0) as usize;
        if idx >= self.entries.len() {
            return None;
        }
        Some(&self.entries[idx])
    }

    fn seqno_range(&self) -> (SeqNo, SeqNo) {
        (
            self.offset,
            SeqNo(self.offset.0 + (self.entries.len() as u64)),
        )
    }
}

#[cfg(test)]
mod test {
    use std::task::Poll;
    use std::time::Duration;

    use futures::future;
    use futures::stream::StreamExt;
    use futures::stream::TryStreamExt;

    use super::Entry;
    use super::SeqNo;
    use super::Wal;

    impl Entry for u64 {
        fn size(&self) -> u64 {
            1
        }
    }

    #[tokio::test]
    async fn test_basic() -> anyhow::Result<()> {
        let wal = Wal::<u64>::new(3, Duration::from_millis(10000000));

        let mut s = wal.stream(SeqNo(0)).boxed_local();

        assert!(matches!(futures::poll!(s.next()), Poll::Pending));

        future::try_join3(wal.append(5), wal.append(6), wal.append(7)).await?;

        assert_eq!(s.try_next().await?, Some((SeqNo(0), 5)));
        assert_eq!(s.try_next().await?, Some((SeqNo(1), 6)));
        assert_eq!(s.try_next().await?, Some((SeqNo(2), 7)));

        Ok(())
    }

    #[tokio::test]
    async fn test_flush_timeout() -> anyhow::Result<()> {
        let wal = Wal::<u64>::new(1000, Duration::from_millis(10));

        let mut s = wal.stream(SeqNo(0)).boxed_local();

        assert!(matches!(futures::poll!(s.next()), Poll::Pending));

        future::try_join3(wal.append(5), wal.append(6), wal.append(7)).await?;

        assert_eq!(s.try_next().await?, Some((SeqNo(0), 5)));
        assert_eq!(s.try_next().await?, Some((SeqNo(1), 6)));
        assert_eq!(s.try_next().await?, Some((SeqNo(2), 7)));

        Ok(())
    }
}
