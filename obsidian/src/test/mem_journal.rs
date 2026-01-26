use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Mutex;

use async_stream::try_stream;
use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;
use tokio::sync::watch;

use crate::runtime::Journal;
use crate::WalSeq;

pub(crate) struct MemJournal<E> {
    inner: Mutex<MemJournalInner<E>>,
    highest_seqno_send: watch::Sender<WalSeq>,
    highest_seqno: watch::Receiver<WalSeq>,
}

struct MemJournalInner<E> {
    entries: VecDeque<E>,
    offset: WalSeq,
}

impl<E> MemJournal<E> {
    pub fn new() -> Self {
        let (highest_seqno_send, highest_seqno) = watch::channel(WalSeq(0));
        Self {
            inner: Mutex::new(MemJournalInner {
                entries: VecDeque::new(),
                // Eslewhere assumes that WalSeq(0) never exists.
                offset: WalSeq(1),
            }),
            highest_seqno_send,
            highest_seqno,
        }
    }
}

#[async_trait]
impl<E> Journal<E> for MemJournal<E>
where
    E: Clone + Send + 'static,
{
    async fn append(&self, entry: E) -> anyhow::Result<WalSeq> {
        let mut inner = self.inner.lock().unwrap();
        let seqno = WalSeq(inner.offset.0 + (inner.entries.len() as u64));
        inner.entries.push_back(entry);
        let _ = self.highest_seqno_send.send(seqno);
        Ok(seqno)
    }

    fn read(
        &self,
        first: WalSeq,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<(WalSeq, E)>> + Send + '_>> {
        Box::pin(try_stream! {
            let mut i = first;
            loop {
                let entry = {
                    let inner = self.inner.lock().unwrap();
                    if i < inner.offset {
                        Err(anyhow::anyhow!("fell behind"))
                    } else {
                        let offset = (i.0 - inner.offset.0) as usize;
                        if offset >= inner.entries.len() {
                            break;
                        }
                        Ok(inner.entries[offset].clone())
                    }
                }?;
                yield (i, entry);
                i = WalSeq(i.0+1);
            }
        })
    }

    fn tail(
        &self,
        first: WalSeq,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<(WalSeq, E)>> + Send + '_>> {
        Box::pin(try_stream! {
            let mut i = first;
            loop {
                let mut highest_seqno = self.highest_seqno.clone();
                let mut r = Box::pin(self.read(i));
                while let Some((seqno, entry)) = r.next().await.transpose()? {
                    yield (seqno, entry);
                    i = WalSeq(seqno.0 + 1);
                }
                let _ = highest_seqno
                    .changed()
                    .await;
            }
        })
    }

    async fn oldest_available(&self) -> anyhow::Result<WalSeq> {
        Ok(self.inner.lock().unwrap().offset)
    }

    async fn trim(&self, before: WalSeq) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        while inner.offset < before && !inner.entries.is_empty() {
            inner.offset = WalSeq(inner.offset.0 + 1);
            inner.entries.pop_front();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::task::Poll;

    use futures::StreamExt;
    use futures::TryStreamExt;

    use crate::runtime::Journal;
    use crate::test::MemJournal;
    use crate::Timestamp;
    use crate::WalEntry;
    use crate::WalSeq;

    fn wal_entry(i: usize) -> WalEntry {
        WalEntry::Write(Timestamp(i as u64), vec![])
    }

    fn write_timestamp(x: (WalSeq, WalEntry)) -> (WalSeq, Timestamp) {
        if let WalEntry::Write(ts, _) = x.1 {
            (x.0, ts)
        } else {
            panic!();
        }
    }

    fn write_timestamps(
        iter: impl IntoIterator<Item = (WalSeq, WalEntry)>,
    ) -> Vec<(WalSeq, Timestamp)> {
        iter.into_iter().map(write_timestamp).collect()
    }

    #[tokio::test]
    async fn test_mem_wal() -> anyhow::Result<()> {
        let wal = MemJournal::new();

        assert_eq!(wal.append(wal_entry(1)).await?, WalSeq(1));
        assert_eq!(wal.append(wal_entry(2)).await?, WalSeq(2));
        assert_eq!(wal.append(wal_entry(3)).await?, WalSeq(3));

        assert_eq!(wal.oldest_available().await?, WalSeq(1));

        assert_eq!(
            write_timestamps(wal.read(WalSeq(2)).try_collect::<Vec<_>>().await?),
            vec![(WalSeq(2), Timestamp(2)), (WalSeq(3), Timestamp(3))]
        );

        let mut tail = wal.tail(WalSeq(2));

        assert_eq!(
            tail.try_next().await?.map(write_timestamp),
            Some((WalSeq(2), Timestamp(2)))
        );
        assert_eq!(
            tail.try_next().await?.map(write_timestamp),
            Some((WalSeq(3), Timestamp(3)))
        );

        assert_matches!(futures::poll!(tail.next()), Poll::Pending);

        assert_eq!(wal.append(wal_entry(4)).await?, WalSeq(4));

        assert_eq!(
            tail.try_next().await?.map(write_timestamp),
            Some((WalSeq(4), Timestamp(4)))
        );

        wal.trim(WalSeq(3)).await?;

        assert_eq!(wal.oldest_available().await?, WalSeq(3));
        assert_matches!(wal.read(WalSeq(2)).next().await, Some(Err(_)));
        assert_eq!(
            write_timestamps(wal.read(WalSeq(3)).try_collect::<Vec<_>>().await?),
            vec![(WalSeq(3), Timestamp(3)), (WalSeq(4), Timestamp(4))]
        );

        Ok(())
    }
}
