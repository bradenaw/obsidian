use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Mutex;

use async_stream::try_stream;
use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;
use obsidian_common::JournalSeq;
use tokio::sync::watch;

use crate::Journal;

pub struct MemJournal<E> {
    inner: Mutex<MemJournalInner<E>>,
    highest_seqno_send: watch::Sender<JournalSeq>,
    highest_seqno: watch::Receiver<JournalSeq>,
}

struct MemJournalInner<E> {
    entries: VecDeque<E>,
    offset: JournalSeq,
}

impl<E> MemJournal<E>
where
    E: Clone + Send + 'static,
{
    pub fn new() -> Self {
        let (highest_seqno_send, highest_seqno) = watch::channel(JournalSeq(0));
        Self {
            inner: Mutex::new(MemJournalInner {
                entries: VecDeque::new(),
                // Eslewhere assumes that JournalSeq(0) never exists.
                offset: JournalSeq(1),
            }),
            highest_seqno_send,
            highest_seqno,
        }
    }

    fn read(
        &self,
        first: JournalSeq,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<(JournalSeq, E)>> + Send + '_>> {
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
                i = JournalSeq(i.0+1);
            }
        })
    }
}

impl<E> Default for MemJournal<E>
where
    E: Clone + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<E> Journal<E> for MemJournal<E>
where
    E: Clone + Send + 'static,
{
    async fn append(&self, entry: E) -> anyhow::Result<JournalSeq> {
        let mut inner = self.inner.lock().unwrap();
        let seqno = JournalSeq(inner.offset.0 + (inner.entries.len() as u64));
        inner.entries.push_back(entry);
        let _ = self.highest_seqno_send.send(seqno);
        Ok(seqno)
    }

    fn tail(
        &self,
        first: JournalSeq,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<(JournalSeq, E)>> + Send + '_>> {
        Box::pin(try_stream! {
            let mut i = first;
            loop {
                let mut highest_seqno = self.highest_seqno.clone();
                let mut r = Box::pin(self.read(i));
                while let Some((seqno, entry)) = r.next().await.transpose()? {
                    yield (seqno, entry);
                    i = JournalSeq(seqno.0 + 1);
                }
                let _ = highest_seqno
                    .changed()
                    .await;
            }
        })
    }

    async fn latest(&self) -> anyhow::Result<JournalSeq> {
        Ok(*self.highest_seqno.borrow())
    }

    async fn oldest_available(&self) -> anyhow::Result<JournalSeq> {
        Ok(self.inner.lock().unwrap().offset)
    }

    async fn trim(&self, before: JournalSeq) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        while inner.offset < before && !inner.entries.is_empty() {
            inner.offset = JournalSeq(inner.offset.0 + 1);
            inner.entries.pop_front();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use core::assert_matches;
    use std::task::Poll;

    use futures::StreamExt;
    use futures::TryStreamExt;
    use obsidian_common::JournalSeq;
    use obsidian_common::TabletJournalEntry;
    use obsidian_common::Timestamp;

    use crate::mem::MemJournal;
    use crate::Journal;

    fn wal_entry(i: usize) -> TabletJournalEntry {
        TabletJournalEntry::Write(Timestamp(i as u64), vec![])
    }

    fn write_timestamp(x: (JournalSeq, TabletJournalEntry)) -> (JournalSeq, Timestamp) {
        if let TabletJournalEntry::Write(ts, _) = x.1 {
            (x.0, ts)
        } else {
            panic!();
        }
    }

    fn write_timestamps(
        iter: impl IntoIterator<Item = (JournalSeq, TabletJournalEntry)>,
    ) -> Vec<(JournalSeq, Timestamp)> {
        iter.into_iter().map(write_timestamp).collect()
    }

    #[tokio::test]
    async fn test_mem_wal() -> anyhow::Result<()> {
        let wal = MemJournal::new();

        assert_eq!(wal.append(wal_entry(1)).await?, JournalSeq(1));
        assert_eq!(wal.append(wal_entry(2)).await?, JournalSeq(2));
        assert_eq!(wal.append(wal_entry(3)).await?, JournalSeq(3));

        assert_eq!(wal.oldest_available().await?, JournalSeq(1));

        assert_eq!(
            write_timestamps(wal.read(JournalSeq(2)).try_collect::<Vec<_>>().await?),
            vec![(JournalSeq(2), Timestamp(2)), (JournalSeq(3), Timestamp(3))]
        );

        let mut tail = wal.tail(JournalSeq(2));

        assert_eq!(
            tail.try_next().await?.map(write_timestamp),
            Some((JournalSeq(2), Timestamp(2)))
        );
        assert_eq!(
            tail.try_next().await?.map(write_timestamp),
            Some((JournalSeq(3), Timestamp(3)))
        );

        assert_matches!(futures::poll!(tail.next()), Poll::Pending);

        assert_eq!(wal.append(wal_entry(4)).await?, JournalSeq(4));

        assert_eq!(
            tail.try_next().await?.map(write_timestamp),
            Some((JournalSeq(4), Timestamp(4)))
        );

        wal.trim(JournalSeq(3)).await?;

        assert_eq!(wal.oldest_available().await?, JournalSeq(3));
        assert_matches!(wal.read(JournalSeq(2)).next().await, Some(Err(_)));
        assert_eq!(
            write_timestamps(wal.read(JournalSeq(3)).try_collect::<Vec<_>>().await?),
            vec![(JournalSeq(3), Timestamp(3)), (JournalSeq(4), Timestamp(4))]
        );

        Ok(())
    }
}
