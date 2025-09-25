use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Mutex;

use async_stream::try_stream;
use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;
use tokio::sync::watch;

use crate::runtime::Wal;
use crate::runtime::WalSeq;

pub(crate) struct MemWal<E> {
    inner: Mutex<MemWalInner<E>>,
    highest_seqno_send: watch::Sender<WalSeq>,
    highest_seqno: watch::Receiver<WalSeq>,
}

struct MemWalInner<E> {
    entries: VecDeque<E>,
    offset: WalSeq,
}

impl<E> MemWal<E> {
    pub fn new() -> Self {
        let (highest_seqno_send, highest_seqno) = watch::channel(WalSeq(0));
        Self {
            inner: Mutex::new(MemWalInner {
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
impl<E> Wal<E> for MemWal<E>
where
    // TODO: TryFrom<bytes> + Into<bytes>
    E: Clone + Send + Sync + 'static,
{
    async fn append(&self, e: E) -> anyhow::Result<WalSeq> {
        let mut inner = self.inner.lock().unwrap();
        let seqno = WalSeq(inner.offset.0 + (inner.entries.len() as u64));
        inner.entries.push_back(e);
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

    use crate::runtime::Wal;
    use crate::runtime::WalSeq;
    use crate::test::MemWal;

    #[tokio::test]
    async fn test_mem_wal() -> anyhow::Result<()> {
        let wal = MemWal::new();

        assert_eq!(wal.append(1).await?, WalSeq(1));
        assert_eq!(wal.append(2).await?, WalSeq(2));
        assert_eq!(wal.append(3).await?, WalSeq(3));

        assert_eq!(wal.oldest_available().await?, WalSeq(1));

        assert_eq!(
            wal.read(WalSeq(2)).try_collect::<Vec<_>>().await?,
            vec![(WalSeq(2), 2), (WalSeq(3), 3)]
        );

        let mut tail = wal.tail(WalSeq(2));

        assert_eq!(tail.try_next().await?, Some((WalSeq(2), 2)));
        assert_eq!(tail.try_next().await?, Some((WalSeq(3), 3)));

        assert_matches!(futures::poll!(tail.next()), Poll::Pending);

        assert_eq!(wal.append(4).await?, WalSeq(4));

        assert_eq!(tail.try_next().await?, Some((WalSeq(4), 4)));

        wal.trim(WalSeq(3)).await?;

        assert_eq!(wal.oldest_available().await?, WalSeq(3));
        assert_matches!(wal.read(WalSeq(2)).next().await, Some(Err(_)));
        assert_eq!(
            wal.read(WalSeq(3)).try_collect::<Vec<_>>().await?,
            vec![(WalSeq(3), 3), (WalSeq(4), 4)]
        );

        Ok(())
    }
}
