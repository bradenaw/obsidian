use std::collections::VecDeque;
use std::sync::Mutex;

use async_stream::try_stream;
use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;
use tokio::sync::watch;

use crate::runtime::WalSeq;
use crate::runtime::Wal;

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
            inner: Mutex::new(MemWalInner{
                entries: VecDeque::new(),
                offset: WalSeq(0),
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
        inner.entries.push_back(e);
        let seqno = WalSeq(inner.offset.0 + (inner.entries.len() as u64));
        let _ = self.highest_seqno_send.send(seqno);
        Ok(seqno)
    }

    fn read(&self, first: WalSeq) -> impl Stream<Item = anyhow::Result<(WalSeq, E)>> + Send + '_ {
        try_stream! {
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
        }
    }

    fn tail(&self, first: WalSeq) -> impl Stream<Item = anyhow::Result<(WalSeq, E)>> + Send + '_ {
        try_stream! {
            let mut i = first;
            loop {
                let mut highest_seqno = self.highest_seqno.clone();
                let mut r = Box::pin(self.read(i));
                while let Some((seqno, entry)) = r.next().await.transpose()? {
                    yield (seqno, entry);
                    i = seqno;
                }
                let _ = highest_seqno
                    .changed()
                    .await;
            }
        }
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
