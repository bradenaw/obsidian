use std::collections::BTreeMap;
use std::sync::Arc;

use crate::meta::Meta;
use crate::range::Range;
use crate::types::Timestamp;
use crate::types::Value;
use crate::util::AtomicArc;
use crate::util::Background;
use crate::util::Retry;

struct MetaSynced<M: Meta> {
    bg: Background,
    inner: Arc<MetaSyncedInner<M>>,
}

struct MetaSyncedInner<M: Meta> {
    m: M,
    kv: AtomicArc<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl<M: Meta + Sync + Send + 'static> MetaSynced<M> {
    fn new(m: M) -> Self {
        let bg = Background::new();

        let inner = Arc::new(MetaSyncedInner {
            m,
            kv: AtomicArc::new(Arc::new(BTreeMap::new())),
        });

        bg.spawn({
            let inner = inner.clone();
            async move { inner.sync().await }
        });

        Self { bg, inner }
    }
}

impl<M: Meta> MetaSyncedInner<M> {
    async fn sync(&self) {
        let mut ts = Retry::new()
            .indefinitely(|| async move {
                let ts = self.m.latest_snapshot().await?;
                Ok::<_, anyhow::Error>(ts)
            })
            .await;

        let mut kv = BTreeMap::new();
        let mut maybe_cursor = Some(Range::all());
        while let Some(cursor) = maybe_cursor {
            let (page, continue_cursor) = Retry::new()
                .indefinitely(|| {
                    let cursor = cursor.clone();
                    async move {
                        let out = self.m.scan_page(ts, cursor.clone()).await?;
                        Ok::<_, anyhow::Error>(out)
                    }
                })
                .await;

            for (key, _, value) in page {
                kv.insert(key, value);
            }

            maybe_cursor = continue_cursor;
        }

        self.kv.store(Arc::new(kv.clone()));

        loop {
            // TODO: need to make sync a long-poll
            let (records, new_ts) = Retry::new()
                .indefinitely(|| async move {
                    let (records, new_ts) = self.m.sync(ts).await?;
                    Ok::<_, anyhow::Error>((records, new_ts))
                })
                .await;

            for record in records {
                match record.value {
                    Value::Regular(v) => {
                        kv.insert(record.key, v);
                    }
                    Value::Tombstone => {
                        kv.remove(&record.key);
                    }
                }
            }

            self.kv.store(Arc::new(kv.clone()));
            ts = new_ts;
        }
    }
}
