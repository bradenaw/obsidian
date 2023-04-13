use std::collections::BTreeMap;
use std::sync::Arc;

use crate::meta::Meta;
use crate::util::Background;

struct MetaSynced<M: Meta> {
    bg: Background,
    inner: Arc<MetaSyncedInner<M>>,
}

struct MetaSyncedInner<M: Meta> {
    m: M,
    kv: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl<M: Meta + Sync + Send> MetaSynced<M> {
    fn new(m: M) -> Self {
        let bg = Background::new();

        let inner = Arc::new(MetaSyncedInner {
            m,
            kv: BTreeMap::new(),
        });

        bg.spawn({
            let inner = inner.clone();
            async move { inner.sync().await }
        });

        Self { bg, inner }
    }
}

impl<M: Meta> MetaSyncedInner<M> {
    async fn sync(&self) {}
}
