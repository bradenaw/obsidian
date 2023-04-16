use std::collections::BTreeMap;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::anyhow;
use prost::Message;

use crate::meta::Meta;
use crate::obsidian::Router;
use crate::obsidian::TabletId;
use crate::pb;
use crate::range::Bound;
use crate::range::Range;
use crate::meta::PFX_TABLETS;
use crate::router::StaticRouter;
use crate::tuple_encoding::tuple_decode;
use crate::tuple_encoding::tuple_encode;
use crate::types::ColoGroupId;
use crate::types::Direction;
use crate::types::ShardId;
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
    router: AtomicArc<Option<StaticRouter>>,
}

impl<M: Meta + Sync + Send + 'static> MetaSynced<M> {
    fn new(m: M) -> Self {
        let bg = Background::new();

        let inner = Arc::new(MetaSyncedInner {
            m,
            kv: AtomicArc::new(Arc::new(BTreeMap::new())),
            router: AtomicArc::new(Arc::new(None)),
        });

        bg.spawn({
            let inner = inner.clone();
            async move { inner.sync().await }
        });

        Self { bg, inner }
    }
}

impl<M: Meta> Router for MetaSynced<M> {
    fn tablet_id_for_key(
        &self,
        colo_group_id: ColoGroupId,
        key: &[u8],
    ) -> anyhow::Result<TabletId> {
        let maybe_router = self.inner.router.load();
        if let Some(router) = maybe_router.deref() {
            return router.tablet_id_for_key(colo_group_id, key);
        }
        Err(anyhow!("routing table not synced yet"))
    }

    fn tablet_id_for_bound(
        &self,
        colo_group_id: ColoGroupId,
        bound: Bound<&[u8]>,
        direction: Direction,
    ) -> anyhow::Result<TabletId> {
        let maybe_router = self.inner.router.load();
        if let Some(router) = maybe_router.deref() {
            return router.tablet_id_for_bound(colo_group_id, bound, direction);
        }
        Err(anyhow!("routing table not synced yet"))
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
        _ = self.regen_router();

        loop {
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
            _ = self.regen_router();
            if new_ts == ts {
                Retry::new()
                    .indefinitely(|| async move {
                        self.m.wait_for_newer(ts).await?;
                        Ok::<_, anyhow::Error>(())
                    })
                    .await;
            }
            ts = new_ts;
        }
    }

    fn regen_router(&self) -> anyhow::Result<()> {
        let kv = self.kv.load();
        let mut ranges_by_colo_group = HashMap::new();
        let r = kv.range(tuple_encode(&(PFX_TABLETS,))..tuple_encode(&(PFX_TABLETS, u64::MAX)));
        for (k, v) in r {
            let (_, shard_id_raw, tablet_id_id_raw): (u64, u64, u64) = tuple_decode(&k[..])?;
            let tablet_id = TabletId(ShardId(shard_id_raw as u32), tablet_id_id_raw);
            let tablet_metadata = pb::MetaTablet::decode(&v[..])?;

            if tablet_metadata.colo_group_ids.len() != tablet_metadata.ranges.len() {
                // TODO: log
                return Err(anyhow!("corrupted MetaTablet"));
            }

            for i in 0..tablet_metadata.ranges.len() {
                let colo_group_id_pb = &tablet_metadata.colo_group_ids[i];
                let colo_group_id = ColoGroupId(*colo_group_id_pb);
                let range_pb = &tablet_metadata.ranges[i];

                let range = Range::try_from(range_pb.clone())?;

                ranges_by_colo_group.entry(colo_group_id).or_insert_with(Vec::new).push((range, tablet_id));
            }
        }

        let mut routing_map = HashMap::new();
        for (colo_group_id, ranges) in ranges_by_colo_group.iter_mut() {
            ranges.sort_unstable_by_key(|(range, _)| range.lower.clone());

            let mut tablet_ids = vec![];
            let mut bounds = vec![];
            tablet_ids.push(ranges[0].1);
            for (range, tablet_id) in &ranges[1..] {
                bounds.push(range.lower.clone());
                tablet_ids.push(*tablet_id);
            }
            routing_map.insert(*colo_group_id, (bounds, tablet_ids));
        }
        self.router.store(Arc::new(Some(StaticRouter::new(routing_map))));
        Ok(())
    }
}
