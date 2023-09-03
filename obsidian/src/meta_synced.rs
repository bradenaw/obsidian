use std::collections::BTreeMap;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::anyhow;
use prost::Message;

use crate::meta::Meta;
use crate::meta::PFX_TABLETS;
use crate::obsidian::Router;
use crate::obsidian::TabletId;
use crate::pb;
use crate::range::Bound;
use crate::range::Range;
use crate::range::RangeSet;
use crate::router::StaticRouter;
use crate::tuple_encoding::tuple_decode;
use crate::tuple_encoding::tuple_encode;
use crate::types::ColoGroupId;
use crate::types::Direction;
use crate::types::ShardId;
use crate::types::Timestamp;
use crate::types::Value;
use crate::util::Background;
use crate::util::Retry;
use crate::util::WaitableTimestamp;

pub(crate) struct MetaSynced {
    bg: Background,
    inner: Arc<RwLock<MetaSyncedInner>>,
}

struct MetaSyncedInner {
    synced_ts: Arc<WaitableTimestamp>,
    kv: BTreeMap<Vec<u8>, Vec<u8>>,
    router: StaticRouter,
    owned_ranges: HashMap<TabletId, HashMap<ColoGroupId, RangeSet<Vec<u8>>>>,
}

impl MetaSynced {
    pub(crate) fn new<M: Meta + Sync + Send + 'static>(m: M) -> Self {
        let bg = Background::new();

        let inner = Arc::new(RwLock::new(MetaSyncedInner {
            synced_ts: Arc::new(WaitableTimestamp::new()),
            kv: BTreeMap::new(),
            router: StaticRouter::new(HashMap::new()),
            owned_ranges: HashMap::new(),
        }));

        bg.spawn({
            let inner = inner.clone();
            async move { MetaSyncedInner::sync(inner, m).await }
        });

        Self { bg, inner }
    }

    pub(crate) fn ranges_for_tablet(
        &self,
        tablet_id: TabletId,
        colo_group_id: ColoGroupId,
    ) -> RangeSet<Vec<u8>> {
        if colo_group_id == ColoGroupId::META && tablet_id == TabletId::META {
            return RangeSet::from(Range::all());
        }
        if colo_group_id == ColoGroupId::TABLET_META {
            return RangeSet::from(Range::prefix(tablet_id.encode_fixed().to_vec()));
        }
        let inner = self.inner.read().unwrap();
        if let Some(range_set_by_colo_group_id) = inner.owned_ranges.get(&tablet_id) {
            if let Some(range_set) = range_set_by_colo_group_id.get(&colo_group_id) {
                return range_set.clone();
            }
        }
        RangeSet::new()
    }

    pub(crate) async fn wait(&self, ts: Timestamp) -> anyhow::Result<()> {
        let synced_ts = {
            let inner = self.inner.read().unwrap();
            inner.synced_ts.clone()
        };
        synced_ts.wait(ts).await?;
        Ok(())
    }
}

impl Router for MetaSynced {
    fn tablet_id_for_key(
        &self,
        colo_group_id: ColoGroupId,
        key: &[u8],
    ) -> anyhow::Result<TabletId> {
        let inner = self.inner.read().unwrap();
        return inner.router.tablet_id_for_key(colo_group_id, key);
    }

    fn tablet_id_for_bound(
        &self,
        colo_group_id: ColoGroupId,
        bound: Bound<&[u8]>,
        direction: Direction,
    ) -> anyhow::Result<TabletId> {
        let inner = self.inner.read().unwrap();
        return inner
            .router
            .tablet_id_for_bound(colo_group_id, bound, direction);
    }
}

impl MetaSyncedInner {
    async fn sync<M: Meta>(inner_lock: Arc<RwLock<Self>>, meta: M) {
        let mut ts = Retry::new()
            .indefinitely(|| async {
                let ts = meta.latest_snapshot().await?;
                Ok::<_, anyhow::Error>(ts)
            })
            .await;

        let mut kv = BTreeMap::new();
        let mut maybe_cursor = Some(Range::all());
        while let Some(cursor) = maybe_cursor {
            let (page, continue_cursor) = Retry::new()
                .indefinitely(|| {
                    let cursor = cursor.clone();
                    async {
                        let out = meta.scan_page(ts, cursor).await?;
                        Ok::<_, anyhow::Error>(out)
                    }
                })
                .await;

            for (key, _, value) in page {
                kv.insert(key, value);
            }

            maybe_cursor = continue_cursor;
        }

        {
            let mut inner = inner_lock.write().unwrap();
            _ = inner.regen_router(&kv);
            inner.synced_ts.set(ts);
        }

        loop {
            let (records, new_ts) = Retry::new()
                .indefinitely(|| async {
                    let (records, new_ts) = meta.sync(ts).await?;
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

            {
                let mut inner = inner_lock.write().unwrap();
                _ = inner.regen_router(&kv);
                inner.synced_ts.set(new_ts);
            }
            if new_ts == ts {
                Retry::new()
                    .indefinitely(|| async {
                        meta.wait_for_newer(ts).await?;
                        Ok::<_, anyhow::Error>(())
                    })
                    .await;
            }
            ts = new_ts;
        }
    }

    fn regen_router(&mut self, kv: &BTreeMap<Vec<u8>, Vec<u8>>) -> anyhow::Result<()> {
        let mut ranges_by_colo_group = HashMap::new();
        let mut tablet_map = HashMap::new();

        let r = kv.range(tuple_encode(&(PFX_TABLETS,))..tuple_encode(&(PFX_TABLETS, u64::MAX)));
        for (k, v) in r {
            let (_, shard_id_raw, tablet_id_id_raw): (u64, u64, u64) = tuple_decode(&k[..])?;
            let tablet_id = TabletId(ShardId(shard_id_raw as u32), tablet_id_id_raw);
            let tablet_metadata = pb::internal::MetaTablet::decode(&v[..])?;

            if tablet_metadata.colo_group_ids.len() != tablet_metadata.ranges.len() {
                // TODO: log
                return Err(anyhow!("corrupted MetaTablet"));
            }

            for i in 0..tablet_metadata.ranges.len() {
                let colo_group_id_pb = &tablet_metadata.colo_group_ids[i];
                let colo_group_id = ColoGroupId(*colo_group_id_pb);
                let range_pb = &tablet_metadata.ranges[i];

                let range = Range::try_from(range_pb.clone())?;

                ranges_by_colo_group
                    .entry(colo_group_id)
                    .or_insert_with(Vec::new)
                    .push((range.clone(), tablet_id));
                tablet_map
                    .entry(tablet_id)
                    .or_insert_with(HashMap::new)
                    .entry(colo_group_id)
                    .or_insert_with(RangeSet::new)
                    .add_range(range);
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

        self.router = StaticRouter::new(routing_map);
        self.owned_ranges = tablet_map;

        Ok(())
    }
}
