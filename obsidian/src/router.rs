use std::collections::HashMap;

use anyhow::anyhow;
use byteorder::BigEndian;
use byteorder::ByteOrder;

use crate::obsidian::Router;
use crate::obsidian::TabletId;
use crate::range::Bound;
use crate::range::KeyOrBound;
use crate::types::ColoGroupId;
use crate::types::ShardId;
use crate::util::hexlify;

pub(crate) struct StaticRouter {
    map: HashMap<ColoGroupId, (Vec<Bound<Vec<u8>>>, Vec<TabletId>)>,
}

impl StaticRouter {
    pub fn new(m: HashMap<ColoGroupId, (Vec<Bound<Vec<u8>>>, Vec<TabletId>)>) -> Self {
        Self { map: m }
    }
}

impl Router for StaticRouter {
    fn tablet_id_for_key(
        &self,
        colo_group_id: ColoGroupId,
        key: &[u8],
    ) -> anyhow::Result<TabletId> {
        if colo_group_id == ColoGroupId::META {
            return Ok(TabletId(ShardId(1), 1));
        }
        if colo_group_id == ColoGroupId::TABLET_META {
            if key.len() < 12 {
                anyhow::bail!(
                    "TABLET_META key must be 12 bytes or longer: {}",
                    hexlify(key)
                );
            }
            return Ok(TabletId(
                ShardId(BigEndian::read_u32(&key[0..4])),
                BigEndian::read_u64(&key[4..12]),
            ));
        }

        let (splits, tablet_ids) = self
            .map
            .get(&colo_group_id)
            .ok_or_else(|| anyhow!("{:?} has no routing", colo_group_id))?;

        let idx = splits
            .binary_search_by_key(&KeyOrBound::Key(key.to_vec()), |bound| {
                KeyOrBound::Bound(bound.clone())
            })
            .unwrap_or_else(core::convert::identity);

        Ok(tablet_ids[idx])
    }
}
