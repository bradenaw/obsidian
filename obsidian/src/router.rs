use std::collections::HashMap;

use anyhow::anyhow;
use byteorder::BigEndian;
use byteorder::ByteOrder;

use crate::obsidian::Router;
use crate::range::Bound;
use crate::range::KeyOrBound;
use crate::tablet::TabletId;
use crate::types::ColoGroupId;
use crate::types::Direction;
use crate::types::ShardId;
use crate::util::hexlify;

pub(crate) struct StaticRouter {
    map: HashMap<ColoGroupId, (Vec<Bound<Vec<u8>>>, Vec<TabletId>)>,
}

impl StaticRouter {
    pub fn new(m: HashMap<ColoGroupId, (Vec<Bound<Vec<u8>>>, Vec<TabletId>)>) -> Self {
        for (bounds, tablet_ids) in m.values() {
            assert!(bounds.is_sorted());
            assert_eq!(bounds.len() + 1, tablet_ids.len());
        }
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
            return Ok(TabletId::META);
        }
        if colo_group_id == ColoGroupId::SHARD_META {
            if key.len() < ShardId::ENCODED_LEN {
                anyhow::bail!(
                    "SHARD_META key must be {} bytes or longer to be prefixed with a shard ID: {}",
                    ShardId::ENCODED_LEN,
                    hexlify(key),
                );
            }
            return Ok(TabletId::shard_meta(ShardId(BigEndian::read_u32(
                &key[0..4],
            ))));
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

    fn tablet_id_for_bound(
        &self,
        colo_group_id: ColoGroupId,
        bound: Bound<&[u8]>,
        direction: Direction,
    ) -> anyhow::Result<TabletId> {
        if colo_group_id == ColoGroupId::META {
            return Ok(TabletId::META);
        }
        if colo_group_id == ColoGroupId::SHARD_META {
            // TODO: It'd be nice to just jump over the gap to the next shard so that something
            // like scan(Keyspace::TX_OUTCOMES, Range:all()) would work.
            let key = match bound {
                Bound::BeforeAll | Bound::AfterAll => {
                    return Err(anyhow!("{}/{:?} not routeable", colo_group_id, bound))
                }
                Bound::Before(key) => {
                    // key.len() == ShardId::ENCODED_LEN means this is a tablet boundary, so can't
                    // scan off the edge of it.
                    if direction == Direction::Desc && key.len() == ShardId::ENCODED_LEN {
                        return Err(anyhow!("{}/{:?} not routeable", colo_group_id, bound));
                    }
                    key
                }
                Bound::After(key) => key,
                Bound::AfterPrefix(key) => {
                    // key.len() == ShardId::ENCODED_LEN means this is a tablet boundary, so can't
                    // scan off the edge of it.
                    if direction == Direction::Asc && key.len() == ShardId::ENCODED_LEN {
                        return Err(anyhow!("{}/{:?} not routeable", colo_group_id, bound));
                    }
                    key
                }
            };

            if key.len() < ShardId::ENCODED_LEN {
                anyhow::bail!(
                    "SHARD_META key must be {} bytes or longer: {}",
                    ShardId::ENCODED_LEN,
                    hexlify(key),
                );
            }
            let shard_id = ShardId(BigEndian::read_u32(&key[..ShardId::ENCODED_LEN]));
            return Ok(TabletId::shard_meta(shard_id));
        }

        let (splits, tablet_ids) = self
            .map
            .get(&colo_group_id)
            .ok_or_else(|| anyhow!("{:?} has no routing", colo_group_id))?;

        let idx = match splits.binary_search(&bound.to_vec()) {
            Ok(idx) => match direction {
                Direction::Asc => idx + 1,
                Direction::Desc => idx,
            },
            Err(idx) => idx,
        };

        Ok(tablet_ids[idx])
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::StaticRouter;
    use crate::obsidian::Router;
    use crate::range::Bound;
    use crate::tablet::TabletId;
    use crate::types::ColoGroupId;
    use crate::types::Direction;
    use crate::types::ShardId;
    use crate::util::encode;

    #[test]
    fn test_tablet_id_for_bound() -> anyhow::Result<()> {
        let router = StaticRouter::new(
            vec![
                (
                    ColoGroupId(1),
                    (
                        vec![Bound::Before(vec![1])],
                        vec![TabletId(ShardId(1), 1), TabletId(ShardId(2), 2)],
                    ),
                ),
                (
                    ColoGroupId(2),
                    (
                        vec![Bound::Before(vec![1]), Bound::After(vec![5, 0])],
                        vec![
                            TabletId(ShardId(8), 1),
                            TabletId(ShardId(9), 5),
                            TabletId(ShardId(10), 3),
                        ],
                    ),
                ),
            ]
            .into_iter()
            .collect(),
        );

        assert_eq!(
            router.tablet_id_for_bound(ColoGroupId::META, Bound::Before(&[0]), Direction::Asc)?,
            TabletId::META,
        );

        assert_eq!(
            router.tablet_id_for_bound(
                ColoGroupId::SHARD_META,
                Bound::Before(&encode(&TabletId(ShardId(1), 5))),
                Direction::Asc,
            )?,
            TabletId::shard_meta(ShardId(1)),
        );

        assert_matches!(
            router.tablet_id_for_bound(
                ColoGroupId::SHARD_META,
                Bound::Before(&[0, 0, 0, 1]),
                Direction::Desc,
            ),
            Err(_)
        );

        assert_eq!(
            router.tablet_id_for_bound(
                ColoGroupId::SHARD_META,
                Bound::Before(&[0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 5, 10]),
                Direction::Desc,
            )?,
            TabletId::shard_meta(ShardId(1)),
        );

        assert_eq!(
            router.tablet_id_for_bound(
                ColoGroupId::SHARD_META,
                Bound::AfterPrefix(&[0, 0, 0, 1]),
                Direction::Desc,
            )?,
            TabletId::shard_meta(ShardId(1)),
        );

        assert_matches!(
            router.tablet_id_for_bound(
                ColoGroupId::SHARD_META,
                Bound::AfterPrefix(&[0, 0, 0, 1]),
                Direction::Asc,
            ),
            Err(_)
        );

        assert_eq!(
            router.tablet_id_for_bound(ColoGroupId(1), Bound::Before(&[0]), Direction::Asc)?,
            TabletId(ShardId(1), 1),
        );
        assert_eq!(
            router.tablet_id_for_bound(ColoGroupId(1), Bound::Before(&[1]), Direction::Desc)?,
            TabletId(ShardId(1), 1),
        );
        assert_eq!(
            router.tablet_id_for_bound(ColoGroupId(1), Bound::Before(&[1]), Direction::Asc)?,
            TabletId(ShardId(2), 2),
        );
        assert_eq!(
            router.tablet_id_for_bound(ColoGroupId(1), Bound::Before(&[1, 1]), Direction::Desc)?,
            TabletId(ShardId(2), 2),
        );

        assert_eq!(
            router.tablet_id_for_bound(ColoGroupId(2), Bound::Before(&[0]), Direction::Asc)?,
            TabletId(ShardId(8), 1),
        );
        assert_eq!(
            router.tablet_id_for_bound(ColoGroupId(2), Bound::Before(&[1]), Direction::Desc)?,
            TabletId(ShardId(8), 1),
        );
        assert_eq!(
            router.tablet_id_for_bound(ColoGroupId(2), Bound::Before(&[1]), Direction::Asc)?,
            TabletId(ShardId(9), 5),
        );
        assert_eq!(
            router.tablet_id_for_bound(ColoGroupId(2), Bound::Before(&[1, 5]), Direction::Asc)?,
            TabletId(ShardId(9), 5),
        );
        assert_eq!(
            router.tablet_id_for_bound(ColoGroupId(2), Bound::After(&[5, 0]), Direction::Desc)?,
            TabletId(ShardId(9), 5),
        );
        assert_eq!(
            router.tablet_id_for_bound(ColoGroupId(2), Bound::After(&[5, 0]), Direction::Asc)?,
            TabletId(ShardId(10), 3),
        );
        assert_eq!(
            router.tablet_id_for_bound(
                ColoGroupId(2),
                Bound::AfterPrefix(&[5, 0]),
                Direction::Desc,
            )?,
            TabletId(ShardId(10), 3),
        );

        Ok(())
    }
}
