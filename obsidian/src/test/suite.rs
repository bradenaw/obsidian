use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;

use crate::Bound;
use crate::ColoGroupId;
use crate::Direction;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Obsidian;
use crate::Range;
use crate::Record;
use crate::Timestamp;

/// Provides a test suite for the Obsidian trait.
///
/// The argument $make is expected to be AsyncFn() -> anyhow::Result<Arc<dyn Obsidian>>.
macro_rules! obsidian_test_suite {
    ($make:expr) => {
        mod obsidian_test_suite {
            use crate::test::suite;

            #[tokio::test]
            async fn test_2pc() -> anyhow::Result<()> {
                suite::test_2pc($make).await
            }

            #[tokio::test]
            async fn test_scan_page() -> anyhow::Result<()> {
                suite::test_scan_page($make).await
            }
        }
    };
}

pub(crate) use obsidian_test_suite;

/// Intended to be used via the obsidian_test_suite macro.
pub(crate) async fn test_2pc<F>(make: F) -> anyhow::Result<()>
where
    F: AsyncFn() -> anyhow::Result<Arc<dyn Obsidian>>,
{
    let _ = pretty_env_logger::try_init();

    let colo_group_id = ColoGroupId(1);
    let keyspace_id = KeyspaceId(colo_group_id, 1);

    let obs = make().await?;
    obs.create_colo_group(colo_group_id, vec![Bound::Before(vec![2])])
        .await?;
    obs.create_keyspace(keyspace_id).await?;

    let key1 = vec![1];
    let key2 = vec![2];

    let write_ts = obs
        .write(
            vec![],
            BTreeMap::from([
                ((keyspace_id, key1.clone()), Mutation::Put(vec![1, 2, 3])),
                ((keyspace_id, key2.clone()), Mutation::Put(vec![4, 5, 6])),
            ]),
        )
        .await?;

    assert_eq!(
        obs.get(write_ts, &(keyspace_id, key1))
            .await?
            .map(|record| record.value),
        Some(vec![1, 2, 3])
    );
    assert_eq!(
        obs.get(write_ts, &(keyspace_id, key2))
            .await?
            .map(|record| record.value),
        Some(vec![4, 5, 6])
    );

    Ok(())
}

/// Intended to be used via the obsidian_test_suite macro.
pub(crate) async fn test_scan_page<F>(make: F) -> anyhow::Result<()>
where
    F: AsyncFn() -> anyhow::Result<Arc<dyn Obsidian>>,
{
    let _ = pretty_env_logger::try_init();

    let colo_group_id = ColoGroupId(1);
    let keyspace_id = KeyspaceId(colo_group_id, 1);

    let obs = make().await?;
    obs.create_colo_group(
        colo_group_id,
        vec![Bound::Before(vec![2]), Bound::Before(vec![3])],
    )
    .await?;
    obs.create_keyspace(keyspace_id).await?;

    let writes: [(Vec<u8>, _); 12] = [
        //          ts=0123456789
        (vec![1, 0], b" o  o    o"),
        (vec![1, 1], b"   o     o"),
        (vec![1, 2], b"   o x    "),
        (vec![1, 3], b"   oxo    "),
        (vec![2, 0], b"    o   o "),
        (vec![2, 1], b"     o  o "),
        (vec![2, 2], b" o x  o  o"),
        (vec![3, 0], b"  o oxo  o"),
        (vec![3, 1], b"  o  oo o "),
        (vec![3, 2], b" xoxoxoxox"),
        (vec![3, 3], b"        o "),
        (vec![3, 4], b" ooooooooo"),
    ];

    let mut timestamps = vec![Timestamp(0)];
    for ts_idx in 1..writes[0].1.len() {
        let mut mutations = BTreeMap::new();
        for (key, versions) in &writes {
            let mutation = match versions[ts_idx] {
                b'o' => Mutation::Put(format!("{:?} {}", key, ts_idx).into()),
                b'x' => Mutation::Delete,
                _ => continue,
            };

            mutations.insert((keyspace_id, key.clone()), mutation);
        }

        if mutations.is_empty() {
            timestamps.push(timestamps.last().cloned().unwrap_or(Timestamp(0)));
            continue;
        }

        let ts = obs.write(vec![], mutations).await?;
        timestamps.push(ts);
    }

    async fn check(
        obs: &dyn Obsidian,
        timestamps: &[Timestamp],
        ts_idx: usize,
        range: Range<&[u8]>,
        expected: Vec<(Vec<u8>, usize)>,
    ) -> anyhow::Result<()> {
        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
        for direction in [Direction::Asc, Direction::Desc] {
            for page_size in 1..=expected.len() {
                let mut maybe_cursor = Some(range.to_vec());
                let mut results = vec![];
                while let Some(cursor) = maybe_cursor {
                    let (page, continue_cursor) = obs
                        .scan_page(
                            timestamps[ts_idx],
                            keyspace_id,
                            cursor.borrow(),
                            direction,
                            page_size,
                        )
                        .await?;

                    assert!(page.len() <= page_size);
                    results.extend(page);
                    assert_ne!(continue_cursor, Some(cursor));
                    maybe_cursor = continue_cursor;
                }

                if direction == Direction::Desc {
                    results.reverse();
                }

                assert_eq!(
                    results,
                    expected
                        .clone()
                        .into_iter()
                        .map(|(key, ts_idx)| Record {
                            key: (keyspace_id, key.clone()),
                            ts: timestamps[ts_idx],
                            value: format!("{:?} {}", key, ts_idx).into(),
                        })
                        .collect::<Vec<_>>(),
                    "scan_page(ts={:?}, /*keyspace_id*/, /*cursor*/, direction={:?}, page_size={})",
                    timestamps[ts_idx],
                    direction,
                    page_size,
                );
            }
        }

        Ok(())
    }

    check(
        obs.deref(),
        &timestamps,
        5,
        Range {
            lower: Bound::Before(&[1, 1]),
            upper: Bound::After(&[2, 0]),
        },
        vec![(vec![1, 1], 3), (vec![1, 3], 5), (vec![2, 0], 4)],
    )
    .await?;

    check(
        obs.deref(),
        &timestamps,
        4,
        Range::all(),
        vec![
            (vec![1, 0], 4),
            (vec![1, 1], 3),
            (vec![1, 2], 3),
            // [1,3] got deleted at 4
            (vec![2, 0], 4),
            // [2,1] doesn't exist yet
            // [2,2] got deleted at 3
            (vec![3, 0], 4),
            (vec![3, 1], 2),
            (vec![3, 2], 4),
            // [3,3] doesn't exist yet
            (vec![3, 4], 4),
        ],
    )
    .await?;

    Ok(())
}
