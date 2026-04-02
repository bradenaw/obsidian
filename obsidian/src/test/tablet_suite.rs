use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::ops::Deref;
use std::sync::Arc;

use crate::runtime::Tablet;
use crate::Bound;
use crate::ColoGroupId;
use crate::Direction;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Range;
use crate::Record;
use crate::Timestamp;

/// Provides a test suite for the Tablet trait.
///
/// The argument $make is expected to be
///   AsyncFn() -> anyhow::Result<Deref<Target = Arc<dyn Tablet>>>.
///
/// The tablet must have ksp:1/1 available and own Range::prefix(vec![0x00]).
macro_rules! tablet_test_suite {
    ($make:expr) => {
        mod tablet_test_suite {
            use crate::test::tablet_suite;

            #[tokio::test]
            async fn test_write() -> anyhow::Result<()> {
                let _ = pretty_env_logger::try_init();
                let tablet = $make().await?;
                tablet_suite::test_write(&tablet).await
            }

            #[tokio::test]
            async fn test_scan_page() -> anyhow::Result<()> {
                let _ = pretty_env_logger::try_init();
                let tablet = $make().await?;
                tablet_suite::test_scan_page(&tablet).await
            }
        }
    };
}

pub(crate) use tablet_test_suite;

/// Intended to be used via the tablet_test_suite macro.
pub(crate) async fn test_write(tablet: &Arc<dyn Tablet>) -> anyhow::Result<()> {
    let colo_group_id = ColoGroupId(1);
    let keyspace_id = KeyspaceId(colo_group_id, 1);

    let key1 = (keyspace_id, vec![0, 1]);
    let key2 = (keyspace_id, vec![0, 2]);

    let write_ts = tablet
        .write(
            vec![],
            BTreeMap::from([
                (key1.clone(), Mutation::Put(vec![1, 2, 3])),
                (key2.clone(), Mutation::Put(vec![4, 5, 6])),
            ]),
        )
        .await?;

    assert_eq!(
        tablet
            .get(write_ts, &key1)
            .await?
            .map(|record| record.value),
        Some(vec![1, 2, 3])
    );
    assert_eq!(
        tablet
            .get(write_ts, &key2)
            .await?
            .map(|record| record.value),
        Some(vec![4, 5, 6])
    );

    assert_eq!(
        tablet
            .get_multi(write_ts, BTreeSet::from([key1.clone(), key2.clone()]))
            .await?,
        BTreeMap::from([
            (
                key1.clone(),
                Record {
                    key: key1.clone(),
                    ts: write_ts,
                    value: vec![1, 2, 3]
                }
            ),
            (
                key2.clone(),
                Record {
                    key: key2.clone(),
                    ts: write_ts,
                    value: vec![4, 5, 6]
                }
            )
        ]),
    );

    Ok(())
}

/// Intended to be used via the tablet_test_suite macro.
pub(crate) async fn test_scan_page(tablet: &Arc<dyn Tablet>) -> anyhow::Result<()> {
    let keyspace_id = KeyspaceId(ColoGroupId(1), 1);

    let writes: [(Vec<u8>, _); 12] = [
        //          ts=0123456789
        (vec![0, 1, 0], b" o  o    o"),
        (vec![0, 1, 1], b"   o     o"),
        (vec![0, 1, 2], b"   o x    "),
        (vec![0, 1, 3], b"   oxo    "),
        (vec![0, 2, 0], b"    o   o "),
        (vec![0, 2, 1], b"     o  o "),
        (vec![0, 2, 2], b" o x  o  o"),
        (vec![0, 3, 0], b"  o oxo  o"),
        (vec![0, 3, 1], b"  o  oo o "),
        (vec![0, 3, 2], b" xoxoxoxox"),
        (vec![0, 3, 3], b"        o "),
        (vec![0, 3, 4], b" ooooooooo"),
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

        let ts = tablet.write(vec![], mutations).await?;
        timestamps.push(ts);
    }

    async fn check(
        tablet: &dyn Tablet,
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
                    let (page, continue_cursor) = tablet
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
        tablet.deref(),
        &timestamps,
        5,
        Range {
            lower: Bound::Before(&[0, 1, 1]),
            upper: Bound::After(&[0, 2, 0]),
        },
        vec![(vec![0, 1, 1], 3), (vec![0, 1, 3], 5), (vec![0, 2, 0], 4)],
    )
    .await?;

    check(
        tablet.deref(),
        &timestamps,
        4,
        Range::prefix(vec![0]).borrow(),
        vec![
            (vec![0, 1, 0], 4),
            (vec![0, 1, 1], 3),
            (vec![0, 1, 2], 3),
            // [0,1,3] got deleted at 4
            (vec![0, 2, 0], 4),
            // [0,2,1] doesn't exist yet
            // [0,2,2] got deleted at 3
            (vec![0, 3, 0], 4),
            (vec![0, 3, 1], 2),
            (vec![0, 3, 2], 4),
            // [0,3,3] doesn't exist yet
            (vec![0, 3, 4], 4),
        ],
    )
    .await?;

    Ok(())
}
