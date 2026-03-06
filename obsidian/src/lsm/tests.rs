use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;

use byteorder::BigEndian;
use byteorder::ByteOrder;
use futures::TryStreamExt;
use proptest::prelude::*;

use crate::lsm::index::Keyspace;
use crate::lsm::index::Level;
use crate::lsm::memtable::Memtable;
use crate::lsm::run::dump_run;
use crate::lsm::run::Run;
use crate::lsm::run::RunBuilder;
use crate::lsm::KeyspaceReader;
use crate::lsm::Lsm;
use crate::lsm::LsmOptions;
use crate::lsm::LsmRevision;
use crate::lsm::RunId;
use crate::test::MemFileReader;
use crate::test::MemStorage;
use crate::util::binary_search_by_idx;
use crate::Bound;
use crate::ColoGroupId;
use crate::Direction;
use crate::HistoryRange;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Range;
use crate::Revision;
use crate::RevisionValue;
use crate::Timestamp;

#[tokio::test]
async fn test_put_get() -> anyhow::Result<()> {
    let lsm = Lsm::empty(LsmOptions::default(), Arc::new(MemStorage::new())).await?;
    let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
    let k = b"abc";
    let not_k = b"def";
    let v = b"foo";

    lsm.create_keyspace_with_depth(keyspace_id, 2 /*depth*/)?;
    lsm.write(
        Timestamp(5),
        (keyspace_id, k.to_vec()),
        Mutation::Put(v.to_vec()),
    )?;
    assert_eq!(lsm.get(Timestamp(4), keyspace_id, k).await?, None);
    assert_eq!(
        lsm.get(Timestamp(5), keyspace_id, k).await?,
        Some((Timestamp(5), RevisionValue::Regular(v.to_vec())))
    );
    assert_eq!(
        lsm.get(Timestamp(6), keyspace_id, k).await?,
        Some((Timestamp(5), RevisionValue::Regular(v.to_vec())))
    );
    assert_eq!(lsm.get(Timestamp(4), keyspace_id, not_k).await?, None);
    assert_eq!(lsm.get(Timestamp(5), keyspace_id, not_k).await?, None);
    assert_eq!(lsm.get(Timestamp(6), keyspace_id, not_k).await?, None);

    Ok(())
}

#[tokio::test]
async fn test_compact_l0() -> anyhow::Result<()> {
    _ = pretty_env_logger::try_init();
    let lsm = Lsm::empty(
        LsmOptions {
            l0_max_size: 128,
            block_size_target: 128,
            run_size_target: 512,
        },
        Arc::new(MemStorage::new()),
    )
    .await?;
    let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
    lsm.create_keyspace_with_depth(keyspace_id, 2 /*depth*/)?;
    let mut map = BTreeMap::new();
    let mut last_ts = Timestamp::ZERO;
    for _ in 0..10 {
        let compacted = lsm.pending_compactions();
        // We consider these writes to be 10 bytes (1 key + 8 ts + 1 value), so this is
        // enough to overfill a memtable.
        for i in 0..24 {
            let v = (i % 179) as u8;
            last_ts = Timestamp(last_ts.0 + 1);
            lsm.write(
                last_ts,
                (keyspace_id, vec![i as u8]),
                Mutation::Put(vec![v]),
            )?;
            map.insert(i as u8, v);
        }
        compacted.await;

        for (k, v) in &map {
            assert_eq!(
                lsm.get(last_ts, keyspace_id, &[*k]).await?.map(|(_, b)| b),
                Some(RevisionValue::Regular(vec![*v])),
            );
        }
    }

    // Make sure we actually did ever do a compaction.
    assert!(
        lsm.index_snapshot()
            .keyspaces
            .get(&keyspace_id)
            .unwrap()
            .levels[1]
            .runs
            .len()
            >= 1
    );

    Ok(())
}

#[tokio::test]
async fn test_compact_l1() -> anyhow::Result<()> {
    _ = pretty_env_logger::try_init();

    let lsm = Lsm::empty(
        LsmOptions {
            l0_max_size: 128,
            block_size_target: 128,
            run_size_target: 512,
        },
        Arc::new(MemStorage::new()),
    )
    .await?;
    let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
    lsm.create_keyspace_with_depth(keyspace_id, 3 /*depth*/)?;
    let mut map = BTreeMap::new();
    let mut last_ts = Timestamp::ZERO;
    let mut ctr = 1u32;
    for j in 0..10 {
        loop {
            // We consider these writes to be 10 bytes (1 key + 8 ts + 1 value), so this is
            // enough to overfill a memtable.
            for i in 0..24 {
                let k = (j * 5 + i) as u8;
                let mut v = [0u8; 4];
                BigEndian::write_u32(&mut v, ctr);
                ctr += 1;
                lsm.write(
                    Timestamp(ctr as u64),
                    (keyspace_id, vec![k]),
                    Mutation::Put(v.to_vec()),
                )?;
                last_ts = Timestamp(ctr as u64);
                map.insert(k, v.to_vec());
            }

            lsm.pending_compactions().await;
            if lsm
                .index_snapshot()
                .keyspaces
                .get(&keyspace_id)
                .unwrap()
                .levels[2]
                .runs
                .len()
                >= (j + 1) as usize
            {
                break;
            }
        }

        dump_lsm(&lsm).await?;

        for (k, v) in &map {
            let actual = lsm.get(last_ts, keyspace_id, &[*k]).await?.map(|(_, b)| b);
            assert_eq!(actual, Some(RevisionValue::Regular(v.clone())));
        }
    }

    Ok(())
}

#[test]
fn test_binary_search_by_key() {
    for n in 1..32 {
        for i in 0..n {
            assert_eq!(binary_search_by_idx(n, i, |x| x), Ok(i));
        }
    }
    for n in 1..32 {
        for i in 0..=n {
            assert_eq!(binary_search_by_idx(n, 2 * i, |x| 2 * x + 1), Err(i));
        }
    }
}

#[tokio::test]
async fn test_scan_page() -> anyhow::Result<()> {
    let lsm = Lsm::empty(
        LsmOptions {
            l0_max_size: 32,
            block_size_target: 48,
            run_size_target: 96,
        },
        Arc::new(MemStorage::new()),
    )
    .await?;
    let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
    lsm.create_keyspace_with_depth(keyspace_id, 3 /*depth*/)?;

    let writes = [
        //   ts=0123456789
        ("a", b" o  o    o"),
        ("b", b"   o     o"),
        ("c", b"   o x    "),
        ("d", b"   oxo    "),
        ("e", b"    o   o "),
        ("f", b"     o  o "),
        ("g", b" o x  o  o"),
        ("h", b"  o oxo  o"),
        ("i", b"  o  oo o "),
        ("j", b" xoxoxoxox"),
        ("k", b"        o "),
        ("l", b" ooooooooo"),
    ];

    //let mut expecteds = vec![];
    for ts in 1..writes[0].1.len() {
        //let mut expected = match expecteds.last() {
        //    Some(prev) => prev.clone(),
        //    None => BTreeMap::new(),
        //};

        for (key, versions) in writes {
            let mutation = match versions[ts] {
                b'o' => Mutation::Put(format!("{} {}", key, ts).into()),
                b'x' => Mutation::Delete,
                _ => continue,
            };

            //let value = match mutation {
            //    Mutation::Put(v) => RevisionValue::Regular(v),
            //    Mutation::Delete => RevisionValue::Tombstone,
            //};
            lsm.write(Timestamp(ts as u64), (keyspace_id, key.into()), mutation)?;

            //expected.insert(key, value);
        }
        if ts < writes[0].1.len() - 2 && ts % 3 == 0 {
            lsm.pending_compactions().await;
        }
        //expecteds.push(expected);
    }

    async fn check(
        lsm: &Lsm,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<Vec<u8>>,
        expected: Vec<(&str, usize)>,
    ) -> anyhow::Result<()> {
        for direction in [Direction::Asc, Direction::Desc] {
            for page_size in 1..=expected.len() {
                println!("== check");
                let mut maybe_cursor: Option<Range<Vec<u8>>> = Some(range.clone());
                let mut results = vec![];
                while let Some(cursor) = maybe_cursor {
                    let (page, continue_cursor) = lsm
                        .scan_page(ts, keyspace_id, cursor.borrow(), direction, page_size)
                        .await?;

                    println!(
                        "scan_page(ts={}, /*keyspace_id*/, {:?}, {:?}, {}) -> ({:?}, {:?})",
                        ts, cursor, direction, page_size, continue_cursor, page,
                    );
                    assert!(page.len() <= page_size);
                    results.extend(page);
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
                        .map(|(key, ts)| Revision {
                            key: (keyspace_id, (key).into()),
                            ts: Timestamp(ts as u64),
                            value: RevisionValue::Regular(format!("{} {}", key, ts).into()),
                        })
                        .collect::<Vec<Revision>>(),
                    "scan_page(ts={:?}, /*keyspace_id*/, /*cursor*/, direction={:?}, page_size={})",
                    ts,
                    direction,
                    page_size,
                );
            }
        }

        Ok(())
    }

    dump_lsm(&lsm).await?;

    check(
        &lsm,
        Timestamp(5),
        keyspace_id,
        Range {
            lower: Bound::Before("b".into()),
            upper: Bound::After("e".into()),
        },
        vec![("b", 3), ("d", 5), ("e", 4)],
    )
    .await?;

    check(
        &lsm,
        Timestamp(4),
        keyspace_id,
        Range::all(),
        vec![
            ("a", 4),
            ("b", 3),
            ("c", 3),
            // d got deleted at 4
            ("e", 4),
            // f doesn't exist yet
            ("h", 4),
            ("i", 2),
            ("j", 4),
            // k doesn't exist yet
            ("l", 4),
        ],
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_history_page() -> anyhow::Result<()> {
    let diagram = vec![
        //                         1
        //   ts= 1 2 3 4 5 6 7 8 9 0
        ("a", b"   o  |  o|  o|o o| ".as_slice()),
        (" ", b"------+   |   | +-+ "),
        ("b", b" o   x|o  |o o|x|o  "),
        (" ", b"----+-+---+---+ +-+ "),
        ("c", b"   o|o o     o|o x| "),
        (" ", b"----+-+---+   | +-+ "),
        ("d", b"     o|o o|x o| |o  "),
    ];

    let keyspace = keyspace_from_diagram(diagram).await?;

    async fn check(
        keyspace: &Keyspace,
        key: &[u8],
        range: HistoryRange,
        expected: &[(usize, bool)],
    ) -> anyhow::Result<()> {
        for direction in [Direction::Asc, Direction::Desc] {
            for page_size in 1..=expected.len() {
                let mut maybe_cursor = Some(range.clone());
                let mut results = vec![];
                while let Some(cursor) = maybe_cursor {
                    let (page, continue_cursor) = KeyspaceReader(keyspace)
                        .history_page(key, cursor, direction, page_size)
                        .await?;

                    println!(
                            "history_page(key = {:?}, cursor = {:?}, direction={:?}, page_size={}) -> ({:?}, {:?})",
                            key,
                            cursor,
                            direction,
                            page_size,
                            page,
                            continue_cursor,
                        );

                    assert!(page.len() <= page_size);
                    results.extend(page);
                    maybe_cursor = continue_cursor;
                }

                if direction == Direction::Desc {
                    results.reverse();
                }

                assert_eq!(
                    results,
                    expected
                        .into_iter()
                        .map(|(ts, is_tombstone)| {
                            let revision = lsm_diagram_revision(key, *ts, *is_tombstone);
                            (revision.ts, revision.value)
                        })
                        .collect::<Vec<_>>(),
                    "history_page(key = {:?}, range = {:?}, direction={:?}, page_size={})",
                    key,
                    range,
                    direction,
                    page_size,
                );
            }
        }
        Ok(())
    }

    let all_b_versions = vec![
        (1, false),
        (3, true),
        (4, false),
        (6, false),
        (7, false),
        (8, true),
        (9, false),
    ];

    check(&keyspace, b"b", HistoryRange::All, &all_b_versions).await?;

    check(
        &keyspace,
        b"b",
        HistoryRange::Between(Timestamp(1), Timestamp(9)),
        &all_b_versions,
    )
    .await?;

    check(
        &keyspace,
        b"b",
        HistoryRange::Until(Timestamp(9)),
        &all_b_versions,
    )
    .await?;

    check(
        &keyspace,
        b"b",
        HistoryRange::Since(Timestamp(1)),
        &all_b_versions,
    )
    .await?;

    Ok(())
}

fn bound_strategy() -> impl Strategy<Value = Bound<Vec<u8>>> {
    prop_oneof![
        Just(Bound::BeforeAll),
        proptest::collection::vec(u8::arbitrary(), 0..16).prop_map(|v| Bound::Before(v)),
        proptest::collection::vec(u8::arbitrary(), 0..16).prop_map(|v| Bound::After(v)),
        proptest::collection::vec(u8::arbitrary(), 0..16).prop_map(|v| Bound::AfterPrefix(v)),
        Just(Bound::AfterAll),
    ]
}
fn range_strategy() -> impl Strategy<Value = Range<Vec<u8>>> {
    (bound_strategy(), bound_strategy()).prop_map(|(lower, upper)| Range { lower, upper })
}

proptest! {
    #[test]
    #[ignore]
    fn proptest_lsm_scan(
        keys in proptest::collection::btree_set(
            proptest::collection::vec(u8::arbitrary(), 0..16),
            1..100,
        ),
        write_indexes in proptest::collection::vec(any::<prop::sample::Index>(), 1..4096),
        log_indexes in proptest::collection::vec(any::<prop::sample::Index>(), 1000),
        ranges in proptest::collection::vec(range_strategy(), 1000),
        direction in proptest::sample::select(std::borrow::Cow::Owned(vec![
            Direction::Asc,
            //Direction::Desc,
        ])),
    ) {
        tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(async {
            let keys_vec: Vec<_> = keys.iter().collect();

            let mut writes = vec![];

            let lsm = Lsm::empty(
                LsmOptions {
                    l0_max_size: 128,
                    block_size_target: 128,
                    run_size_target: 512,
                },
                Arc::new(MemStorage::new()),
            )
            .await
            .unwrap();
            let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
            lsm.create_keyspace_with_depth(keyspace_id, 3 /*depth*/).unwrap();

            let mut write_ts = 5;
            for (i, index) in write_indexes.iter().enumerate() {
                let key = keys_vec[index.index(keys_vec.len())];
                let mut value = vec![0; 16];
                BigEndian::write_u64(&mut value[8..], i as u64);
                lsm
                    .write(
                        Timestamp(write_ts),
                        (keyspace_id, key.clone()),
                        Mutation::Put(value.clone()),
                    )
                    .unwrap();
                writes.push((key.clone(), Timestamp(write_ts), value.clone()));
                write_ts += 2;
            }

            for (log_index_gen, range) in std::iter::zip(log_indexes, ranges) {
                let log_idx = log_index_gen.index(writes.len());
                let ts = writes[log_idx].1;

                let mut expected = BTreeMap::new();
                for (key, ts, value) in writes[..=log_idx].iter() {
                    if !range.contains(key) {
                        continue;
                    }
                    expected.insert(key, (ts, value));
                }


                let mut maybe_cursor = Some(range.clone());
                let mut results = vec![];
                while let Some(cursor) = maybe_cursor {
                    let (mut page, continue_cursor) = lsm.scan_page(
                        ts,
                        keyspace_id,
                        cursor.borrow(),
                        direction,
                        100,
                    ).await.unwrap();
                    results.append(&mut page);
                    assert!(Some(cursor) != continue_cursor);
                    maybe_cursor = continue_cursor;
                }

                let mut expected_recs: Vec<Revision> = expected.into_iter().map(|(key, (ts, value))| {
                    Revision{key: (keyspace_id, key.clone()), ts: *ts, value: RevisionValue::Regular(value.clone())}
                }).collect();
                if direction == Direction::Desc {
                    expected_recs.reverse();
                }

                assert_eq!(results, expected_recs);
            }
        });
    }
}

async fn dump_lsm(lsm: &Lsm) -> anyhow::Result<()> {
    let index_snapshot = lsm.index_snapshot();
    for (keyspace_id, keyspace) in &index_snapshot.keyspaces {
        println!("keyspace_id {:?}", keyspace_id);
        dump_keyspace(&keyspace).await?;
    }

    Ok(())
}

async fn dump_keyspace(keyspace: &Keyspace) -> anyhow::Result<()> {
    println!("== manifest =====");
    println!("l0_active");
    {
        let memtable = &keyspace.l0_active;
        println!(
            "  {} ({} bytes) {:?}",
            memtable.id(),
            memtable.size(),
            memtable.range(),
        );
    }
    println!("l0_sealed");
    for memtable in &keyspace.l0_sealed {
        println!(
            "  {} ({} bytes) {:?}",
            memtable.id(),
            memtable.size(),
            memtable.range(),
        );
    }
    for (i, level) in keyspace.levels[1..]
        .iter()
        .enumerate()
        .map(|(i, level)| (i + 1, level))
    {
        println!("l{} ({} bytes)", i, level.size());
        for run in &level.runs {
            println!("  {} ({} bytes) {:?}", run.id(), run.size(), run.range());
        }
    }
    println!("============");

    println!("== kvs =====");
    println!("l0_active");
    {
        let memtable = &keyspace.l0_active;
        println!(
            "  {} ({} bytes) {:?}",
            memtable.id(),
            memtable.size(),
            memtable.range(),
        );
        memtable.dump();
    }
    println!("l0_sealed");
    for memtable in &keyspace.l0_sealed {
        println!(
            "  {} ({} bytes) {:?}",
            memtable.id(),
            memtable.size(),
            memtable.range(),
        );
        memtable.dump();
    }
    for (i, level) in keyspace.levels[1..]
        .iter()
        .enumerate()
        .map(|(i, level)| (i + 1, level))
    {
        println!("l{} ({} bytes)", i, level.size());
        for run in &level.runs {
            println!("  {} ({} bytes) {:?}", run.id(), run.size(), run.range());
            dump_run(&run).await?;
        }
    }
    println!("============");
    Ok(())
}

#[tokio::test]
async fn test_keyspace_from_diagram() -> anyhow::Result<()> {
    let diagram = vec![
        //                         1
        //   ts= 1 2 3 4 5 6 7 8 9 0
        ("a", b"   o  |  o|  o|o o| ".as_slice()),
        (" ", b"------+   |   | +-+ "),
        ("b", b" o   x|o  |o o|x|o  "),
        (" ", b"----+-+---+---+ +-+ "),
        ("c", b"   o|o o     o|o x| "),
        (" ", b"----+-+---+   | +-+ "),
        ("d", b"     o|o o|x o| |o  "),
    ];

    let keyspace = keyspace_from_diagram(diagram).await?;

    let a = "a";
    let b = "b";
    let c = "c";
    let d = "d";

    assert_eq!(
        keyspace.l0_active.iter().collect::<Vec<_>>(),
        vec![
            lsm_diagram_revision(b.as_bytes(), 9, false),
            lsm_diagram_revision(d.as_bytes(), 9, false),
        ],
    );
    assert_eq!(
        keyspace.l0_sealed[0].iter().collect::<Vec<_>>(),
        vec![
            lsm_diagram_revision(a.as_bytes(), 9, false),
            lsm_diagram_revision(a.as_bytes(), 8, false),
            lsm_diagram_revision(b.as_bytes(), 8, true),
            lsm_diagram_revision(c.as_bytes(), 9, true),
            lsm_diagram_revision(c.as_bytes(), 8, false),
        ],
    );

    assert_eq!(
        keyspace.levels[1..]
            .iter()
            .map(|level| {
                level
                    .runs
                    .iter()
                    .map(|run| futures::executor::block_on(run.stream().try_collect::<Vec<_>>()))
                    .collect::<anyhow::Result<Vec<_>>>()
            })
            .collect::<anyhow::Result<Vec<_>>>()?,
        vec![
            vec![
                vec![(a, 7, false), (b, 7, false), (b, 6, false)],
                vec![
                    (c, 7, false),
                    (c, 4, false),
                    (c, 3, false),
                    (d, 7, false),
                    (d, 6, true),
                ],
            ],
            vec![
                vec![(a, 5, false), (b, 4, false)],
                vec![(d, 5, false), (d, 4, false)],
            ],
            vec![
                vec![(a, 2, false)],
                vec![(b, 3, true), (b, 1, false)],
                vec![(d, 3, false)],
            ],
            vec![vec![(c, 2, false)]],
        ]
        .into_iter()
        .map(|level| {
            level
                .into_iter()
                .map(|run| {
                    run.into_iter()
                        .map(|(key, ts, is_tombstone)| {
                            lsm_diagram_revision(key.as_bytes(), ts, is_tombstone)
                        })
                        .collect::<Vec<LsmRevision>>()
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>(),
    );

    Ok(())
}

fn lsm_diagram_value(key: &[u8], ts: usize) -> RevisionValue {
    RevisionValue::Regular(format!("{:?} {}", key, ts).into())
}

fn lsm_diagram_revision(key: &[u8], ts: usize, is_tombstone: bool) -> LsmRevision {
    LsmRevision {
        key: key.into(),
        ts: Timestamp(ts as u64),
        value: match is_tombstone {
            false => lsm_diagram_value(key, ts),
            true => RevisionValue::Tombstone,
        },
    }
}

async fn keyspace_from_diagram(diagram: Vec<(&str, &[u8])>) -> anyhow::Result<Keyspace> {
    fn find_touching(
        diagram: &[(&str, &[u8])],
        visited: &mut HashSet<(usize, usize)>,
        x: usize,
        y: usize,
    ) -> Vec<LsmRevision> {
        fn find_touching_inner(
            diagram: &[(&str, &[u8])],
            visited: &mut HashSet<(usize, usize)>,
            x: usize,
            y: usize,
            out: &mut Vec<LsmRevision>,
        ) {
            if visited.contains(&(x, y)) {
                return;
            }
            visited.insert((x, y));

            let key_str = diagram[y].0;
            let key = key_str.as_bytes().to_vec();
            let ts = Timestamp((x / 2 + 1) as u64);

            if let Some(value) = match diagram[y].1[x] {
                b'o' => Some(lsm_diagram_value(&key, ts.0 as usize)),
                b'x' => Some(RevisionValue::Tombstone),
                b' ' => None,
                _ => return,
            } {
                out.push(LsmRevision { key, ts, value });
            }

            for (dx, dy) in [(0isize, -1isize), (1, 0), (0, 1), (-1, 0)] {
                let next_x = (x as isize) + dx;
                let next_y = (y as isize) + dy;

                if next_x < 0
                    || next_x >= diagram[0].1.len() as isize
                    || next_y < 0
                    || next_y >= diagram.len() as isize
                {
                    continue;
                }

                find_touching_inner(diagram, visited, next_x as usize, next_y as usize, out);
            }
        }

        let mut out = vec![];
        find_touching_inner(diagram, visited, x, y, &mut out);
        out
    }

    let mut visited = HashSet::new();

    let x_max = diagram[0].1.len() - 1;
    let l0_active_revisions = find_touching(&diagram[..], &mut visited, x_max, 0);
    let l0_active = Memtable::new();
    for revision in l0_active_revisions {
        l0_active.insert(revision.key, revision.ts, revision.value);
    }
    let mut keyspace = Keyspace {
        l0_active: Arc::new(l0_active),
        l0_sealed: vec![Arc::new(Memtable::new())],
        levels: vec![Level { runs: Vec::new() }],
    };
    for x in (0..=x_max).rev().filter(|x| x % 2 == 1) {
        let mut level = Level { runs: Vec::new() };
        for y in (0..diagram.len()).filter(|y| y % 2 == 0) {
            let revisions = find_touching(&diagram[..], &mut visited, x, y);
            if revisions.is_empty() {
                continue;
            }

            if keyspace.l0_sealed[0].is_empty() {
                for revision in revisions {
                    keyspace.l0_sealed[0].insert(revision.key, revision.ts, revision.value);
                }
            } else {
                let mut v = vec![];
                let mut run_builder = RunBuilder::new(
                    &mut v,
                    RunId::new(),
                    KeyspaceId(ColoGroupId(1), 1),
                    1024, // block_size_target
                );
                for revision in revisions {
                    run_builder.push(revision).await?;
                }
                run_builder.finish().await?;
                let run = Run::open(Arc::new(MemFileReader::new(v))).await?;
                level.runs.push(Arc::new(run));
            }
        }

        if level.runs.is_empty() {
            continue;
        }

        keyspace.levels.push(level);
    }

    Ok(keyspace)
}
