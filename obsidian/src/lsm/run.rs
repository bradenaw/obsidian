use std::collections::BTreeMap;

use anyhow::anyhow;
use async_stream::try_stream;
use byteorder::ByteOrder;
use byteorder::LittleEndian;
use futures::pin_mut;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::lsm::block::dump_block;
use crate::lsm::block::Block;
use crate::lsm::util::LsmRevision;
use crate::lsm::util::PrefixCompressedKV;
use crate::range::Bound;
use crate::range::KeyOrBound;
use crate::range::Range;
use crate::types::ColoGroupId;
use crate::types::Direction;
use crate::types::HistoryRange;
use crate::types::KeyspaceId;
use crate::types::RevisionValue;
use crate::types::Timestamp;
use crate::util::binary_search_by_idx;
use crate::util::hexlify;
use crate::util::AsyncReadExactAt;
use crate::util::IteratorEither;

#[derive(Clone)]
pub(super) struct Run<R> {
    r: R,

    id: Uuid,
    size: usize,
    keyspace_id: KeyspaceId,
    min_ts: Timestamp,
    max_ts: Timestamp,

    index: PrefixCompressedKV<u32>,

    min_key: Vec<u8>,
    max_key: Vec<u8>,
}

const INDEX_BLOCK_HEADER_SIZE: usize = 48;

impl<R> Run<R> {
    // Assumes S is in (key, rev(ts)) order, and assumes termination at a reasonable size limit.
    pub(super) async fn write<
        W: AsyncWrite + Unpin,
        S: Stream<Item = anyhow::Result<LsmRevision>>,
    >(
        w: &mut W,
        id: Uuid,
        keyspace_id: KeyspaceId,
        block_size_limit: u64,
        s: S,
    ) -> anyhow::Result<()> {
        pin_mut!(s);

        let mut b = RunBuilder::new(w, id, keyspace_id, block_size_limit);
        while let Some(revision) = s.next().await.transpose()? {
            b.push(revision).await?;
        }
        b.finish().await?;

        Ok(())
    }
}

impl<R: AsyncReadExactAt> Run<R> {
    pub(super) async fn open(r: R) -> anyhow::Result<Self> {
        let file_len = r.len().await?;
        let mut index_block_offset_buf = [0u8; 4];
        r.read_exact_at(&mut index_block_offset_buf[..], file_len - 4)
            .await?;
        let index_block_offset = LittleEndian::read_u32(&index_block_offset_buf[..]);

        let mut header = [0u8; INDEX_BLOCK_HEADER_SIZE];
        r.read_exact_at(&mut header[..], index_block_offset as u64)
            .await?;

        let id = {
            let mut uuid_bytes = [0u8; 16];
            uuid_bytes.copy_from_slice(&header[0..16]);
            Uuid::from_bytes(uuid_bytes)
        };
        let keyspace_id = KeyspaceId(
            ColoGroupId(LittleEndian::read_u32(&header[16..20])),
            LittleEndian::read_u32(&header[20..24]),
        );
        let min_ts = Timestamp::from_nanos(LittleEndian::read_u64(&header[24..32]));
        let max_ts = Timestamp::from_nanos(LittleEndian::read_u64(&header[32..40]));
        let max_key_len = LittleEndian::read_u32(&header[40..44]);
        let index_len = LittleEndian::read_u32(&header[44..48]);

        let max_key = {
            let mut max_key = vec![0u8; max_key_len as usize];
            r.read_exact_at(
                &mut max_key[..],
                (index_block_offset as u64) + (header.len() as u64),
            )
            .await?;
            max_key
        };

        let index = {
            let mut index_bytes = vec![0u8; index_len as usize];
            r.read_exact_at(
                &mut index_bytes[..],
                (index_block_offset as u64) + (header.len() as u64) + (max_key_len as u64),
            )
            .await?;
            PrefixCompressedKV::decode(index_bytes)
        };

        let min_key = index.get_key(0);

        let size = r.len().await? as usize;
        Ok(Self {
            r,

            id,
            size,
            keyspace_id,
            min_ts,
            max_ts,
            index,

            min_key,
            max_key,
        })
    }

    pub(super) fn id(&self) -> Uuid {
        self.id
    }

    pub(super) fn size(&self) -> usize {
        self.size
    }

    pub(super) async fn get(
        &self,
        ts: Timestamp,
        k: &[u8],
    ) -> anyhow::Result<Option<(Timestamp, RevisionValue)>> {
        if ts < self.min_ts {
            return Ok(None);
        }
        if let Some(block) = self.block_for_key(k).await? {
            return block.get(ts, k).await;
        }
        return Ok(None);
    }

    pub(super) fn scan(
        &self,
        ts: Timestamp,
        range: Range<Vec<u8>>,
        direction: Direction,
    ) -> impl Stream<Item = anyhow::Result<LsmRevision>> + '_ {
        try_stream! {
            if ts < self.min_ts {
                return;
            }

            let lower_block_idx = binary_search_by_idx(
                self.index.len(),
                KeyOrBound::Bound(range.lower.clone()),
                |idx| KeyOrBound::Key(self.index.get_key(idx)),
            )
            .unwrap_or_else(|idx| if idx != 0 { idx - 1 } else { idx });

            let upper_block_idx = binary_search_by_idx(
                self.index.len(),
                KeyOrBound::Bound(range.upper.clone()),
                |idx| KeyOrBound::Key(self.index.get_key(idx)),
            )
            .unwrap_or_else(core::convert::identity);

            let asc_block_idxs = lower_block_idx..upper_block_idx;
            let block_idxs = match direction {
                Direction::Asc => IteratorEither::Left(asc_block_idxs),
                Direction::Desc => IteratorEither::Right(asc_block_idxs.rev()),
            };

            for i in block_idxs {
                let block_header_offset = self.index.get_value(i);
                let block = Block::open(&self.r, block_header_offset as u64).await?;
                let block_scan = block.scan(ts, range.clone(), direction);
                pin_mut!(block_scan);
                while let Some(revision) = block_scan.try_next().await? {
                    yield revision;
                }
            }
        }
    }

    pub(super) fn history<'a>(
        &'a self,
        k: &[u8],
        range: HistoryRange,
        direction: Direction,
    ) -> impl Stream<Item = anyhow::Result<(Timestamp, RevisionValue)>> + 'a {
        let k_owned = k.to_vec();
        try_stream! {
            if !range.intersects(self.min_ts, self.max_ts) {
                return;
            }
            let block = match self.block_for_key(&k_owned).await? {
                Some(block) => block,
                None => return,
            };

            let history = block.history(&k_owned, range, direction);
            pin_mut!(history);
            while let Some(revision) = history.try_next().await? {
                yield revision;
            }
        }
    }

    pub(super) fn range(&self) -> Range<Vec<u8>> {
        Range {
            lower: Bound::Before(self.min_key.clone()),
            upper: Bound::After(self.max_key.clone()),
        }
    }

    pub(super) fn stream(&self) -> impl Stream<Item = anyhow::Result<LsmRevision>> + '_ {
        try_stream! {
            for i in 0..self.index.len() {
                let block_header_offset = self.index.get_value(i);
                let block = Block::open(&self.r, block_header_offset as u64).await?;
                let block_stream = block.stream();
                pin_mut!(block_stream);
                while let Some(revision) = block_stream.try_next().await? {
                    yield revision;
                }
            }
        }
    }

    async fn block_for_key(&self, k: &[u8]) -> anyhow::Result<Option<Block<'_, R>>> {
        let block_header_idx = match self.index.search(k) {
            Ok(idx) => idx,
            Err(idx) => {
                if idx == 0 {
                    return Ok(None);
                }
                idx - 1
            }
        };
        let block_header_offset = self.index.get_value(block_header_idx);
        Ok(Some(
            Block::open(&self.r, block_header_offset as u64).await?,
        ))
    }
}

struct RunBuilder<W> {
    w: W,
    id: Uuid,
    keyspace_id: KeyspaceId,
    block_size_limit: u64,

    bytes_written: usize,
    index: BTreeMap<Vec<u8>, u32>,
    last_key: Vec<u8>,
    buffer: BTreeMap<Vec<u8>, Vec<(Timestamp, RevisionValue)>>,
    buffer_size_estimate: u64,
    min_ts: Timestamp,
    max_ts: Timestamp,
}

impl<W: AsyncWrite + Unpin> RunBuilder<W> {
    fn new(w: W, id: Uuid, keyspace_id: KeyspaceId, block_size_limit: u64) -> Self {
        Self {
            w,
            id,
            keyspace_id,
            block_size_limit,
            buffer: BTreeMap::new(),
            bytes_written: 0,
            buffer_size_estimate: 0,
            index: BTreeMap::new(),
            min_ts: Timestamp::MAX,
            max_ts: Timestamp::ZERO,
            last_key: vec![],
        }
    }

    async fn push(&mut self, revision: LsmRevision) -> anyhow::Result<()> {
        let revision_size_estimate = {
            let key_len = if self.buffer.contains_key(&revision.key) {
                0
            } else {
                (revision.key.len() as u64) + 4
            };
            (key_len as u64) + 10 + (revision.value.len() as u64)
        };

        if !self.buffer.is_empty()
            && self.buffer_size_estimate + revision_size_estimate > self.block_size_limit
            && !self.buffer.contains_key(&revision.key)
        {
            self.flush_block().await?;
        }

        if let Some(prev_revision) = self
            .buffer
            .get(&revision.key)
            .map(|versions| versions.last())
            .flatten()
        {
            assert!(prev_revision.0 > revision.ts);
        }
        self.buffer
            .entry(revision.key)
            .or_insert_with(Vec::new)
            .push((revision.ts, revision.value));
        self.buffer_size_estimate += revision_size_estimate;

        self.min_ts = std::cmp::min(self.min_ts, revision.ts);
        self.max_ts = std::cmp::max(self.max_ts, revision.ts);

        Ok(())
    }

    async fn flush_block(&mut self) -> anyhow::Result<()> {
        let (first_key, last_key_) =
            match (self.buffer.first_key_value(), self.buffer.last_key_value()) {
                (Some((first_key, _)), Some((last_key, _))) => (first_key, last_key),
                _ => anyhow::bail!("empty block"),
            };
        self.last_key = last_key_.clone();

        let (block, header_offset_in_block) = Block::<()>::encode(&self.buffer)?;
        self.w.write_all(&block[..]).await?;

        let header_offset_in_file = self.bytes_written + header_offset_in_block;
        self.index
            .insert(first_key.clone(), header_offset_in_file as u32);

        self.bytes_written += block.len();
        self.buffer.clear();
        self.buffer_size_estimate = 0;

        Ok(())
    }

    async fn finish(&mut self) -> anyhow::Result<()> {
        if !self.buffer.is_empty() {
            self.flush_block().await?;
        }

        if self.bytes_written == 0 {
            return Err(anyhow!("empty run"));
        }

        let index_compressed = PrefixCompressedKV::encode(&self.index);

        let index_block_offset = self.bytes_written;
        let mut header = [0u8; INDEX_BLOCK_HEADER_SIZE];
        header[0..16].copy_from_slice(&self.id.as_bytes()[..]);
        LittleEndian::write_u32(&mut header[16..20], self.keyspace_id.0 .0);
        LittleEndian::write_u32(&mut header[20..24], self.keyspace_id.1);
        LittleEndian::write_u64(&mut header[24..32], self.min_ts.as_nanos());
        LittleEndian::write_u64(&mut header[32..40], self.max_ts.as_nanos());
        LittleEndian::write_u32(&mut header[40..44], self.last_key.len() as u32);
        LittleEndian::write_u32(&mut header[44..48], index_compressed.len() as u32);
        self.w.write_all(&header[..]).await?;
        self.w.write_all(&self.last_key[..]).await?;
        self.w.write_all(&index_compressed).await?;

        let mut index_block_offset_buf = [0u8; 4];
        LittleEndian::write_u32(&mut index_block_offset_buf[..], index_block_offset as u32);
        self.w.write_all(&index_block_offset_buf[..]).await?;

        Ok(())
    }
}

pub(super) async fn dump_run<R: AsyncReadExactAt>(run: &Run<R>) -> anyhow::Result<()> {
    println!("    min_ts: {}", run.min_ts);
    println!("    max_ts: {}", run.max_ts);
    println!("    index");
    for i in 0..run.index.len() {
        println!(
            "      {} header offset {}",
            hexlify(&run.index.get_key(i)),
            run.index.get_value(i)
        );
    }
    println!("    blocks");
    for i in 0..run.index.len() {
        println!("    == block {} ======", i);
        println!("    first key: [{}]", hexlify(&run.index.get_key(i)),);
        println!("    header_offset: {}", run.index.get_value(i));
        let header_offset = run.index.get_value(i);
        let block = Block::open(&run.r, header_offset as u64).await?;
        dump_block(&block).await?;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use std::cmp::Reverse;

    use futures::StreamExt;
    use futures::TryStreamExt;
    use proptest::prelude::*;
    use rand::RngCore;
    use uuid::Uuid;

    use super::dump_run;
    use super::Run;
    use super::RunBuilder;
    use crate::lsm::util::LsmRevision;
    use crate::range::Bound;
    use crate::range::Range;
    use crate::types::ColoGroupId;
    use crate::types::Direction;
    use crate::types::KeyspaceId;
    use crate::types::RevisionValue;
    use crate::types::Timestamp;
    use crate::util::AsyncReadExactAt;

    #[tokio::test]
    async fn test_run_file() -> anyhow::Result<()> {
        fn rand_bytes(n: usize) -> Vec<u8> {
            let mut out = vec![0u8; n];
            rand::thread_rng().fill_bytes(&mut out);
            out
        }
        let revisions = vec![
            LsmRevision {
                key: b"prefixbar".to_vec(),
                ts: Timestamp(20101),
                value: RevisionValue::Regular(rand_bytes(10_000)),
            },
            LsmRevision {
                key: b"prefixbar".to_vec(),
                ts: Timestamp(19230),
                value: RevisionValue::Tombstone,
            },
            LsmRevision {
                key: b"prefixbar".to_vec(),
                ts: Timestamp(10230),
                value: RevisionValue::Regular(rand_bytes(128)),
            },
            LsmRevision {
                key: b"prefixfoo".to_vec(),
                ts: Timestamp(21925),
                value: RevisionValue::Regular(rand_bytes(10_000)),
            },
            LsmRevision {
                key: b"prefixfoo".to_vec(),
                ts: Timestamp(12031),
                value: RevisionValue::Regular(rand_bytes(10_000)),
            },
        ];
        let mut v = vec![];
        Run::<()>::write(
            &mut v,
            Uuid::new_v4(),
            KeyspaceId(ColoGroupId(1), 1),
            32768,
            futures::stream::iter(revisions.iter().map(|revision| Ok(revision.clone()))),
        )
        .await
        .unwrap();

        let run = Run::open(v).await?;

        assert_eq!(run.min_ts, Timestamp(10230));
        assert_eq!(run.max_ts, Timestamp(21925));
        assert_eq!(run.min_key, b"prefixbar".to_vec());
        assert_eq!(run.max_key, b"prefixfoo".to_vec());

        for revision in revisions {
            assert_eq!(
                run.get(revision.ts, &revision.key).await?,
                Some((revision.ts, revision.value)),
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_scan() -> anyhow::Result<()> {
        let writes = [
            vec![
                //   ts=0123456789
                ("a", b" o  o    o"),
                ("b", b"   o     o"),
            ],
            vec![
                ("c", b"   o x    "),
                ("d", b"   oxo    "),
                ("e", b"    o   o "),
                ("f", b"     o  o "),
                ("g", b" o x  o  o"),
                ("h", b"  o oxo  o"),
            ],
            vec![
                ("i", b"  o  oo o "),
                ("j", b" xoxoxoxox"),
                ("k", b"        o "),
                ("l", b" ooooooooo"),
            ],
        ];

        let mut v = vec![];
        let mut b = RunBuilder::new(
            &mut v,
            Uuid::new_v4(),
            KeyspaceId(ColoGroupId(1), 1),
            u64::MAX,
        );

        for block in writes {
            for (key_str, versions_str) in block {
                for ts in (1..versions_str.len()).rev() {
                    let value = match versions_str[ts] {
                        b'o' => RevisionValue::Regular(format!("{} {}", key_str, ts).into()),
                        b'x' => RevisionValue::Tombstone,
                        _ => continue,
                    };

                    b.push(LsmRevision {
                        key: key_str.into(),
                        ts: Timestamp(ts as u64),
                        value,
                    })
                    .await?;
                }
            }
            b.flush_block().await?;
        }
        b.finish().await?;

        let run = Run::open(v).await?;

        async fn check<R: AsyncReadExactAt>(
            block: &Run<R>,
            ts: Timestamp,
            range: Range<Vec<u8>>,
            expected: Vec<(&str, usize, bool)>,
        ) -> anyhow::Result<()> {
            for direction in [Direction::Asc, Direction::Desc] {
                let mut results: Vec<_> = block
                    .scan(ts, range.clone(), direction)
                    .try_collect()
                    .await?;

                if direction == Direction::Desc {
                    results.reverse();
                }

                assert_eq!(
                    results,
                    expected
                        .clone()
                        .into_iter()
                        .map(|(key, ts, tombstone)| LsmRevision {
                            key: (key).into(),
                            ts: Timestamp(ts as u64),
                            value: match tombstone {
                                false => RevisionValue::Regular(format!("{} {}", key, ts).into()),
                                true => RevisionValue::Tombstone,
                            },
                        })
                        .collect::<Vec<LsmRevision>>(),
                    "direction={:?}",
                    direction,
                );
            }
            Ok(())
        }

        check(
            &run,
            Timestamp(5),
            Range {
                lower: Bound::Before("b".into()),
                upper: Bound::After("e".into()),
            },
            vec![
                ("b", 3, false),
                ("c", 5, true),
                ("d", 5, false),
                ("e", 4, false),
            ],
        )
        .await?;

        check(
            &run,
            Timestamp(4),
            Range::all(),
            vec![
                ("a", 4, false),
                ("b", 3, false),
                ("c", 3, false),
                ("d", 4, true),
                ("e", 4, false),
                // f doesn't exist yet
                ("g", 3, true),
                ("h", 4, false),
                ("i", 2, false),
                ("j", 4, false),
                // k doesn't exist yet
                ("l", 4, false),
            ],
        )
        .await?;

        check(
            &run,
            Timestamp(4),
            Range {
                lower: Bound::Before("c".into()),
                upper: Bound::After("h".into()),
            },
            vec![
                ("c", 3, false),
                ("d", 4, true),
                ("e", 4, false),
                // f doesn't exist yet
                ("g", 3, true),
                ("h", 4, false),
            ],
        )
        .await?;

        check(
            &run,
            Timestamp(4),
            Range {
                lower: Bound::After("b".into()),
                upper: Bound::Before("i".into()),
            },
            vec![
                ("c", 3, false),
                ("d", 4, true),
                ("e", 4, false),
                // f doesn't exist yet
                ("g", 3, true),
                ("h", 4, false),
            ],
        )
        .await?;

        Ok(())
    }

    proptest! {
        #[test]
        fn proptest_run_file(m in proptest::collection::btree_map(
            (proptest::collection::vec(u8::arbitrary(), 0..2), 0..(1u64 << 63)),
            proptest::option::of(proptest::collection::vec(u8::arbitrary(), 0..128)),
            1..4096,
        )) {
            let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();

            rt.block_on(async {
                let mut revisions = m.into_iter().map(|((key, ts), maybe_value)| LsmRevision{
                    key, ts: Timestamp(ts), value: match maybe_value {
                        Some(v) => RevisionValue::Regular(v),
                        None => RevisionValue::Tombstone,
                    },
                }).collect::<Vec<LsmRevision>>();
                revisions.sort_by_key(|revision| (revision.key.clone(), Reverse(revision.ts)));

                let mut v = vec![];
                Run::<()>::write(
                    &mut v,
                    Uuid::new_v4(),
                    KeyspaceId(ColoGroupId(1), 1),
                    1024,
                    futures::stream::iter(revisions.iter().map(|revision| Ok(revision.clone()))),
                ).await.unwrap();

                let run = Run::open(v).await.unwrap();

                dump_run(&run).await.unwrap();

                for revision in &revisions {
                    assert_eq!(
                        run.get(revision.ts, &revision.key[..]).await.unwrap(),
                        Some((revision.ts, revision.value.clone())),
                    );
                }

                let streamed_out_revisions = run
                    .stream()
                    .collect::<Vec<anyhow::Result<LsmRevision>>>()
                    .await
                    .into_iter()
                    .collect::<anyhow::Result<Vec<LsmRevision>>>()
                    .unwrap();

                assert_eq!(streamed_out_revisions, revisions);
            });
        }
    }
}
