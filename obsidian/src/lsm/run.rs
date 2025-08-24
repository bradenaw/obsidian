use std::cmp;
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
use crate::lsm::block::BlockBuilder;
use crate::lsm::util::LsmRevision;
use crate::lsm::util::PrefixCompressedKV;
use crate::lsm::RunId;
use crate::range::Bound;
use crate::range::KeyOrBound;
use crate::range::Range;
use crate::storage::FileReader;
use crate::types::ColoGroupId;
use crate::types::Direction;
use crate::types::HistoryRange;
use crate::types::KeyspaceId;
use crate::types::RevisionValue;
use crate::types::Timestamp;
use crate::util::binary_search_by_idx;
use crate::util::hexlify;
use crate::util::IteratorEither;

#[derive(Clone)]
pub(super) struct Run<R> {
    reader: R,

    id: RunId,
    size: usize,
    keyspace_id: KeyspaceId,
    min_ts: Timestamp,
    max_ts: Timestamp,

    // The run index is a map with one item per data block. The key is the minimum key that appears
    // in that block, and the value is the file offset of the _end_ of the block.
    index: PrefixCompressedKV<Vec<u8>>,

    min_key: Vec<u8>,
    max_key: Vec<u8>,
}

impl<R> Run<R> {
    // Assumes S is in (key, rev(ts)) order, and assumes termination at a reasonable size limit.
    pub(super) async fn write<W, S>(
        w: &mut W,
        id: RunId,
        keyspace_id: KeyspaceId,
        block_size_target: u64,
        s: S,
    ) -> anyhow::Result<()>
    where
        W: AsyncWrite + Unpin,
        S: Stream<Item = anyhow::Result<LsmRevision>>,
    {
        pin_mut!(s);

        let mut b = RunBuilder::new(w, id, keyspace_id, block_size_target);
        while let Some(revision) = s.next().await.transpose()? {
            b.push(revision).await?;
        }
        b.finish().await?;

        Ok(())
    }
}

impl<R: FileReader> Run<R> {
    pub(super) async fn open(reader: R) -> anyhow::Result<Self> {
        let trailer = RunTrailer::open(&reader).await?;

        let max_key = {
            let mut max_key = vec![0u8; trailer.max_key_len as usize];
            reader
                .read_exact_at(&mut max_key[..], trailer.max_key_offset)
                .await?;
            max_key
        };

        let index = {
            let mut index_bytes = vec![0u8; trailer.index_len as usize];
            reader
                .read_exact_at(&mut index_bytes[..], trailer.index_offset)
                .await?;
            PrefixCompressedKV::open(index_bytes)?
        };

        let min_key = index.get_key(0);
        let size = reader.len().await? as usize;

        Ok(Self {
            reader,

            id: trailer.id,
            keyspace_id: trailer.keyspace_id,
            min_ts: trailer.min_ts,
            max_ts: trailer.max_ts,

            size,
            index,

            min_key,
            max_key,
        })
    }

    pub(super) fn id(&self) -> RunId {
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
                let block_end_offset = self.index.get_value(i);
                let block = Block::open(&self.reader, block_end_offset as u64).await?;
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
                let block_end_offset = self.index.get_value(i);
                let block = Block::open(&self.reader, block_end_offset as u64).await?;
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
        let block_end_offset = self.index.get_value(block_header_idx);
        Ok(Some(
            Block::open(&self.reader, block_end_offset as u64).await?,
        ))
    }
}

struct RunBuilder<W> {
    w: W,
    id: RunId,
    keyspace_id: KeyspaceId,
    block_size_target: u64,

    bytes_written: usize,
    index: BTreeMap<Vec<u8>, u64>,
    last_key: Vec<u8>,
    buffer: BlockBuilder,
    min_ts: Timestamp,
    max_ts: Timestamp,
}

impl<W: AsyncWrite + Unpin> RunBuilder<W> {
    fn new(w: W, id: RunId, keyspace_id: KeyspaceId, block_size_target: u64) -> Self {
        Self {
            w,
            id,
            keyspace_id,
            block_size_target,
            buffer: BlockBuilder::new(),
            bytes_written: 0,
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
                revision.key.len() + 4
            };
            key_len + 10 + revision.value.len()
        };

        if !self.buffer.is_empty()
            && (self.buffer.size_estimate() + revision_size_estimate) as u64
                > self.block_size_target
            && !self.buffer.contains_key(&revision.key)
        {
            self.flush_block().await?;
        }

        self.min_ts = cmp::min(revision.ts, self.min_ts);
        self.max_ts = cmp::max(revision.ts, self.max_ts);
        self.buffer.push(revision)?;

        Ok(())
    }

    async fn flush_block(&mut self) -> anyhow::Result<()> {
        let (first_key, last_key) = match (self.buffer.first_key(), self.buffer.last_key()) {
            (Some(first_key), Some(last_key)) => (first_key, last_key),
            _ => anyhow::bail!("empty block"),
        };

        self.last_key = last_key.clone();

        let block = self.buffer.encode()?;
        self.w.write_all(&block[..]).await?;
        self.bytes_written += block.len();

        let block_end_offset = self.bytes_written;
        self.index
            .insert(first_key.clone(), block_end_offset as u64);

        self.buffer = BlockBuilder::new();

        Ok(())
    }

    async fn finish(&mut self) -> anyhow::Result<()> {
        if !self.buffer.is_empty() {
            self.flush_block().await?;
        }

        if self.bytes_written == 0 {
            return Err(anyhow!("empty run"));
        }

        let index_offset = self.bytes_written;
        let mut encoded_index = Vec::new();
        PrefixCompressedKV::<()>::write(&mut encoded_index, &self.index);

        self.w.write_all(&encoded_index).await?;
        self.bytes_written += encoded_index.len();

        let max_key_offset = self.bytes_written;
        self.w.write_all(&self.last_key).await?;
        self.bytes_written += self.last_key.len();

        let trailer_offset = self.bytes_written;

        let trailer = RunTrailer {
            id: self.id,
            keyspace_id: self.keyspace_id,
            min_ts: self.min_ts,
            max_ts: self.max_ts,
            max_key_offset: max_key_offset as u64,
            max_key_len: self.last_key.len() as u32,
            index_offset: index_offset as u64,
            index_len: encoded_index.len() as u64,
        };
        trailer.write(&mut self.w, trailer_offset as u64).await?;

        Ok(())
    }
}

struct RunTrailer {
    id: RunId,
    keyspace_id: KeyspaceId,
    min_ts: Timestamp,
    max_ts: Timestamp,
    index_offset: u64,
    index_len: u64,
    max_key_offset: u64,
    max_key_len: u32,
}

impl RunTrailer {
    const ENCODED_LEN: usize = 68;

    async fn open<R: FileReader>(reader: &R) -> anyhow::Result<Self> {
        let file_len = reader.len().await?;
        let mut trailer_offset_buf = [0u8; 4];
        reader
            .read_exact_at(&mut trailer_offset_buf[..], file_len - 4)
            .await?;
        let trailer_offset = LittleEndian::read_u32(&trailer_offset_buf[..]);

        let mut trailer = [0u8; Self::ENCODED_LEN];
        reader
            .read_exact_at(&mut trailer[..], trailer_offset as u64)
            .await?;

        let id = {
            let mut uuid_bytes = [0u8; 16];
            uuid_bytes.copy_from_slice(&trailer[0..16]);
            RunId(Uuid::from_bytes(uuid_bytes))
        };
        let keyspace_id = KeyspaceId(
            ColoGroupId(LittleEndian::read_u32(&trailer[16..20])),
            LittleEndian::read_u32(&trailer[20..24]),
        );
        let min_ts = Timestamp::from_nanos(LittleEndian::read_u64(&trailer[24..32]));
        let max_ts = Timestamp::from_nanos(LittleEndian::read_u64(&trailer[32..40]));
        let index_offset = LittleEndian::read_u64(&trailer[40..48]);
        let index_len = LittleEndian::read_u64(&trailer[48..56]);
        let max_key_offset = LittleEndian::read_u64(&trailer[56..64]);
        let max_key_len = LittleEndian::read_u32(&trailer[64..68]);

        Ok(Self {
            id,
            keyspace_id,
            min_ts,
            max_ts,
            index_offset,
            index_len,
            max_key_offset,
            max_key_len,
        })
    }

    async fn write<W: AsyncWrite + Unpin>(
        &self,
        mut w: W,
        trailer_offset: u64,
    ) -> anyhow::Result<()> {
        let mut trailer = [0u8; Self::ENCODED_LEN];

        trailer[0..16].copy_from_slice(&self.id.encode_fixed()[..]);
        LittleEndian::write_u32(&mut trailer[16..20], self.keyspace_id.0 .0);
        LittleEndian::write_u32(&mut trailer[20..24], self.keyspace_id.1);
        LittleEndian::write_u64(&mut trailer[24..32], self.min_ts.as_nanos());
        LittleEndian::write_u64(&mut trailer[32..40], self.max_ts.as_nanos());
        LittleEndian::write_u64(&mut trailer[40..48], self.index_offset);
        LittleEndian::write_u64(&mut trailer[48..56], self.index_len);
        LittleEndian::write_u64(&mut trailer[56..64], self.max_key_offset);
        LittleEndian::write_u32(&mut trailer[64..68], self.max_key_len);
        w.write_all(&trailer[..]).await?;

        let mut trailer_offset_buf = [0u8; 4];
        LittleEndian::write_u32(&mut trailer_offset_buf[..], trailer_offset as u32);
        w.write_all(&trailer_offset_buf[..]).await?;

        Ok(())
    }
}

pub(super) async fn dump_run<R: FileReader>(run: &Run<R>) -> anyhow::Result<()> {
    println!("    min_ts: {}", run.min_ts);
    println!("    max_ts: {}", run.max_ts);
    println!("    range: {:?}", run.range());
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
        println!("    block_end_offset: {}", run.index.get_value(i));
        let block_end_offset = run.index.get_value(i);
        let block = Block::open(&run.reader, block_end_offset as u64).await?;
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

    use super::dump_run;
    use super::Run;
    use super::RunBuilder;
    use crate::lsm::test::TestFile;
    use crate::lsm::util::LsmRevision;
    use crate::lsm::RunId;
    use crate::range::Bound;
    use crate::range::Range;
    use crate::types::ColoGroupId;
    use crate::types::Direction;
    use crate::types::KeyspaceId;
    use crate::types::RevisionValue;
    use crate::types::Timestamp;

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
            RunId::new(),
            KeyspaceId(ColoGroupId(1), 1),
            32768,
            futures::stream::iter(revisions.iter().map(|revision| Ok(revision.clone()))),
        )
        .await
        .unwrap();

        let run = Run::open(TestFile::from(v)).await?;

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
            RunId::new(),
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

        let run = Run::open(TestFile::from(v)).await?;

        async fn check(
            run: &Run<TestFile>,
            ts: Timestamp,
            range: Range<Vec<u8>>,
            expected: Vec<(&str, usize, bool)>,
        ) -> anyhow::Result<()> {
            for direction in [Direction::Asc, Direction::Desc] {
                let mut results: Vec<_> =
                    run.scan(ts, range.clone(), direction).try_collect().await?;

                if direction == Direction::Desc {
                    results.reverse();
                }

                dump_run(run).await?;

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
                    RunId::new(),
                    KeyspaceId(ColoGroupId(1), 1),
                    1024,
                    futures::stream::iter(revisions.iter().map(|revision| Ok(revision.clone()))),
                ).await.unwrap();

                let run = Run::open(TestFile::from(v)).await.unwrap();

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
