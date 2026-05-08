use std::cmp;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::anyhow;
use async_stream::try_stream;
use byteorder::ByteOrder;
use byteorder::LittleEndian;
use futures::pin_mut;
use futures::Stream;
use futures::TryStreamExt;
use obsidian_common::Bound;
use obsidian_common::ColoGroupId;
use obsidian_common::Direction;
use obsidian_common::HistoryRange;
use obsidian_common::KeyOrBound;
use obsidian_common::KeyspaceId;
use obsidian_common::Range;
use obsidian_common::Revision;
use obsidian_common::RevisionValue;
use obsidian_common::Timestamp;
use obsidian_external::FileReader;
use obsidian_external::FileWriter;
use obsidian_util::binary_search_by_idx;
use obsidian_util::hexlify;
use obsidian_util::IteratorEither;
use uuid::Uuid;

use crate::block::dump_block;
use crate::block::Block;
use crate::block::BlockBuilder;
use crate::block_revision::BlockRevision;
use crate::util::PrefixCompressedKV;

#[derive(Clone)]
pub struct OlfFile {
    reader: Arc<dyn FileReader>,

    id: Uuid,
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

impl OlfFile {
    pub async fn open(reader: Arc<dyn FileReader>) -> anyhow::Result<Self> {
        let trailer = OlfFileTrailer::open(reader.deref()).await?;

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
        let size = reader.len() as usize;

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

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn keyspace_id(&self) -> KeyspaceId {
        self.keyspace_id
    }

    pub fn min_key(&self) -> &[u8] {
        &self.min_key[..]
    }

    pub fn max_key(&self) -> &[u8] {
        &self.max_key[..]
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub async fn get(
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

    pub fn scan(
        &self,
        ts: Timestamp,
        range: Range<Vec<u8>>,
        direction: Direction,
    ) -> impl Stream<Item = anyhow::Result<Revision>> + '_ {
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
                let block = Block::open(self.reader.deref(), block_end_offset as u64).await?;
                let block_scan = block.scan(ts, range.clone(), direction);
                pin_mut!(block_scan);
                while let Some(lsm_revision) = block_scan.try_next().await? {
                    yield Revision{
                        key: (self.keyspace_id, lsm_revision.key),
                        ts: lsm_revision.ts,
                        value: lsm_revision.value,
                    };
                }
            }
        }
    }

    pub fn history<'a>(
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

    pub fn range(&self) -> Range<Vec<u8>> {
        Range {
            lower: Bound::Before(self.min_key.clone()),
            upper: Bound::After(self.max_key.clone()),
        }
    }

    pub fn stream(&self) -> impl Stream<Item = anyhow::Result<Revision>> + '_ {
        try_stream! {
            for i in 0..self.index.len() {
                let block_end_offset = self.index.get_value(i);
                let block = Block::open(self.reader.deref(), block_end_offset as u64).await?;
                let block_stream = block.stream();
                pin_mut!(block_stream);
                while let Some(lsm_revision) = block_stream.try_next().await? {
                    yield Revision{
                        key: (self.keyspace_id, lsm_revision.key),
                        ts: lsm_revision.ts,
                        value: lsm_revision.value,
                    };
                }
            }
        }
    }

    async fn block_for_key(&self, k: &[u8]) -> anyhow::Result<Option<Block<'_>>> {
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
            Block::open(self.reader.deref(), block_end_offset as u64).await?,
        ))
    }
}

pub struct OlfFileBuilder<'a> {
    w: &'a mut dyn FileWriter,
    id: Uuid,
    keyspace_id: KeyspaceId,
    block_size_target: u64,

    bytes_written: u64,
    index: BTreeMap<Vec<u8>, u64>,
    last_key: Vec<u8>,
    buffer: BlockBuilder,
    min_ts: Timestamp,
    max_ts: Timestamp,
}

impl<'a> OlfFileBuilder<'a> {
    pub fn new(
        w: &'a mut dyn FileWriter,
        id: Uuid,
        keyspace_id: KeyspaceId,
        block_size_target: u64,
    ) -> Self {
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

    pub fn size_estimate(&self) -> u64 {
        self.bytes_written + self.buffer.size_estimate()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes_written == 0 && self.buffer.is_empty()
    }

    pub async fn push(&mut self, revision: Revision) -> anyhow::Result<()> {
        if revision.key.0 != self.keyspace_id {
            return Err(anyhow!("wrong keyspace {:?}", self.keyspace_id));
        }

        let revision_size_estimate = {
            let key_len = if self.buffer.contains_key(&revision.key.1) {
                0
            } else {
                revision.key.1.len() + 4
            };
            key_len + 10 + revision.value.len()
        };

        if !self.buffer.is_empty()
            && self.buffer.size_estimate() + (revision_size_estimate as u64)
                > self.block_size_target
            && !self.buffer.contains_key(&revision.key.1)
        {
            self.flush_block().await?;
        }

        self.min_ts = cmp::min(revision.ts, self.min_ts);
        self.max_ts = cmp::max(revision.ts, self.max_ts);
        self.buffer.push(BlockRevision {
            key: revision.key.1,
            ts: revision.ts,
            value: revision.value,
        })?;

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
        self.bytes_written += block.len() as u64;

        let block_end_offset = self.bytes_written;
        self.index
            .insert(first_key.clone(), block_end_offset as u64);

        self.buffer = BlockBuilder::new();

        Ok(())
    }

    pub async fn finish(mut self) -> anyhow::Result<()> {
        if !self.buffer.is_empty() {
            self.flush_block().await?;
        }

        if self.bytes_written == 0 {
            return Err(anyhow!("empty file"));
        }

        let index_offset = self.bytes_written;
        let mut encoded_index = Vec::new();
        PrefixCompressedKV::<()>::write(&mut encoded_index, &self.index);

        self.w.write_all(&encoded_index).await?;
        self.bytes_written += encoded_index.len() as u64;

        let max_key_offset = self.bytes_written;
        self.w.write_all(&self.last_key).await?;
        self.bytes_written += self.last_key.len() as u64;

        let trailer_offset = self.bytes_written;

        let trailer = OlfFileTrailer {
            id: self.id,
            keyspace_id: self.keyspace_id,
            min_ts: self.min_ts,
            max_ts: self.max_ts,
            max_key_offset: max_key_offset as u64,
            max_key_len: self.last_key.len() as u32,
            index_offset: index_offset as u64,
            index_len: encoded_index.len() as u64,
        };
        trailer.write(self.w, trailer_offset as u64).await?;

        Ok(())
    }
}

struct OlfFileTrailer {
    id: Uuid,
    keyspace_id: KeyspaceId,
    min_ts: Timestamp,
    max_ts: Timestamp,
    index_offset: u64,
    index_len: u64,
    max_key_offset: u64,
    max_key_len: u32,
}

impl OlfFileTrailer {
    const ENCODED_LEN: usize = 68;

    async fn open(reader: &dyn FileReader) -> anyhow::Result<Self> {
        let file_len = reader.len();
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
            Uuid::from_bytes(uuid_bytes)
        };
        let keyspace_id = KeyspaceId(
            ColoGroupId(LittleEndian::read_u32(&trailer[16..20])),
            LittleEndian::read_u32(&trailer[20..24]),
        );
        let min_ts = Timestamp::from_micros(LittleEndian::read_u64(&trailer[24..32]));
        let max_ts = Timestamp::from_micros(LittleEndian::read_u64(&trailer[32..40]));
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

    async fn write(&self, w: &mut dyn FileWriter, trailer_offset: u64) -> anyhow::Result<()> {
        let mut trailer = [0u8; Self::ENCODED_LEN];

        trailer[0..16].copy_from_slice(&self.id.as_bytes()[..]);
        LittleEndian::write_u32(&mut trailer[16..20], self.keyspace_id.0 .0);
        LittleEndian::write_u32(&mut trailer[20..24], self.keyspace_id.1);
        LittleEndian::write_u64(&mut trailer[24..32], self.min_ts.as_micros());
        LittleEndian::write_u64(&mut trailer[32..40], self.max_ts.as_micros());
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

/// Prints a debug representation of the given file to stdout.
pub async fn dump_olf_file(olf: &OlfFile) -> anyhow::Result<()> {
    println!("    min_ts: {}", olf.min_ts);
    println!("    max_ts: {}", olf.max_ts);
    println!("    range: {:?}", olf.range());
    println!("    index");
    for i in 0..olf.index.len() {
        println!(
            "      {} header offset {}",
            hexlify(&olf.index.get_key(i)),
            olf.index.get_value(i)
        );
    }
    println!("    blocks");
    for i in 0..olf.index.len() {
        println!("    == block {} ======", i);
        println!("    first key: [{}]", hexlify(&olf.index.get_key(i)),);
        println!("    block_end_offset: {}", olf.index.get_value(i));
        let block_end_offset = olf.index.get_value(i);
        let block = Block::open(olf.reader.deref(), block_end_offset as u64).await?;
        dump_block(&block).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::cmp::Reverse;
    use std::sync::Arc;

    use futures::StreamExt;
    use futures::TryStreamExt;
    use obsidian_common::Bound;
    use obsidian_common::ColoGroupId;
    use obsidian_common::Direction;
    use obsidian_common::KeyspaceId;
    use obsidian_common::Range;
    use obsidian_common::Revision;
    use obsidian_common::RevisionValue;
    use obsidian_common::Timestamp;
    use obsidian_external::mem::MemFileWriter;
    use proptest::prelude::*;
    use rand::RngCore;
    use uuid::Uuid;

    use super::dump_olf_file;
    use super::OlfFile;
    use super::OlfFileBuilder;

    #[tokio::test]
    async fn test_olf_file() -> anyhow::Result<()> {
        fn rand_bytes(n: usize) -> Vec<u8> {
            let mut out = vec![0u8; n];
            rand::thread_rng().fill_bytes(&mut out);
            out
        }
        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
        let revisions = vec![
            Revision {
                key: (keyspace_id, b"prefixbar".to_vec()),
                ts: Timestamp(20101),
                value: RevisionValue::Regular(rand_bytes(10_000)),
            },
            Revision {
                key: (keyspace_id, b"prefixbar".to_vec()),
                ts: Timestamp(19230),
                value: RevisionValue::Tombstone,
            },
            Revision {
                key: (keyspace_id, b"prefixbar".to_vec()),
                ts: Timestamp(10230),
                value: RevisionValue::Regular(rand_bytes(128)),
            },
            Revision {
                key: (keyspace_id, b"prefixfoo".to_vec()),
                ts: Timestamp(21925),
                value: RevisionValue::Regular(rand_bytes(10_000)),
            },
            Revision {
                key: (keyspace_id, b"prefixfoo".to_vec()),
                ts: Timestamp(12031),
                value: RevisionValue::Regular(rand_bytes(10_000)),
            },
        ];
        let mut file_writer = MemFileWriter::new();
        let mut builder = OlfFileBuilder::new(
            &mut file_writer,
            Uuid::new_v4(),
            KeyspaceId(ColoGroupId(1), 1),
            32768,
        );
        for revision in &revisions {
            builder.push(revision.clone()).await?;
        }
        builder.finish().await?;

        let run = OlfFile::open(Arc::new(file_writer.into_reader())).await?;

        assert_eq!(run.min_ts, Timestamp(10230));
        assert_eq!(run.max_ts, Timestamp(21925));
        assert_eq!(run.min_key, b"prefixbar".to_vec());
        assert_eq!(run.max_key, b"prefixfoo".to_vec());

        for revision in revisions {
            assert_eq!(
                run.get(revision.ts, &revision.key.1).await?,
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

        let mut file_writer = MemFileWriter::new();
        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
        let mut b = OlfFileBuilder::new(&mut file_writer, Uuid::new_v4(), keyspace_id, u64::MAX);

        for block in writes {
            for (key_str, versions_str) in block {
                for ts in (1..versions_str.len()).rev() {
                    let value = match versions_str[ts] {
                        b'o' => RevisionValue::Regular(format!("{} {}", key_str, ts).into()),
                        b'x' => RevisionValue::Tombstone,
                        _ => continue,
                    };

                    b.push(Revision {
                        key: (keyspace_id, key_str.into()),
                        ts: Timestamp(ts as u64),
                        value,
                    })
                    .await?;
                }
            }
            b.flush_block().await?;
        }
        b.finish().await?;

        let run = OlfFile::open(Arc::new(file_writer.into_reader())).await?;

        async fn check(
            run: &OlfFile,
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

                dump_olf_file(run).await?;

                assert_eq!(
                    results,
                    expected
                        .clone()
                        .into_iter()
                        .map(|(key_bytes, ts, tombstone)| Revision {
                            key: (run.keyspace_id(), key_bytes.into()),
                            ts: Timestamp(ts as u64),
                            value: match tombstone {
                                false =>
                                    RevisionValue::Regular(format!("{} {}", key_bytes, ts).into()),
                                true => RevisionValue::Tombstone,
                            },
                        })
                        .collect::<Vec<_>>(),
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
        fn proptest_olf_file(m in proptest::collection::btree_map(
            (proptest::collection::vec(u8::arbitrary(), 0..2), 0..(1u64 << 63)),
            proptest::option::of(proptest::collection::vec(u8::arbitrary(), 0..128)),
            1..4096,
        )) {
            let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();

            rt.block_on(async {
                let mut revisions = m.into_iter().map(|((key_bytes, ts), maybe_value)| Revision{
                    key: (KeyspaceId(ColoGroupId(1), 1), key_bytes),
                    ts: Timestamp(ts),
                    value: match maybe_value {
                        Some(v) => RevisionValue::Regular(v),
                        None => RevisionValue::Tombstone,
                    },
                }).collect::<Vec<Revision>>();
                revisions.sort_by_key(|revision| (revision.key.clone(), Reverse(revision.ts)));

                let mut file_writer = MemFileWriter::new();
                let mut builder = OlfFileBuilder::new(
                    &mut file_writer,
                    Uuid::new_v4(),
                    KeyspaceId(ColoGroupId(1), 1),
                    1024,
                );

                for revision in &revisions {
                    builder.push(revision.clone()).await.unwrap();
                }
                builder.finish().await.unwrap();


                let run = OlfFile::open(Arc::new(file_writer.into_reader())).await.unwrap();

                dump_olf_file(&run).await.unwrap();

                for revision in &revisions {
                    assert_eq!(
                        run.get(revision.ts, &revision.key.1[..]).await.unwrap(),
                        Some((revision.ts, revision.value.clone())),
                    );
                }

                let streamed_out_revisions = run
                    .stream()
                    .collect::<Vec<anyhow::Result<Revision>>>()
                    .await
                    .into_iter()
                    .collect::<anyhow::Result<Vec<Revision>>>()
                    .unwrap();

                assert_eq!(streamed_out_revisions, revisions);
            });
        }
    }
}
