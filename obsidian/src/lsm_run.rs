use std::collections::BTreeMap;

use async_stream::stream;
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

use crate::lsm_block::dump_block;
use crate::lsm_block::Block;
use crate::lsm_util::PrefixCompressedKV;
use crate::range::Bound;
use crate::range::KeyOrBound;
use crate::range::Range;
use crate::types::ColoGroupId;
use crate::types::Direction;
use crate::types::KeyspaceId;
use crate::types::Record;
use crate::types::Timestamp;
use crate::types::Value;
use crate::util::binary_search_by_idx;
use crate::util::hexlify;
use crate::util::AsyncReadExactAt;
use crate::util::IteratorEither;

#[derive(Clone)]
pub(crate) struct Run<R> {
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
    pub(crate) async fn write<W: AsyncWrite + Unpin, S: Stream<Item = anyhow::Result<Record>>>(
        w: &mut W,
        id: Uuid,
        keyspace_id: KeyspaceId,
        block_size_limit: u64,
        s: S,
    ) -> anyhow::Result<()> {
        async fn flush<W: AsyncWrite + Unpin>(
            w: &mut W,
            bytes_written: &mut usize,
            index: &mut BTreeMap<Vec<u8>, u32>,
            last_key: &mut Vec<u8>,
            buffer: &BTreeMap<Vec<u8>, Vec<(Timestamp, Value)>>,
        ) -> anyhow::Result<()> {
            let (first_key, last_key_) = match (buffer.first_key_value(), buffer.last_key_value()) {
                (Some((first_key, _)), Some((last_key, _))) => (first_key, last_key),
                _ => anyhow::bail!("empty block"),
            };
            *last_key = last_key_.clone();

            let (block, header_offset_in_block) = Block::<()>::encode(buffer)?;
            w.write_all(&block[..]).await?;
            let header_offset_in_file = *bytes_written + header_offset_in_block;

            index.insert(first_key.clone(), header_offset_in_file as u32);

            *bytes_written += block.len();

            Ok(())
        }

        pin_mut!(s);

        let mut buffer: BTreeMap<Vec<u8>, Vec<(Timestamp, Value)>> = BTreeMap::new();
        let mut bytes_written = 0;
        let mut buffer_size = 0;
        let mut index: BTreeMap<Vec<u8>, u32> = BTreeMap::new();
        let mut min_ts = Timestamp::MAX;
        let mut max_ts = Timestamp::ZERO;
        let mut last_key = vec![];
        while let Some(record) = s.next().await.transpose()? {
            let record_size = {
                let key_len = if buffer.contains_key(&record.key) {
                    0
                } else {
                    (record.key.len() as u64) + 4
                };
                (key_len as u64) + 10 + (record.value.len() as u64)
            };

            if !buffer.is_empty()
                && buffer_size + record_size > block_size_limit
                && !buffer.contains_key(&record.key)
            {
                flush(w, &mut bytes_written, &mut index, &mut last_key, &buffer).await?;
                buffer.clear();
                buffer_size = 0;
            }

            if let Some(prev_record) = buffer
                .get(&record.key)
                .map(|versions| versions.last())
                .flatten()
            {
                assert!(prev_record.0 > record.ts);
            }
            buffer
                .entry(record.key)
                .or_insert_with(Vec::new)
                .push((record.ts, record.value));
            buffer_size += record_size;

            min_ts = std::cmp::min(min_ts, record.ts);
            max_ts = std::cmp::max(max_ts, record.ts);
        }
        if !buffer.is_empty() {
            flush(w, &mut bytes_written, &mut index, &mut last_key, &buffer).await?;
        }

        let index_compressed = PrefixCompressedKV::encode(&index);

        let index_block_offset = bytes_written;
        let mut header = [0u8; INDEX_BLOCK_HEADER_SIZE];
        header[0..16].copy_from_slice(&id.as_bytes()[..]);
        LittleEndian::write_u32(&mut header[16..20], keyspace_id.0 .0);
        LittleEndian::write_u32(&mut header[20..24], keyspace_id.1);
        LittleEndian::write_u64(&mut header[24..32], min_ts.as_nanos());
        LittleEndian::write_u64(&mut header[32..40], max_ts.as_nanos());
        LittleEndian::write_u32(&mut header[40..44], last_key.len() as u32);
        LittleEndian::write_u32(&mut header[44..48], index_compressed.len() as u32);
        w.write_all(&header[..]).await?;
        w.write_all(&last_key[..]).await?;
        w.write_all(&index_compressed).await?;

        let mut index_block_offset_buf = [0u8; 4];
        LittleEndian::write_u32(&mut index_block_offset_buf[..], index_block_offset as u32);
        w.write_all(&index_block_offset_buf[..]).await?;

        Ok(())
    }
}

impl<R: AsyncReadExactAt> Run<R> {
    pub(crate) async fn open(r: R) -> anyhow::Result<Self> {
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

    pub(crate) fn id(&self) -> Uuid {
        self.id
    }

    pub(crate) fn size(&self) -> usize {
        self.size
    }

    pub(crate) async fn get(
        &self,
        ts: Timestamp,
        k: &[u8],
    ) -> anyhow::Result<Option<(Timestamp, Value)>> {
        if ts < self.min_ts {
            return Ok(None);
        }
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
        let block = Block::open(&self.r, block_header_offset as u64).await?;
        block.get(ts, k).await
    }

    pub(crate) fn scan(
        &self,
        ts: Timestamp,
        range: Range<Vec<u8>>,
        direction: Direction,
    ) -> impl Stream<Item = anyhow::Result<Record>> + '_ {
        try_stream! {
            if direction == Direction::Desc {
                todo!();
            }

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
                while let Some(record) = block_scan.try_next().await? {
                    yield record;
                }
            }
        }
    }

    pub(crate) fn range(&self) -> Range<Vec<u8>> {
        Range {
            lower: Bound::Before(self.min_key.clone()),
            upper: Bound::After(self.max_key.clone()),
        }
    }

    pub(crate) fn stream(&self) -> impl Stream<Item = anyhow::Result<Record>> + '_ {
        try_stream! {
            for i in 0..self.index.len() {
                let block_header_offset = self.index.get_value(i);
                let block = Block::open(&self.r, block_header_offset as u64).await?;
                let block_stream = block.stream();
                pin_mut!(block_stream);
                while let Some(record) = block_stream.try_next().await? {
                    yield record;
                }
            }
        }
    }

    pub(crate) fn into_stream(self) -> impl Stream<Item = anyhow::Result<Record>> {
        stream! {
            let mut s = self.stream().boxed_local();
            while let Some(x) = s.next().await {
                yield x;
            }
        }
    }
}

pub(crate) async fn dump_run<R: AsyncReadExactAt>(run: &Run<R>) -> anyhow::Result<()> {
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
    use proptest::prelude::*;
    use rand::RngCore;
    use uuid::Uuid;

    use crate::types::ColoGroupId;
    use crate::types::KeyspaceId;
    use crate::types::Record;
    use crate::types::Timestamp;
    use crate::types::Value;

    use super::dump_run;
    use super::Run;

    #[tokio::test]
    async fn test_run_file() -> anyhow::Result<()> {
        fn rand_bytes(n: usize) -> Vec<u8> {
            let mut out = vec![0u8; n];
            rand::thread_rng().fill_bytes(&mut out);
            out
        }
        let records = vec![
            Record {
                key: b"prefixbar".to_vec(),
                ts: Timestamp(20101),
                value: Value::Regular(rand_bytes(10_000)),
            },
            Record {
                key: b"prefixbar".to_vec(),
                ts: Timestamp(19230),
                value: Value::Tombstone,
            },
            Record {
                key: b"prefixbar".to_vec(),
                ts: Timestamp(10230),
                value: Value::Regular(rand_bytes(128)),
            },
            Record {
                key: b"prefixfoo".to_vec(),
                ts: Timestamp(21925),
                value: Value::Regular(rand_bytes(10_000)),
            },
            Record {
                key: b"prefixfoo".to_vec(),
                ts: Timestamp(12031),
                value: Value::Regular(rand_bytes(10_000)),
            },
        ];
        let mut v = vec![];
        Run::<()>::write(
            &mut v,
            Uuid::new_v4(),
            KeyspaceId(ColoGroupId(1), 1),
            32768,
            futures::stream::iter(records.iter().map(|record| Ok(record.clone()))),
        )
        .await
        .unwrap();

        let run = Run::open(v).await?;

        assert_eq!(run.min_ts, Timestamp(10230));
        assert_eq!(run.max_ts, Timestamp(21925));
        assert_eq!(run.min_key, b"prefixbar".to_vec());
        assert_eq!(run.max_key, b"prefixfoo".to_vec());

        for record in records {
            assert_eq!(
                run.get(record.ts, &record.key).await?,
                Some((record.ts, record.value)),
            );
        }

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
                let mut records = m.into_iter().map(|((key, ts), maybe_value)| Record{
                    key, ts: Timestamp(ts), value: match maybe_value {
                        Some(v) => Value::Regular(v),
                        None => Value::Tombstone,
                    },
                }).collect::<Vec<Record>>();
                records.sort_by_key(|record| (record.key.clone(), Reverse(record.ts)));

                let mut v = vec![];
                Run::<()>::write(
                    &mut v,
                    Uuid::new_v4(),
                    KeyspaceId(ColoGroupId(1), 1),
                    1024,
                    futures::stream::iter(records.iter().map(|record| Ok(record.clone()))),
                ).await.unwrap();

                let run = Run::open(v).await.unwrap();

                dump_run(&run).await.unwrap();

                for record in &records {
                    assert_eq!(
                        run.get(record.ts, &record.key[..]).await.unwrap(),
                        Some((record.ts, record.value.clone())),
                    );
                }

                let streamed_out_records = run
                    .stream()
                    .collect::<Vec<anyhow::Result<Record>>>()
                    .await
                    .into_iter()
                    .collect::<anyhow::Result<Vec<Record>>>()
                    .unwrap();

                assert_eq!(streamed_out_records, records);
            });
        }
    }
}
