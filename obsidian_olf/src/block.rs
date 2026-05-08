use std::cmp;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::ops::Deref;

use anyhow::anyhow;
use async_stream::try_stream;
use byteorder::ByteOrder;
use byteorder::LittleEndian;
use futures::Stream;
use obsidian_common::Direction;
use obsidian_common::HistoryRange;
use obsidian_common::KeyOrBound;
use obsidian_common::Range;
use obsidian_common::RevisionValue;
use obsidian_common::Timestamp;
use obsidian_external::FileReader;
use obsidian_util::binary_search_by_idx;
use obsidian_util::byte_width;
use obsidian_util::hexlify;
use obsidian_util::longest_shared_prefix_len;
use obsidian_util::IteratorEither;

use crate::block_revision::BlockRevision;
use crate::util::PackedVec2;
use crate::util::PrefixCompressedKV;

/// A Block is conceptually a [`BTreeMap<Vec<u8>, BTreeMap<Timestamp, RevisionValue>>`], but it is
/// compactly serialized and can be used as-is without fully deserializing.
pub(super) struct Block<'a> {
    values_offset_in_file: u64,
    key_index: PrefixCompressedKV<Vec<u8>>,
    version_index: BlockVersionIndex<Vec<u8>>,
    reader: &'a dyn FileReader,
}

impl<'a> Block<'a> {
    pub(super) async fn open(
        reader: &'a dyn FileReader,
        block_end_offset: u64,
    ) -> anyhow::Result<Block<'a>> {
        let trailer = BlockTrailer::open(reader, block_end_offset).await?;

        let block_offset_in_file = block_end_offset - (trailer.block_size as u64);

        let mut key_index_and_version_index_bytes =
            vec![0u8; (trailer.key_index_len + trailer.version_index_len) as usize];
        reader
            .read_exact_at(
                &mut key_index_and_version_index_bytes[..],
                block_offset_in_file + (trailer.key_index_offset_in_block as u64),
            )
            .await?;

        let version_index_bytes =
            key_index_and_version_index_bytes.split_off(trailer.key_index_len as usize);
        let key_index_bytes = key_index_and_version_index_bytes;

        let key_index = PrefixCompressedKV::open(key_index_bytes)?;

        Ok(Self {
            values_offset_in_file: block_offset_in_file,
            reader,
            key_index,
            version_index: BlockVersionIndex::open(version_index_bytes)?,
        })
    }

    pub(super) async fn get(
        &self,
        ts: Timestamp,
        k: &[u8],
    ) -> anyhow::Result<Option<(Timestamp, RevisionValue)>> {
        let key_idx = match self.key_index.search(k) {
            Ok(idx) => idx,
            Err(_) => return Ok(None),
        };
        let key_versions = self.versions_for_key(key_idx);

        let version_idx = binary_search_by_idx(key_versions.len(), Reverse(ts), |idx| {
            Reverse(key_versions.ts(idx))
        })
        .unwrap_or_else(core::convert::identity);
        if version_idx == key_versions.len() {
            return Ok(None);
        }
        let revision_ts = key_versions.ts(version_idx);

        Ok(Some((
            revision_ts,
            self.value(&key_versions, version_idx).await?,
        )))
    }

    pub(super) fn scan(
        &self,
        ts: Timestamp,
        range: Range<Vec<u8>>,
        direction: Direction,
    ) -> impl Stream<Item = anyhow::Result<BlockRevision>> + '_ {
        try_stream! {
            let key_idxs = match direction {
                Direction::Asc => {
                    let lower_key_idx =
                        binary_search_by_idx(
                            self.key_index.len(),
                            KeyOrBound::Bound(range.lower.clone()),
                            |idx| KeyOrBound::Key(self.key_index.get_key(idx)),
                        )
                        .unwrap_or_else(core::convert::identity);
                    IteratorEither::Left(lower_key_idx..self.key_index.len())
                },
                Direction::Desc => {
                    let upper_key_idx = binary_search_by_idx(
                            self.key_index.len(),
                            KeyOrBound::Bound(range.upper.clone()),
                            |idx| KeyOrBound::Key(self.key_index.get_key(idx)),
                        )
                        .unwrap_or_else(core::convert::identity);

                    IteratorEither::Right((0..upper_key_idx).rev())
                },
            };

            for j in key_idxs {
                let key = self.key_index.get_key(j);
                if !range.contains(&key) {
                    break;
                }

                let versions = self.versions_for_key(j);
                let version_idx = binary_search_by_idx(versions.len(), Reverse(ts), |idx| {
                    Reverse(versions.ts(idx))
                })
                .unwrap_or_else(core::convert::identity);
                if version_idx == versions.len() {
                    continue;
                }

                let ts = versions.ts(version_idx);
                let value = self.value(&versions, version_idx).await?;

                yield BlockRevision { key, ts, value };
            }
        }
    }

    pub(super) fn history<'b>(
        &'b self,
        k: &[u8],
        range: HistoryRange,
        direction: Direction,
    ) -> impl Stream<Item = anyhow::Result<(Timestamp, RevisionValue)>> + 'b {
        let k_owned = k.to_vec();
        try_stream! {
            let key_idx = match self.key_index.search(&k_owned) {
                Ok(idx) => idx,
                Err(_) => {
                    return;
                },
            };

            let key_versions = self.versions_for_key(key_idx);
            let (min, max) = range.as_min_max();

            let min_version_idx = match binary_search_by_idx(key_versions.len(), Reverse(min), |idx| {
                Reverse(key_versions.ts(idx))
            }) {
                Ok(idx) => idx,
                Err(idx) => if idx > 0 { idx-1 } else { return },
            };
            let max_version_idx = binary_search_by_idx(key_versions.len(), Reverse(max), |idx| {
                Reverse(key_versions.ts(idx))
            })
            .unwrap_or_else(core::convert::identity);

            // Reversed because versions are in descending order.
            let version_idxs_desc = max_version_idx..=min_version_idx;
            let version_idxs = match direction {
                Direction::Asc => IteratorEither::Left(version_idxs_desc.rev()),
                Direction::Desc => IteratorEither::Right(version_idxs_desc),
            };

            for idx in version_idxs {
                let revision_ts = key_versions.ts(idx);
                let value = self.value(&key_versions, idx).await?;

                assert!(min <= revision_ts, "{:?} <= {:?}", min, revision_ts);
                assert!(max >= revision_ts, "{:?} >= {:?}", max, revision_ts);

                yield (revision_ts, value);
            }
        }
    }

    /// Produces all revisions contained in this block in BlockRevision's natural ordering: (key,
    /// Reverse(ts)).
    pub(super) fn stream(&self) -> impl Stream<Item = anyhow::Result<BlockRevision>> + '_ {
        try_stream! {
            for j in 0..self.key_index.len() {
                let key = self.key_index.get_key(j);
                let versions = self.versions_for_key(j);
                for k in 0..versions.len() {
                    let ts = versions.ts(k);
                    let value = self.value(&versions, k).await?;
                    yield BlockRevision{key: key.clone(), ts, value};
                }
            }
        }
    }

    fn versions_for_key(&self, key_idx: usize) -> BlockVersionIndex<&[u8]> {
        let start_idx = self.key_index.get_value(key_idx) as usize;
        let end_idx = if key_idx == self.key_index.len() - 1 {
            self.version_index.len()
        } else {
            self.key_index.get_value(key_idx + 1) as usize
        };
        self.version_index.slice(start_idx, end_idx)
    }

    async fn value<'b>(
        &'b self,
        versions: &BlockVersionIndex<&'b [u8]>,
        idx: usize,
    ) -> anyhow::Result<RevisionValue> {
        let (value_start_in_block, value_end_in_block) = match versions.value_offsets(idx) {
            Some(v) => v,
            None => return Ok(RevisionValue::Tombstone),
        };
        let value_len = value_end_in_block - value_start_in_block;

        let mut value = vec![0u8; value_len as usize];
        self.reader
            .read_exact_at(
                &mut value[..],
                self.values_offset_in_file + (value_start_in_block as u64),
            )
            .await?;

        Ok(RevisionValue::Regular(value))
    }
}

pub(super) struct BlockBuilder {
    buffer: BTreeMap<Vec<u8>, Vec<(Timestamp, RevisionValue)>>,
    key_size_estimate: usize,
    value_size: usize,
    n_versions: usize,
    min_ts: Timestamp,
    max_ts: Timestamp,
}

impl BlockBuilder {
    pub(super) fn new() -> Self {
        Self {
            buffer: BTreeMap::new(),
            key_size_estimate: 0,
            value_size: 0,
            n_versions: 0,
            min_ts: Timestamp::MAX,
            max_ts: Timestamp::ZERO,
        }
    }

    pub(super) fn push(&mut self, revision: BlockRevision) -> anyhow::Result<()> {
        let key_len = if self.buffer.contains_key(&revision.key) {
            0
        } else if let Some((last_key, _)) = self.buffer.last_key_value() {
            if &revision.key < last_key {
                return Err(anyhow!(
                    "entries for block not in ascending key order: {} then {}",
                    hexlify(last_key),
                    hexlify(&revision.key[..]),
                ));
            }
            longest_shared_prefix_len(&revision.key[..], &last_key[..])
        } else {
            revision.key.len()
        };

        self.key_size_estimate += key_len;
        self.value_size += revision.value.len();
        self.n_versions += 1;
        self.min_ts = cmp::min(revision.ts, self.min_ts);
        self.max_ts = cmp::max(revision.ts, self.max_ts);

        if let Some(key_revisions) = self.buffer.get(&revision.key) {
            if let Some((prev_ts, prev_value)) = key_revisions.last() {
                if *prev_ts == revision.ts {
                    if prev_value == &revision.value {
                        return Err(anyhow!(
                            "revisions not in descending timestamp order: duplicate revision {:?}",
                            prev_ts,
                        ));
                    } else {
                        return Err(anyhow!(
                            "revisions not in descending timestamp order: conflicting values for {}@{:?}: {:?} != {:?}",
                            hexlify(&revision.key),
                            prev_ts,
                            prev_value,
                            revision.value,
                        ));
                    }
                }
                if *prev_ts <= revision.ts {
                    return Err(anyhow!(
                        "revisions not in descending timestamp order: {:?} then {:?}",
                        prev_ts,
                        revision.ts
                    ));
                }
            }
        }

        self.buffer
            .entry(revision.key)
            .or_insert_with(Vec::new)
            .push((revision.ts, revision.value));

        Ok(())
    }

    pub(super) fn size_estimate(&self) -> u64 {
        let key_offset_size = byte_width(self.key_size_estimate as u64);
        let version_offset_size = byte_width(self.n_versions as u64);
        let value_offset_size = byte_width((self.value_size as u64) << 1);
        let timestamp_size = if self.buffer.is_empty() {
            1
        } else {
            byte_width(self.max_ts.as_micros() - self.min_ts.as_micros())
        };
        let n_keys = self.buffer.len();
        (self.key_size_estimate
            + self.value_size
            + ((key_offset_size + version_offset_size) * n_keys)
            + ((value_offset_size + timestamp_size) * self.n_versions)) as u64
    }

    pub(super) fn first_key(&self) -> Option<&Vec<u8>> {
        self.buffer
            .first_key_value()
            .map(|(first_key, _)| first_key)
    }

    pub(super) fn last_key(&self) -> Option<&Vec<u8>> {
        self.buffer.last_key_value().map(|(last_key, _)| last_key)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub(super) fn contains_key(&self, key: &[u8]) -> bool {
        self.buffer.contains_key(key)
    }

    pub(super) fn encode(&self) -> anyhow::Result<Vec<u8>> {
        if self.buffer.is_empty() {
            return Err(anyhow!("empty block"));
        }
        // For this example block:
        //
        // key       ts   value
        // ====================
        // abcdef    5    "bar"
        //           2    "foo"
        // abcghi    4    "baz"
        //           3    <TOMBSTONE>
        //           1    "hello"
        //
        //
        // First, we smash all of the values together in order and remember the offset of the start
        // of each:
        //           1
        // 01234567890123
        // barfoobazhello
        // ^  ^  ^  ^
        //
        // And we store a list of (timestamp, tombstone, value_offset) in BlockRevision order.
        // [
        //   (5, false, 0),
        //   (2, false, 3),
        //   (4, false, 6),
        //   (3, true, 6),
        //   (1, false, 9),
        // ]
        //
        // Since timestamps in a block together will not differ by much from each other (usually
        // they're written around the same time) and tombstone is only one bit, we actually store
        // each 3-tuple as
        //   ts-block_min_ts, value_offset<<1 | tombstone
        //
        // We compute up-front how many bytes we actually need to fit the largest of the first
        // values in the block, and encode all of them using only that many. That means they're
        // fixed-size within a block and easy to search, but also do not take up much space.
        //
        //   block_max_ts - block_min_ts       bytes needed for timestamp
        //   =========================================================================
        //   256us                             1
        //   ~66ms                             2
        //   ~4 hours                          3
        //   ~48 days                          4
        //   ~34 years                         5
        //
        // Thus, very new blocks (which contain revisions written at similar times) will use 3-4
        // bytes and old blocks (which contain revisions from many different times compacted
        // together) will use 4-5.
        //
        //
        // Then we prefix compress the keys using a similar technique. We store the longest-shared
        // prefix and then all of the suffixes smashed together just like the values above, and
        // then store a list of fixed-sized (key_offset, versions_offset).
        //
        //   abc
        //   defghi
        //   key_offset  versions_offset
        //   0           0
        //   3           2

        let mut key_index: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
        let mut block = vec![];

        let mut version_index = Vec::new();
        for (key, key_versions) in &self.buffer {
            key_index.insert(key.clone(), version_index.len() as u64);
            for (ts, value) in key_versions {
                let value_offset = block.len();

                let is_tombstone = match value {
                    RevisionValue::Regular(value) => {
                        block.extend_from_slice(&value[..]);
                        false
                    }
                    RevisionValue::Tombstone => true,
                };

                version_index.push((*ts, is_tombstone, value_offset as u32));
            }
        }

        let values_len = block.len();
        let key_index_offset_in_block = block.len();
        PrefixCompressedKV::<()>::write(&mut block, &key_index);
        let key_index_len = block.len() - key_index_offset_in_block;

        let version_index_offset_in_block = block.len();
        BlockVersionIndex::<()>::write(&mut block, &version_index, values_len as u32);
        let version_index_len = block.len() - version_index_offset_in_block;

        let trailer = BlockTrailer {
            key_index_offset_in_block: key_index_offset_in_block as u32,
            key_index_len: key_index_len as u32,
            version_index_offset_in_block: version_index_offset_in_block as u32,
            version_index_len: version_index_len as u32,
            block_size: (block.len() + BlockTrailer::ENCODED_LEN) as u32,
        };
        trailer.write(&mut block);

        Ok(block)
    }
}

struct BlockTrailer {
    key_index_offset_in_block: u32,
    key_index_len: u32,
    version_index_offset_in_block: u32,
    version_index_len: u32,
    block_size: u32,
}

impl BlockTrailer {
    const ENCODED_LEN: usize = 20;

    fn write(&self, out: &mut Vec<u8>) {
        let mut trailer = [0u8; Self::ENCODED_LEN];
        LittleEndian::write_u32(&mut trailer[0..4], self.key_index_offset_in_block as u32);
        LittleEndian::write_u32(&mut trailer[4..8], self.key_index_len as u32);
        LittleEndian::write_u32(
            &mut trailer[8..12],
            self.version_index_offset_in_block as u32,
        );
        LittleEndian::write_u32(&mut trailer[12..16], self.version_index_len as u32);
        LittleEndian::write_u32(&mut trailer[16..20], self.block_size);
        out.extend_from_slice(&trailer[..]);
    }

    async fn open(reader: &dyn FileReader, block_end_offset: u64) -> anyhow::Result<Self> {
        let mut trailer = [0u8; Self::ENCODED_LEN];
        reader
            .read_exact_at(
                &mut trailer[..],
                block_end_offset - (Self::ENCODED_LEN as u64),
            )
            .await?;

        let key_index_offset_in_block = LittleEndian::read_u32(&trailer[0..4]);
        let key_index_len = LittleEndian::read_u32(&trailer[4..8]);
        let version_index_offset_in_block = LittleEndian::read_u32(&trailer[8..12]);
        let version_index_len = LittleEndian::read_u32(&trailer[12..16]);
        let block_size = LittleEndian::read_u32(&trailer[16..20]);

        Ok(Self {
            key_index_offset_in_block,
            key_index_len,
            version_index_offset_in_block,
            version_index_len,
            block_size,
        })
    }
}

struct BlockVersionIndex<B> {
    min_ts: Timestamp,
    values_len: u32,
    encoded: PackedVec2<B>,
}

impl<B> BlockVersionIndex<B> {
    pub(super) fn write(out: &mut Vec<u8>, v: &[(Timestamp, bool, u32)], values_len: u32) {
        let min_ts = v
            .iter()
            .map(|(ts, _, _)| ts)
            .min()
            .unwrap_or(&Timestamp::ZERO);

        let packed: Vec<_> = v
            .iter()
            .map(|(ts, tombstone, value_offset)| {
                let tombstone_bit = match tombstone {
                    true => 1u64,
                    false => 0u64,
                };
                (
                    (ts.as_micros() - min_ts.as_micros()),
                    (*value_offset as u64) << 1 | tombstone_bit,
                )
            })
            .collect();

        PackedVec2::<()>::write(out, &packed);
        let mut footer = [0u8; 12];
        LittleEndian::write_u64(&mut footer[..8], min_ts.as_micros());
        LittleEndian::write_u32(&mut footer[8..], values_len);
        out.extend_from_slice(&footer[..])
    }
}

impl<B: Deref<Target = [u8]> + Slice> BlockVersionIndex<B> {
    pub(super) fn open(b: B) -> anyhow::Result<Self> {
        if b.len() < 12 {
            return Err(anyhow!("BlockVersionIndex too short: {} < {}", b.len(), 12));
        }

        let min_ts = Timestamp::from_micros(LittleEndian::read_u64(&b[b.len() - 12..b.len() - 4]));
        let values_len = LittleEndian::read_u32(&b[b.len() - 4..]);
        let packed_end = b.len() - 12;

        Ok(Self {
            min_ts,
            values_len,
            encoded: PackedVec2::<B>::open(b.slice(0, packed_end))?,
        })
    }

    pub(super) fn len(&self) -> usize {
        self.encoded.len()
    }

    pub(super) fn elem(&self, idx: usize) -> (Timestamp, bool, u32) {
        let (ts_offset, value_offset_and_tombstone) = self.encoded.get(idx);
        let ts = Timestamp::from_micros(ts_offset + self.min_ts.as_micros());
        let tombstone = value_offset_and_tombstone & 1 == 1;
        let value_offset = (value_offset_and_tombstone >> 1) as u32;

        (ts, tombstone, value_offset)
    }

    pub(super) fn ts(&self, idx: usize) -> Timestamp {
        self.elem(idx).0
    }

    pub(super) fn value_offsets(&self, idx: usize) -> Option<(u32, u32)> {
        let (_, tombstone, start) = self.elem(idx);
        if tombstone {
            return None;
        }
        let end = if idx == self.len() - 1 {
            self.values_len
        } else {
            self.elem(idx + 1).2
        };
        Some((start, end))
    }

    fn slice<'a>(&'a self, start_idx: usize, end_idx: usize) -> BlockVersionIndex<&'a [u8]> {
        let values_len = if end_idx == self.len() {
            self.values_len
        } else {
            self.elem(end_idx).2
        };
        BlockVersionIndex {
            min_ts: self.min_ts,
            values_len,
            encoded: self.encoded.slice(start_idx, end_idx),
        }
    }

    fn borrow<'a>(&'a self) -> BlockVersionIndex<&'a [u8]> {
        BlockVersionIndex {
            min_ts: self.min_ts,
            values_len: self.values_len,
            encoded: self.encoded.borrow(),
        }
    }
}

trait Slice {
    fn slice(self, start_idx: usize, end_idx: usize) -> Self;
}

impl Slice for Vec<u8> {
    fn slice(mut self, start_idx: usize, end_idx: usize) -> Self {
        if end_idx < self.len() {
            self.truncate(end_idx);
        }
        if start_idx > 0 {
            return self.split_off(start_idx);
        }
        return self;
    }
}

impl Slice for &[u8] {
    fn slice(self, start_idx: usize, end_idx: usize) -> Self {
        &self[start_idx..end_idx]
    }
}

pub(super) async fn dump_block<'a>(block: &Block<'a>) -> anyhow::Result<()> {
    println!("    n_keys: {}", block.key_index.len());
    println!("    n_versions: {}", block.version_index.len());
    println!("      == keys ======");
    for i in 0..block.key_index.len() {
        let key = block.key_index.get_key(i);
        let versions_offset = block.key_index.get_value(i);
        println!("        [{}] {}", hexlify(&key), versions_offset);
    }
    println!("      == versions ======");
    for i in 0..block.version_index.len() {
        let value_str = match block.version_index.value_offsets(i) {
            Some((value_start, value_end)) => format!("({}, {})", value_start, value_end),
            None => "<TOMBSTONE>".into(),
        };

        println!(
            "        {} {} {} {:?}",
            i,
            block.version_index.ts(i),
            value_str,
            block.value(&block.version_index.borrow(), i).await?,
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use futures::TryStreamExt;
    use obsidian_common::Bound;
    use obsidian_common::Direction;
    use obsidian_common::HistoryRange;
    use obsidian_common::Range;
    use obsidian_common::RevisionValue;
    use obsidian_common::Timestamp;
    use obsidian_external::mem::MemFileReader;

    use super::Block;
    use super::BlockBuilder;
    use crate::block_revision::BlockRevision;

    fn encode(
        buffer: &BTreeMap<Vec<u8>, Vec<(Timestamp, RevisionValue)>>,
    ) -> anyhow::Result<Vec<u8>> {
        let mut builder = BlockBuilder::new();
        for (key, values) in buffer {
            for (ts, value) in values {
                builder.push(BlockRevision {
                    key: key.clone(),
                    ts: *ts,
                    value: value.clone(),
                })?;
            }
        }
        builder.encode()
    }

    #[tokio::test]
    async fn test_get() -> anyhow::Result<()> {
        let aa: Vec<u8> = "aa".into();
        let ab: Vec<u8> = "ab".into();
        let aa_279: RevisionValue = RevisionValue::Regular("foo".into());
        let aa_265: RevisionValue = RevisionValue::Regular("bar".into());
        let ab_341: RevisionValue = RevisionValue::Regular("baz".into());
        let ab_302: RevisionValue = RevisionValue::Regular("qux".into());
        let ab_290: RevisionValue = RevisionValue::Regular("garply".into());
        let kvs = {
            let mut kvs = BTreeMap::new();
            kvs.insert(
                aa.clone(),
                vec![
                    (Timestamp(279), aa_279.clone()),
                    (Timestamp(265), aa_265.clone()),
                ],
            );
            kvs.insert(
                ab.clone(),
                vec![
                    (Timestamp(341), ab_341.clone()),
                    (Timestamp(302), ab_302.clone()),
                    (Timestamp(297), RevisionValue::Tombstone),
                    (Timestamp(290), ab_290.clone()),
                ],
            );
            kvs
        };
        let encoded = encode(&kvs)?;
        let end_offset = encoded.len() as u64;
        let f = MemFileReader::new(encoded);
        let block = Block::open(&f, end_offset).await?;

        assert_eq!(
            block.get(Timestamp(279), &aa[..]).await?,
            Some((Timestamp(279), aa_279.clone()))
        );
        assert_eq!(
            block.get(Timestamp(265), &aa[..]).await?,
            Some((Timestamp(265), aa_265.clone()))
        );
        assert_eq!(block.get(Timestamp(123), &aa[..]).await?, None);

        assert_eq!(
            block.get(Timestamp(295), &aa[..]).await?,
            Some((Timestamp(279), aa_279.clone()))
        );
        assert_eq!(
            block.get(Timestamp(269), &aa[..]).await?,
            Some((Timestamp(265), aa_265.clone()))
        );

        assert_eq!(
            block.get(Timestamp(341), &ab[..]).await?,
            Some((Timestamp(341), ab_341.clone()))
        );
        assert_eq!(
            block.get(Timestamp(302), &ab[..]).await?,
            Some((Timestamp(302), ab_302.clone()))
        );
        assert_eq!(
            block.get(Timestamp(297), &ab[..]).await?,
            Some((Timestamp(297), RevisionValue::Tombstone))
        );
        assert_eq!(
            block.get(Timestamp(290), &ab[..]).await?,
            Some((Timestamp(290), ab_290.clone()))
        );
        assert_eq!(block.get(Timestamp(289), &ab[..]).await?, None);

        assert_eq!(
            block.get(Timestamp(500), &ab[..]).await?,
            Some((Timestamp(341), ab_341.clone()))
        );
        assert_eq!(
            block.get(Timestamp(340), &ab[..]).await?,
            Some((Timestamp(302), ab_302.clone()))
        );
        assert_eq!(
            block.get(Timestamp(300), &ab[..]).await?,
            Some((Timestamp(297), RevisionValue::Tombstone))
        );
        assert_eq!(
            block.get(Timestamp(296), &ab[..]).await?,
            Some((Timestamp(290), ab_290.clone()))
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_scan() -> anyhow::Result<()> {
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

        let mut kvs = BTreeMap::new();
        for (key_str, versions_str) in writes {
            let mut versions = vec![];
            for ts in 1..versions_str.len() {
                let value = match versions_str[ts] {
                    b'o' => RevisionValue::Regular(format!("{} {}", key_str, ts).into()),
                    b'x' => RevisionValue::Tombstone,
                    _ => continue,
                };

                versions.push((Timestamp(ts as u64), value));
            }
            versions.reverse();
            kvs.insert(key_str.into(), versions);
        }

        let encoded = encode(&kvs)?;
        let end_offset = encoded.len() as u64;
        let f = MemFileReader::new(encoded);
        let block = Block::open(&f, end_offset).await?;

        async fn check<'a>(
            block: &Block<'a>,
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
                        .map(|(key, ts, tombstone)| BlockRevision {
                            key: (key).into(),
                            ts: Timestamp(ts as u64),
                            value: match tombstone {
                                false => RevisionValue::Regular(format!("{} {}", key, ts).into()),
                                true => RevisionValue::Tombstone,
                            },
                        })
                        .collect::<Vec<BlockRevision>>(),
                    "direction={:?}",
                    direction,
                );
            }
            Ok(())
        }

        check(
            &block,
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
            &block,
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

        Ok(())
    }

    #[tokio::test]
    async fn test_history() -> anyhow::Result<()> {
        let kvs = vec![
            (
                b"a".to_vec(),
                vec![
                    (Timestamp(5), RevisionValue::Regular(b"a five".to_vec())),
                    (Timestamp(2), RevisionValue::Regular(b"a two".to_vec())),
                ],
            ),
            (
                b"b".to_vec(),
                vec![
                    (Timestamp(9), RevisionValue::Regular(b"b nine".to_vec())),
                    (Timestamp(7), RevisionValue::Regular(b"b seven".to_vec())),
                    (Timestamp(4), RevisionValue::Tombstone),
                    (Timestamp(2), RevisionValue::Regular(b"b two".to_vec())),
                ],
            ),
            (
                b"c".to_vec(),
                vec![(Timestamp(3), RevisionValue::Regular(b"c three".to_vec()))],
            ),
        ]
        .into_iter()
        .collect();
        let encoded = encode(&kvs)?;
        let end_offset = encoded.len() as u64;
        let f = MemFileReader::new(encoded);
        let block = Block::open(&f, end_offset).await?;

        assert_eq!(
            block
                .history(
                    b"b",
                    HistoryRange::Between(Timestamp(4), Timestamp(7)),
                    Direction::Asc,
                )
                .try_collect::<Vec<_>>()
                .await?,
            vec![
                (Timestamp(4), RevisionValue::Tombstone),
                (Timestamp(7), RevisionValue::Regular(b"b seven".to_vec())),
            ],
        );

        assert_eq!(
            block
                .history(
                    b"b",
                    HistoryRange::Between(Timestamp(3), Timestamp(8)),
                    Direction::Asc,
                )
                .try_collect::<Vec<_>>()
                .await?,
            vec![
                (Timestamp(4), RevisionValue::Tombstone),
                (Timestamp(7), RevisionValue::Regular(b"b seven".to_vec())),
            ],
        );

        Ok(())
    }
}
