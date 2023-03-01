use std::cmp::Reverse;
use std::collections::BTreeMap;

use anyhow::anyhow;
use async_stream::try_stream;
use byteorder::ByteOrder;
use byteorder::LittleEndian;
use futures::Stream;

use crate::lsm_util::PrefixCompressedKV;
use crate::range::KeyOrBound;
use crate::range::Range;
use crate::types::Direction;
use crate::types::HistoryRange;
use crate::types::Record;
use crate::types::Timestamp;
use crate::types::Value;
use crate::util::binary_search_by_idx;
use crate::util::byte_width;
use crate::util::hexlify;
use crate::util::AsyncReadExactAt;
use crate::util::IteratorEither;

/// A Block is conceptually a BTreeMap<Vec<u8>, BTreeMap<Timestamp, Value>>, but it is compactly
/// serialized and can be used as-is without fully deserializing.
pub(crate) struct Block<'a, R> {
    values_len: usize,
    n_versions: usize,
    min_ts: Timestamp,
    ts_bytes: usize,
    offset_bytes: usize,
    index: PrefixCompressedKV<u16>,
    versions_bytes: Vec<u8>,
    header_offset: u64,
    r: &'a R,
}

const BLOCK_INDEX_HEADER_SIZE: usize = 18;

impl<'a, R> Block<'a, R> {
    /// Assumes that kvs values are in reverse order by timestamp and that the total size of all
    /// values is less than 64K.
    ///
    /// Returns the encoded block and the offset of the header within the block.
    pub(crate) fn encode(
        kvs: &BTreeMap<Vec<u8>, Vec<(Timestamp, Value)>>,
    ) -> anyhow::Result<(Vec<u8>, usize)> {
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
        // And we store a list of (timestamp, tombstone, value_offset) in Record order.
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
        //   (ts-block_min_ts)<<1 | tombstone, value_offset
        //
        // We compute up-front how many bytes we actually need to fit the largest of the first
        // values in the block, and encode all of them using only that many. That means they're
        // fixed-size within a block and easy to search, but also do not take up much space.
        //
        //   block_max_ts - block_min_ts       bytes needed for (timestamp, tombstone)
        //   =========================================================================
        //   128us                             1
        //   ~33ms                             2
        //   ~2 hours                          3
        //   ~24 days                          4
        //   ~17 years                         5
        //
        // Thus, very new blocks (which contain records written at similar times) will use 3-4
        // bytes and old blocks (which contain records from many different times compacted
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
        let (min_ts, max_ts, values_len) = kvs
            .values()
            .map(|versions| {
                let min_ts = versions.last()?.0;
                let max_ts = versions.first()?.0;
                let values_len: usize = versions.iter().map(|(_, v)| v.len()).sum();
                Some((min_ts, max_ts, values_len))
            })
            .flatten()
            .reduce(
                |(min_ts0, max_ts0, values_len0), (min_ts1, max_ts1, values_len1)| {
                    (
                        std::cmp::min(min_ts0, min_ts1),
                        std::cmp::max(max_ts0, max_ts1),
                        values_len0 + values_len1,
                    )
                },
            )
            .ok_or_else(|| anyhow!("malformed block"))?;
        // Shift here for room for the tombstone bit.
        let bytes_per_ts_offset =
            std::cmp::max(byte_width((max_ts.as_nanos() - min_ts.as_nanos()) << 1), 1);
        let bytes_per_value_offset = std::cmp::max(byte_width(values_len as u64), 1);

        let mut index: BTreeMap<Vec<u8>, u16> = BTreeMap::new();
        let mut block = vec![];

        let mut n_versions = 0;
        let mut versions = Vec::new();
        for (key, key_versions) in kvs {
            index.insert(key.clone(), n_versions as u16);
            for (ts, value) in key_versions {
                let value_offset = block.len();
                let tombstone_bit = match value {
                    Value::Regular(value) => {
                        block.extend_from_slice(&value[..]);
                        0
                    }
                    Value::Tombstone => 1,
                };
                let ts_offset_and_tombstone =
                    ((ts.as_nanos() - min_ts.as_nanos()) << 1) | tombstone_bit;

                let mut buf = [0u8; 16];
                LittleEndian::write_u64(&mut buf[..], ts_offset_and_tombstone);
                LittleEndian::write_u64(&mut buf[bytes_per_ts_offset..], value_offset as u64);
                versions.extend_from_slice(&buf[..bytes_per_ts_offset + bytes_per_value_offset]);
            }
            n_versions += key_versions.len();
        }
        let values_len = block.len();
        let encoded_index = PrefixCompressedKV::encode(&index);

        let mut header = [0u8; BLOCK_INDEX_HEADER_SIZE];
        LittleEndian::write_u32(&mut header[0..4], values_len as u32);
        LittleEndian::write_u16(&mut header[4..6], n_versions as u16);
        LittleEndian::write_u64(&mut header[6..14], min_ts.as_nanos());
        LittleEndian::write_u16(&mut header[14..16], encoded_index.len() as u16);
        header[16] = bytes_per_ts_offset as u8;
        header[17] = bytes_per_value_offset as u8;

        let header_idx = block.len();
        block.extend_from_slice(&header[..]);
        block.extend_from_slice(&encoded_index[..]);
        block.extend_from_slice(&versions[..]);

        Ok((block, header_idx))
    }
}

impl<'a, R: AsyncReadExactAt> Block<'a, R> {
    pub(crate) async fn open(r: &'a R, header_offset: u64) -> anyhow::Result<Block<'a, R>> {
        let mut header = [0u8; BLOCK_INDEX_HEADER_SIZE];

        r.read_exact_at(&mut header[..], header_offset).await?;

        let values_len = LittleEndian::read_u32(&header[0..4]) as usize;
        let n_versions = LittleEndian::read_u16(&header[4..6]) as usize;
        let min_ts = Timestamp::from_nanos(LittleEndian::read_u64(&header[6..14]));
        let index_len = LittleEndian::read_u16(&header[14..16]) as usize;
        let bytes_per_ts_offset = header[16] as usize;
        let bytes_per_value_offset = header[17] as usize;
        let versions_len = n_versions * (bytes_per_ts_offset + bytes_per_value_offset);

        let mut index_bytes = vec![0u8; index_len];
        r.read_exact_at(
            &mut index_bytes[..],
            header_offset + (BLOCK_INDEX_HEADER_SIZE as u64),
        )
        .await?;
        let index = PrefixCompressedKV::decode(index_bytes);

        let mut versions_bytes = vec![0u8; versions_len];
        r.read_exact_at(
            &mut versions_bytes[..],
            header_offset + (BLOCK_INDEX_HEADER_SIZE as u64) + (index_len as u64),
        )
        .await?;

        Ok(Self {
            r,
            values_len,
            n_versions,
            min_ts,
            index,
            versions_bytes,
            header_offset,
            ts_bytes: bytes_per_ts_offset,
            offset_bytes: bytes_per_value_offset,
        })
    }

    pub(crate) async fn get(
        &self,
        ts: Timestamp,
        k: &[u8],
    ) -> anyhow::Result<Option<(Timestamp, Value)>> {
        let key_idx = match self.index.search(k) {
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
        let record_ts = key_versions.ts(version_idx);

        Ok(Some((
            record_ts,
            self.value(&key_versions, version_idx).await?,
        )))
    }

    pub(crate) fn scan(
        &self,
        ts: Timestamp,
        range: Range<Vec<u8>>,
        direction: Direction,
    ) -> impl Stream<Item = anyhow::Result<Record>> + '_ {
        try_stream! {
            let key_idxs = match direction {
                Direction::Asc => {
                    let lower_key_idx =
                        binary_search_by_idx(
                            self.index.len(),
                            KeyOrBound::Bound(range.lower.clone()),
                            |idx| KeyOrBound::Key(self.index.get_key(idx)),
                        )
                        .unwrap_or_else(core::convert::identity);
                    IteratorEither::Left(lower_key_idx..self.index.len())
                },
                Direction::Desc => {
                    let upper_key_idx = binary_search_by_idx(
                            self.index.len(),
                            KeyOrBound::Bound(range.upper.clone()),
                            |idx| KeyOrBound::Key(self.index.get_key(idx)),
                        )
                        .unwrap_or_else(core::convert::identity);

                    IteratorEither::Right((0..upper_key_idx).rev())
                },
            };

            for j in key_idxs {
                let key = self.index.get_key(j);
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

                yield Record { key, ts, value };
            }
        }
    }

    pub(crate) fn scan_desc(
        &self,
        ts: Timestamp,
        range: Range<Vec<u8>>,
    ) -> impl Stream<Item = anyhow::Result<Record>> + '_ {
        try_stream! {
            let upper_key_idx = binary_search_by_idx(
                    self.index.len(),
                    KeyOrBound::Bound(range.upper.clone()),
                    |idx| KeyOrBound::Key(self.index.get_key(idx)),
                )
                .unwrap_or_else(core::convert::identity);

            for j in (0..upper_key_idx).rev() {
                let key = self.index.get_key(j);
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

                yield Record { key, ts, value };
            }
        }
    }

    pub(crate) fn history<'b>(
        &'b self,
        k: &[u8],
        range: HistoryRange,
        direction: Direction,
    ) -> impl Stream<Item = anyhow::Result<Record>> + 'b {
        let k_owned = k.to_vec();
        try_stream! {
            let key_idx = match self.index.search(&k_owned) {
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
                let record_ts = key_versions.ts(idx);
                let value = self.value(&key_versions, idx).await?;

                assert!(min <= record_ts, "{:?} <= {:?}", min, record_ts);
                assert!(max >= record_ts, "{:?} >= {:?}", max, record_ts);

                yield Record {
                    key: k_owned.clone(),
                    ts: record_ts,
                    value,
                };
            }
        }
    }

    /// Produces all records contained in this block in Record's natural ordering: (key,
    /// Reverse(ts)).
    pub(crate) fn stream(&self) -> impl Stream<Item = anyhow::Result<Record>> + '_ {
        try_stream! {
            for j in 0..self.index.len() {
                let key = self.index.get_key(j);
                let versions = self.versions_for_key(j);
                for k in 0..versions.len() {
                    let ts = versions.ts(k);
                    let value = self.value(&versions, k).await?;
                    yield Record{key: key.clone(), ts, value};
                }
            }
        }
    }

    fn versions(&self) -> BlockVersions<'_> {
        BlockVersions {
            ts_bytes: self.ts_bytes,
            offset_bytes: self.offset_bytes,
            min_ts: self.min_ts,
            end_offset: self.values_len,
            b: &self.versions_bytes[..],
        }
    }

    fn versions_for_key(&self, key_idx: usize) -> BlockVersions<'_> {
        let start_idx = self.index.get_value(key_idx) as usize;
        let end_idx = if key_idx == self.index.len() - 1 {
            self.n_versions
        } else {
            self.index.get_value(key_idx + 1) as usize
        };
        self.versions().slice(start_idx, end_idx)
    }

    async fn value<'b>(
        &'b self,
        versions: &BlockVersions<'b>,
        idx: usize,
    ) -> anyhow::Result<Value> {
        let (value_start, value_end) = match versions.value_offsets(idx) {
            Some(v) => v,
            None => return Ok(Value::Tombstone),
        };
        let value_len = value_end - value_start;

        let mut value = vec![0u8; value_len];
        self.r
            .read_exact_at(
                &mut value[..],
                self.header_offset - (self.values_len as u64) + (value_start as u64),
            )
            .await?;

        Ok(Value::Regular(value))
    }
}

struct BlockVersions<'a> {
    ts_bytes: usize,
    offset_bytes: usize,
    min_ts: Timestamp,
    end_offset: usize,
    b: &'a [u8],
}

impl<'a> BlockVersions<'a> {
    pub(crate) fn len(&self) -> usize {
        self.b.len() / (self.ts_bytes + self.offset_bytes)
    }

    pub(crate) fn elem(&self, idx: usize) -> (Timestamp, bool, usize) {
        let width = self.ts_bytes + self.offset_bytes;
        let elem = &self.b[width * idx..width * (idx + 1)];
        let mut ts_offset_buf = [0u8; 8];
        ts_offset_buf[..self.ts_bytes].copy_from_slice(&elem[..self.ts_bytes]);
        let ts_offset_and_tombstone = LittleEndian::read_u64(&ts_offset_buf[..]);
        let tombstone = ts_offset_and_tombstone & 1 == 1;
        let ts = Timestamp::from_nanos((ts_offset_and_tombstone >> 1) + self.min_ts.as_nanos());

        let mut value_offset_buf = [0u8; 8];
        value_offset_buf[..self.offset_bytes].copy_from_slice(&elem[self.ts_bytes..]);
        let value_offset = LittleEndian::read_u64(&value_offset_buf[..]) as usize;
        (ts, tombstone, value_offset)
    }

    fn slice(&self, start_idx: usize, end_idx: usize) -> BlockVersions<'a> {
        let width = self.ts_bytes + self.offset_bytes;
        let b = &self.b[start_idx * width..end_idx * width];
        let end_offset = if end_idx == self.len() {
            self.end_offset
        } else {
            self.elem(end_idx).2
        };
        BlockVersions {
            ts_bytes: self.ts_bytes,
            offset_bytes: self.offset_bytes,
            min_ts: self.min_ts,
            end_offset,
            b,
        }
    }

    pub(crate) fn ts(&self, idx: usize) -> Timestamp {
        self.elem(idx).0
    }

    pub(crate) fn value_offsets(&self, idx: usize) -> Option<(usize, usize)> {
        let (_, tombstone, start) = self.elem(idx);
        if tombstone {
            return None;
        }
        let end = if idx == self.len() - 1 {
            self.end_offset
        } else {
            self.elem(idx + 1).2
        };
        Some((start, end))
    }
}

pub(crate) async fn dump_block<'a, R: AsyncReadExactAt>(
    block: &Block<'a, R>,
) -> anyhow::Result<()> {
    println!("    n_keys: {}", block.index.len());
    println!("    n_versions: {}", block.n_versions);
    println!("    values_len: {}", block.values_len);
    println!("      == keys ======");
    for i in 0..block.index.len() {
        let key = block.index.get_key(i);
        let versions_offset = block.index.get_value(i);
        println!("        [{}] {}", hexlify(&key), versions_offset);
    }
    let versions = block.versions();
    println!("      == versions ======");
    for i in 0..block.n_versions {
        let value_str = match versions.value_offsets(i) {
            Some((value_start, value_end)) => format!("({}, {})", value_start, value_end),
            None => "<TOMBSTONE>".into(),
        };

        println!(
            "        {} {} {} {:?}",
            i,
            versions.ts(i),
            value_str,
            block.value(&versions, i).await?,
        );
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use futures::TryStreamExt;

    use crate::range::Bound;
    use crate::range::Range;
    use crate::types::Direction;
    use crate::types::HistoryRange;
    use crate::types::Record;
    use crate::types::Timestamp;
    use crate::types::Value;
    use crate::util::AsyncReadExactAt;

    use super::Block;

    #[tokio::test]
    async fn test_get() -> anyhow::Result<()> {
        let aa: Vec<u8> = "aa".into();
        let ab: Vec<u8> = "ab".into();
        let aa_279: Value = Value::Regular("foo".into());
        let aa_265: Value = Value::Regular("bar".into());
        let ab_341: Value = Value::Regular("baz".into());
        let ab_302: Value = Value::Regular("qux".into());
        let ab_290: Value = Value::Regular("garply".into());
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
                    (Timestamp(297), Value::Tombstone),
                    (Timestamp(290), ab_290.clone()),
                ],
            );
            kvs
        };
        let (encoded, header_offset) = Block::<()>::encode(&kvs)?;

        let block = Block::open(&encoded, header_offset as u64).await?;

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
            Some((Timestamp(297), Value::Tombstone))
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
            Some((Timestamp(297), Value::Tombstone))
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
                    b'o' => Value::Regular(format!("{} {}", key_str, ts).into()),
                    b'x' => Value::Tombstone,
                    _ => continue,
                };

                versions.push((Timestamp(ts as u64), value));
            }
            versions.reverse();
            kvs.insert(key_str.into(), versions);
        }

        let (encoded, header_offset) = Block::<()>::encode(&kvs)?;
        let block = Block::open(&encoded, header_offset as u64).await?;

        async fn check<'a, R: AsyncReadExactAt>(
            block: &Block<'a, R>,
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
                        .map(|(key, ts, tombstone)| Record {
                            key: (key).into(),
                            ts: Timestamp(ts as u64),
                            value: match tombstone {
                                false => Value::Regular(format!("{} {}", key, ts).into()),
                                true => Value::Tombstone,
                            },
                        })
                        .collect::<Vec<Record>>(),
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
                    (Timestamp(5), Value::Regular(b"a five".to_vec())),
                    (Timestamp(2), Value::Regular(b"a two".to_vec())),
                ],
            ),
            (
                b"b".to_vec(),
                vec![
                    (Timestamp(9), Value::Regular(b"b nine".to_vec())),
                    (Timestamp(7), Value::Regular(b"b seven".to_vec())),
                    (Timestamp(4), Value::Tombstone),
                    (Timestamp(2), Value::Regular(b"b two".to_vec())),
                ],
            ),
            (
                b"c".to_vec(),
                vec![(Timestamp(3), Value::Regular(b"c three".to_vec()))],
            ),
        ]
        .into_iter()
        .collect();
        let (encoded, header_offset) = Block::<()>::encode(&kvs)?;
        let block = Block::open(&encoded, header_offset as u64).await?;

        assert_eq!(
            block
                .history(
                    b"b",
                    HistoryRange::Between(Timestamp(4), Timestamp(7)),
                    Direction::Asc,
                )
                .try_collect::<Vec<Record>>()
                .await?,
            vec![
                Record {
                    key: b"b".to_vec(),
                    ts: Timestamp(4),
                    value: Value::Tombstone
                },
                Record {
                    key: b"b".to_vec(),
                    ts: Timestamp(7),
                    value: Value::Regular(b"b seven".to_vec())
                },
            ],
        );

        assert_eq!(
            block
                .history(
                    b"b",
                    HistoryRange::Between(Timestamp(3), Timestamp(8)),
                    Direction::Asc,
                )
                .try_collect::<Vec<Record>>()
                .await?,
            vec![
                Record {
                    key: b"b".to_vec(),
                    ts: Timestamp(4),
                    value: Value::Tombstone
                },
                Record {
                    key: b"b".to_vec(),
                    ts: Timestamp(7),
                    value: Value::Regular(b"b seven".to_vec())
                },
            ],
        );

        Ok(())
    }
}
