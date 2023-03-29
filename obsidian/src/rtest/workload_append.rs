use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;

use anyhow::anyhow;
use byteorder::BigEndian;
use byteorder::ByteOrder;
use futures::pin_mut;
use futures::TryStreamExt;
use rand::thread_rng;
use rand::Rng;

use crate::obsidian::Obsidian;
use crate::obsidian::ObsidianExt;
use crate::range::Range;
use crate::types::Direction;
use crate::types::KeyspaceId;
use crate::types::Mutation;
use crate::types::Precondition;
use crate::types::Timestamp;
use crate::util::encode;
use crate::util::Decode;
use crate::util::Encode;

struct WorkloadAppend<O> {
    obsidian: O,

    edges: HashMap<Txid, Txid>,
}

impl<O: Obsidian + Sync> WorkloadAppend<O> {
    async fn thread(&mut self) -> anyhow::Result<()> {
        let mut maybe_prev_txid = None;
        loop {
            let txid = self.new_txid();

            if let Some(prev_txid) = maybe_prev_txid {
                // sequential dependency
                self.edges.insert(txid, prev_txid);
            }

            match thread_rng().gen_bool(0.1) {
                true => self.write(txid).await?,
                false => self.read(txid).await?,
            };

            maybe_prev_txid = Some(txid);
        }
    }

    async fn write(&self, txid: Txid) -> anyhow::Result<()> {
        let list_id = self.choose_list();
        let (list_keyspace_id, list_key) = list_id.to_key();

        let read_ts = self
            .obsidian
            .latest_snapshot(BTreeSet::from([(list_keyspace_id, list_key.clone())]))
            .await?;

        let list_value = self
            .obsidian
            .get(read_ts, list_keyspace_id, list_key.clone())
            .await?
            .unwrap_or(vec![0u8; 8]);
        let new_len = BigEndian::read_u64(&list_value[..]) + 1;
        let mut new_len_value = vec![0u8; 8];
        BigEndian::write_u64(&mut new_len_value, new_len);

        let list_item = ListItem(list_id, new_len);
        let (list_item_keyspace_id, list_item_key) = list_item.to_key();

        let txid_value = encode(&txid);

        self.obsidian
            .write(
                vec![Precondition::NotChangedSince(
                    list_keyspace_id,
                    list_key.clone(),
                    read_ts,
                )],
                BTreeMap::from([
                    ((list_keyspace_id, list_key), Mutation::Put(new_len_value)),
                    (
                        (list_item_keyspace_id, list_item_key),
                        Mutation::Put(txid_value),
                    ),
                ]),
            )
            .await?;

        Ok(())
    }

    async fn read(&self, txid: Txid) -> anyhow::Result<()> {
        let list_id = self.choose_list();
        let list_item_keyspace_id = todo!();

        let read_ts = self
            .obsidian
            .latest_snapshot(BTreeSet::from([list_id.to_key().clone()]))
            .await?;

        let s = Box::into_pin(self.obsidian.scan(
            read_ts,
            list_item_keyspace_id,
            Range::prefix(list_id.to_key().1),
            Direction::Asc,
        ));
        pin_mut!(s);

        let mut maybe_prev_txid = None;
        while let Some((_, _, value)) = s.try_next().await? {
            let observed_txid = Txid::decode(&value)?;

            if let Some(prev_txid) = maybe_prev_txid {
                // ww dependency
                self.edges.insert(observed_txid, prev_txid);
            }
            maybe_prev_txid = Some(observed_txid);
        }
        if let Some(prev_txid) = maybe_prev_txid {
            // wr dependency
            self.edges.insert(txid, prev_txid);
        }

        Ok(())
    }

    fn new_txid(&self) -> Txid {
        todo!()
    }

    fn choose_list(&self) -> ListId {
        todo!()
    }

    fn new_list_item(&self, _list_id: ListId) -> ListItem {
        todo!();
    }
}

fn analyze(histories: Vec<Vec<(Seq, HistoryItem)>>) {
    let mut edges = HashMap::new();

    let mut longests = HashMap::new();
    for history in histories {
        let mut maybe_prev_txid = None;

        for (_, item) in history {
            match item {
                HistoryItem::StartRead(txid) | HistoryItem::StartAppend(txid, _, _) => {
                    if let Some(prev_txid) = maybe_prev_txid {
                        edges.insert(txid, (prev_txid, EdgeType::Sequential));
                    }
                    maybe_prev_txid = Some(txid);
                }
                HistoryItem::FinishRead(_, _, list_id, txids) => {
                    if txids.len() > longests.get(&list_id).map(Vec::len).unwrap_or(0) {
                        longests.insert(list_id, txids);
                    }
                }
                _ => {}
            }
        }
    }
}

enum HistoryItem {
    StartRead(Txid),
    FinishRead(Txid, Timestamp, ListId, Vec<Txid>),
    StartAppend(Txid, Timestamp, ListId),
    Abort,
    Commit(Timestamp),
}

// Dependency edges between two transactions T1 and T2.
enum EdgeType {
    // T1 finished before T2 started in the same thread.
    Sequential,
    // T1 finished before T2 started.
    Temporal,
    // T1 executed at some timestamp and T2 executed at a higher timestamp.
    Timestamp,
    // T1 wrote a version and T2 wrote the next version.
    WriteWrite,
    // T1 wrote a version and T2 read that version.
    WriteRead,
    // T1 read a version and T2 installed a later version.
    ReadWrite,
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
struct ListId(u64);
impl ListId {
    fn to_key(&self) -> (KeyspaceId, Vec<u8>) {
        todo!();
    }
}

struct ListItem(ListId, u64);
impl ListItem {
    fn to_key(&self) -> (KeyspaceId, Vec<u8>) {
        todo!();
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
struct Txid(u64);

struct Seq(u64);

impl Encode for Txid {
    fn encoded_size_estimate(&self) -> usize {
        return 8;
    }

    fn encode(&self, w: &mut Vec<u8>) {
        let mut b = [0u8; 8];
        BigEndian::write_u64(&mut b[..], self.0);
        w.extend_from_slice(&b);
    }
}

impl Decode for Txid {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() != 8 {
            return Err(anyhow!("wrong length {}, expected 8", b.len()));
        }
        Ok(Self(BigEndian::read_u64(&b[..8])))
    }
}
