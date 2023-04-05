use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use anyhow::anyhow;
use byteorder::BigEndian;
use byteorder::ByteOrder;
use byteorder::LittleEndian;
use futures::pin_mut;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use futures::TryStreamExt;
use priority_queue::PriorityQueue;
use rand::thread_rng;
use rand::Rng;

use crate::obsidian::Obsidian;
use crate::obsidian::ObsidianExt;
use crate::range::Range;
use crate::types::ColoGroupId;
use crate::types::Direction;
use crate::types::KeyspaceId;
use crate::types::Mutation;
use crate::types::Precondition;
use crate::types::Timestamp;
use crate::util::encode;
use crate::util::merge_sorted;
use crate::util::Decode;
use crate::util::Encode;
use crate::util::OrdEqByFirst;

struct WorkloadAppend<O> {
    obsidian: O,

    list_keyspace_id: KeyspaceId,
    list_item_keyspace_id: KeyspaceId,

    seq_gen: AtomicUsize,
    txid_gen: AtomicUsize,
}

impl<O: Obsidian + Sync + Send> WorkloadAppend<O> {
    fn new(obsidian: O) -> Self {
        Self {
            obsidian,

            list_keyspace_id: KeyspaceId(ColoGroupId(1), 1),
            list_item_keyspace_id: KeyspaceId(ColoGroupId(1), 2),
            seq_gen: AtomicUsize::new(0),
            txid_gen: AtomicUsize::new(0),
        }
    }

    async fn run(&self) -> anyhow::Result<()> {
        let mut futures = FuturesUnordered::new();
        let start = Instant::now();
        for _ in 0..32 {
            futures.push(self.thread());
        }
        let mut histories = vec![];
        while let Some(thread_history) = futures.next().await {
            histories.push(thread_history);
            if start.elapsed() < Duration::from_millis(4_800) {
                futures.push(self.thread());
            }
        }

        println!("ran {} threads", histories.len());
        println!(
            "history has {} events",
            histories.iter().map(Vec::len).sum::<usize>(),
        );

        let edges = gen_graph(histories)?;

        println!("graph has {} edges", edges.len());

        find_cycle(edges)?;
        Ok(())
    }

    async fn thread(&self) -> Vec<(Seq, HistoryItem)> {
        let mut history = vec![];
        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(5_000) {
            let txid = self.new_txid();

            let choice = thread_rng().gen_bool(0.1);
            match choice {
                true => {
                    let list_id = self.choose_list();
                    history.push((self.next_seq(), HistoryItem::StartAppend(txid, list_id)));
                    match self.append(txid, list_id).await {
                        Ok(ts) => {
                            history.push((self.next_seq(), HistoryItem::Commit(txid, ts, list_id)));
                        }
                        // TODO: classify some errors as aborts and continue instead of ending
                        Err(e) => {
                            println!("write transaction failed {:?}", e);
                            return history;
                        }
                    };
                }
                false => {
                    let list_id = self.choose_list();
                    history.push((self.next_seq(), HistoryItem::StartRead(txid)));
                    match self.read(list_id).await {
                        Ok((ts, list)) => {
                            history.push((
                                self.next_seq(),
                                HistoryItem::FinishRead(txid, ts, list_id, list),
                            ));
                        }
                        Err(e) => {
                            println!("read failed {:?}", e);
                            history.push((self.next_seq(), HistoryItem::Abort(txid)));
                        }
                    }
                }
            };
        }
        history
    }

    async fn append(&self, txid: Txid, list_id: ListId) -> anyhow::Result<Timestamp> {
        let read_ts = self
            .obsidian
            .latest_snapshot(BTreeSet::from([(self.list_keyspace_id, list_id.to_key())]))
            .await?;

        let list_value = self
            .obsidian
            .get(read_ts, self.list_keyspace_id, list_id.to_key())
            .await?
            .unwrap_or(vec![0u8; 8]);
        let new_len = BigEndian::read_u64(&list_value[..]) + 1;
        let mut new_len_value = vec![0u8; 8];
        BigEndian::write_u64(&mut new_len_value, new_len);

        let list_item = ListItem(list_id, new_len);

        let txid_value = encode(&txid);

        let ts = self
            .obsidian
            .write(
                vec![Precondition::NotChangedSince(
                    self.list_keyspace_id,
                    list_id.to_key(),
                    read_ts,
                )],
                BTreeMap::from([
                    (
                        (self.list_keyspace_id, list_id.to_key()),
                        Mutation::Put(new_len_value),
                    ),
                    (
                        (self.list_item_keyspace_id, list_item.to_key()),
                        Mutation::Put(txid_value),
                    ),
                ]),
            )
            .await?;

        Ok(ts)
    }

    async fn read(&self, list_id: ListId) -> anyhow::Result<(Timestamp, Vec<Txid>)> {
        let read_ts = self
            .obsidian
            .latest_snapshot(BTreeSet::from([(self.list_keyspace_id, list_id.to_key())]))
            .await?;

        let s = Box::into_pin(self.obsidian.scan(
            read_ts,
            self.list_item_keyspace_id,
            Range::prefix(list_id.to_key()),
            Direction::Asc,
        ));
        pin_mut!(s);

        let mut result = vec![];
        while let Some((_, _, value)) = s.try_next().await? {
            let observed_txid = Txid::decode(&value)?;
            result.push(observed_txid);
        }

        Ok((read_ts, result))
    }

    fn choose_list(&self) -> ListId {
        ListId(thread_rng().gen_range(0..100))
    }

    fn new_txid(&self) -> Txid {
        Txid(self.seq_gen.fetch_add(1, Ordering::SeqCst) as u64)
    }

    fn next_seq(&self) -> Seq {
        Seq(self.seq_gen.fetch_add(1, Ordering::SeqCst) as u64)
    }
}

fn gen_graph(
    histories: Vec<Vec<(Seq, HistoryItem)>>,
) -> anyhow::Result<HashMap<Txid, HashMap<Txid, EdgeType>>> {
    let mut edges = HashMap::new();

    let mut longests = HashMap::new();
    let mut possible_txids = HashSet::new();

    for history in &histories {
        for (_, item) in history {
            match item {
                HistoryItem::StartAppend(txid, _) => {
                    possible_txids.insert(txid);
                }
                HistoryItem::Abort(txid) => {
                    possible_txids.remove(&txid);
                }
                HistoryItem::FinishRead(_, _, list_id, txids) => {
                    if txids.len() > longests.get(&list_id).map(Vec::len).unwrap_or(0) {
                        longests.insert(list_id, txids.clone());
                    }
                }
                _ => {}
            }
        }
    }

    for longest in longests.values() {
        let mut prev_txid: Option<Txid> = None;
        for txid in longest {
            if let Some(prev_txid) = prev_txid {
                edges
                    .entry(*txid)
                    .or_insert_with(HashMap::new)
                    .insert(prev_txid, EdgeType::WriteWrite);
            }

            if !possible_txids.contains(txid) {
                return Err(anyhow!("garbage read"));
            }

            prev_txid = Some(*txid);
        }
    }

    let histories_with_thread_ids = histories
        .iter()
        .enumerate()
        .map(|(thread_id, history)| {
            history
                .iter()
                .map(move |(seq, item)| OrdEqByFirst(seq, (thread_id, item)))
        })
        .collect();
    let merged_history = merge_sorted(histories_with_thread_ids);

    let mut most_recent_txid = HashMap::new();
    let mut highest_timestamp: HashMap<ListId, (Timestamp, Txid)> = HashMap::new();
    for OrdEqByFirst(_, (thread_id, item)) in merged_history {
        match item {
            HistoryItem::StartRead(txid) | HistoryItem::StartAppend(txid, _) => {
                for other_txid in most_recent_txid.values() {
                    edges
                        .entry(*txid)
                        .or_insert_with(HashMap::new)
                        .insert(*other_txid, EdgeType::RealTime);
                }
            }
            HistoryItem::Commit(txid, ts, list_id) => {
                if let Some((other_ts, other_txid)) = highest_timestamp.get(list_id) {
                    if ts > other_ts {
                        edges
                            .entry(*txid)
                            .or_insert_with(HashMap::new)
                            .insert(*other_txid, EdgeType::SameKeyTimestamp);
                        highest_timestamp.insert(*list_id, (*ts, *txid));
                    }
                } else {
                    highest_timestamp.insert(*list_id, (*ts, *txid));
                }
            }
            HistoryItem::FinishRead(txid, _, list_id, txids) => {
                if let Some(last_txid) = txids.last() {
                    edges
                        .entry(*txid)
                        .or_insert_with(HashMap::new)
                        .insert(*last_txid, EdgeType::WriteRead);
                }

                let longest = longests.get(&list_id).unwrap();
                if !longest.starts_with(&txids) {
                    return Err(anyhow!("lost or duplicate write?"));
                }

                if longest.len() >= txids.len() {
                    edges
                        .entry(*txid)
                        .or_insert_with(HashMap::new)
                        .insert(longest[txids.len()], EdgeType::ReadWrite);
                }
            }
            _ => {}
        }

        match item {
            HistoryItem::FinishRead(txid, _, _, _)
            | HistoryItem::Commit(txid, _, _)
            | HistoryItem::Abort(txid) => {
                most_recent_txid.insert(thread_id, *txid);
            }
            _ => {}
        }
    }

    Ok(edges)
}

fn find_cycle(edges: HashMap<Txid, HashMap<Txid, EdgeType>>) -> anyhow::Result<()> {
    let mut in_edges = HashMap::new();

    for (_, dsts) in &edges {
        for (dst, _) in dsts {
            in_edges.insert(*dst, in_edges.get(&dst).unwrap_or(&0) + 1usize);
        }
    }

    let mut pq: PriorityQueue<Txid, Reverse<usize>> = PriorityQueue::from(
        in_edges
            .into_iter()
            .map(|(txid, n_in_edges)| (txid, Reverse(n_in_edges)))
            .collect::<Vec<(Txid, Reverse<usize>)>>(),
    );

    while let Some((txid, remaining_in_edges)) = pq.pop() {
        if remaining_in_edges.0 > 0 {
            return Err(anyhow!("cycle detected"));
        }

        for (dst, _) in edges.get(&txid).unwrap() {
            pq.change_priority_by(dst, |p| p.0 = p.0 - 1);
        }
    }

    Ok(())
}

#[derive(Clone)]
enum HistoryItem {
    StartRead(Txid),
    FinishRead(Txid, Timestamp, ListId, Vec<Txid>),
    StartAppend(Txid, ListId),
    Abort(Txid),
    Commit(Txid, Timestamp, ListId),
}

// Dependency edges between two transactions T1 and T2.
enum EdgeType {
    // T1 finished before T2 started.
    RealTime,
    // T1 executed at some timestamp and T2 executed at a higher timestamp on the same key.
    SameKeyTimestamp,
    // T1 wrote a version and T2 wrote the next version.
    WriteWrite,
    // T1 wrote a version and T2 read that version.
    WriteRead,
    // T1 read a version and T2 installed a later version.
    ReadWrite,
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy)]
struct ListId(u64);
impl ListId {
    fn to_key(&self) -> Vec<u8> {
        let mut key = vec![0u8; 8];
        LittleEndian::write_u64(&mut key, self.0);
        key
    }
}

struct ListItem(ListId, u64);
impl ListItem {
    fn to_key(&self) -> Vec<u8> {
        let mut key = vec![0u8; 16];
        LittleEndian::write_u64(&mut key, self.0 .0 + 1000);
        BigEndian::write_u64(&mut key, self.1);
        key
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
struct Txid(u64);

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Copy)]
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

#[cfg(test)]
mod tests {
    use super::WorkloadAppend;

    #[tokio::test]
    async fn test_workload_append() -> anyhow::Result<()> {
        let fe = crate::test::new_with_single_byte_routing(2).await?;

        let wl = WorkloadAppend::new(fe);

        wl.run().await?;

        Ok(())
    }
}
