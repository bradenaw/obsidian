use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
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
        let workload_start = Instant::now();
        let workload_deadline = workload_start + Duration::from_millis(15_000);
        for _ in 0..64 {
            futures.push(self.thread(workload_deadline));
        }
        let mut histories = vec![];
        while let Some(thread_history) = futures.next().await {
            histories.push(thread_history);
            if Instant::now() < workload_deadline {
                futures.push(self.thread(workload_deadline));
            }
        }

        println!("workload took {:?}", workload_start.elapsed());
        println!("ran {} threads", histories.len());
        println!(
            "history has {} events",
            histories.iter().map(Vec::len).sum::<usize>(),
        );

        let gen_graph_start = Instant::now();
        let edges = gen_graph(histories)?;

        println!("graph has {} edges", edges.len());
        println!("gen_graph took {:?}", gen_graph_start.elapsed());

        let find_cycle_start = Instant::now();
        let maybe_cycle = find_cycle(&edges);
        println!("find_cycle took {:?}", find_cycle_start.elapsed());
        if let Some(_) = maybe_cycle {
            return Err(anyhow!("cycle found"));
        }

        Ok(())
    }

    async fn thread(&self, deadline: Instant) -> Vec<(Seq, HistoryItem)> {
        let mut history = vec![];
        while Instant::now() < deadline {
            let txid = self.new_txid();

            let choice = thread_rng().gen_bool(0.1);
            match choice {
                true => {
                    let list_id = self.choose_list();
                    history.push((self.next_seq(), HistoryItem::StartAppend(txid, list_id)));
                    match tokio::time::timeout_at(
                        tokio::time::Instant::from_std(deadline),
                        self.append(txid, list_id),
                    )
                    .await
                    {
                        Ok(Ok(ts)) => {
                            history.push((self.next_seq(), HistoryItem::Commit(txid, ts, list_id)));
                        }
                        // TODO: classify some errors as aborts and continue instead of ending
                        Ok(Err(e)) => {
                            println!("write transaction failed {:?}", e);
                            return history;
                        }
                        Err(_) => {
                            return history;
                        }
                    };
                }
                false => {
                    let list_id = self.choose_list();
                    history.push((self.next_seq(), HistoryItem::StartRead(txid)));
                    match tokio::time::timeout_at(
                        tokio::time::Instant::from_std(deadline),
                        self.read(list_id),
                    )
                    .await
                    {
                        Ok(Ok((ts, list))) => {
                            history.push((
                                self.next_seq(),
                                HistoryItem::FinishRead(txid, ts, list_id, list),
                            ));
                        }
                        Ok(Err(e)) => {
                            println!("read failed {:?}", e);
                            history.push((self.next_seq(), HistoryItem::Abort(txid)));
                        }
                        Err(_) => {
                            return history;
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
                    if txids.len() >= longests.get(&list_id).map(Vec::len).unwrap_or(0) {
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

                if !longest.is_empty() && longest.len() >= txids.len() {
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

enum TxResult {
    Read(Timestamp, ListId, Vec<Txid>),
    Append(Txid, ListId, WriteResult),
}

enum WriteResult {
    Commit(Timestamp),
    Abort,
    Unknown,
}

fn find_results(
    txids: &HashSet<Txid>,
    histories: &Vec<Vec<(Seq, HistoryItem)>>,
) -> HashMap<Txid, TxResult> {
    let mut result = HashMap::new();
    for history in histories {
        for (i, (_, item)) in history.iter().enumerate() {
            match item {
                HistoryItem::FinishRead(txid, ts, list_id, list_items) => {
                    if !txids.contains(txid) {
                        continue;
                    }
                    result.insert(*txid, TxResult::Read(*ts, *list_id, list_items.clone()));
                }
                HistoryItem::StartAppend(txid, list_id) => {
                    if !txids.contains(txid) {
                        continue;
                    }
                    if i == history.len() - 1 {
                        result.insert(
                            *txid,
                            TxResult::Append(*txid, *list_id, WriteResult::Unknown),
                        );
                    } else {
                        let write_result = match history[i + 1].1 {
                            HistoryItem::Commit(_, ts, _) => WriteResult::Commit(ts),
                            HistoryItem::Abort(_) => WriteResult::Abort,
                            _ => panic!("write not followed by commit/abort"),
                        };
                        result.insert(*txid, TxResult::Append(*txid, *list_id, write_result));
                    }
                }
                _ => {}
            }
        }
    }
    result
}

fn find_cycle(edges: &HashMap<Txid, HashMap<Txid, EdgeType>>) -> Option<Vec<(Txid, EdgeType)>> {
    let sccs = strongly_connected_components(edges);
    let smallest_scc = sccs.iter().min_by_key(|scc| scc.len())?;

    let cycle_txids = small_cycle(smallest_scc, edges);

    let mut result = vec![];
    for i in 0..cycle_txids.len() - 1 {
        let a = cycle_txids[i];
        let b = cycle_txids[i + 1];

        let edge_type = *(edges.get(&a).unwrap().get(&b).unwrap());

        result.push((a, edge_type));
    }
    return Some(result);
}

fn strongly_connected_components(
    edges: &HashMap<Txid, HashMap<Txid, EdgeType>>,
) -> Vec<HashSet<Txid>> {
    // This is Tarjan's algorithm for finding strongly connected components, which is O(V+E).

    let mut low_links: HashMap<Txid, Txid> = HashMap::new();
    let mut stack = vec![];
    let mut set = HashSet::new();

    fn visit(
        txid: Txid,
        edges: &HashMap<Txid, HashMap<Txid, EdgeType>>,
        stack: &mut Vec<Txid>,
        set: &mut HashSet<Txid>,
        low_links: &mut HashMap<Txid, Txid>,
    ) {
        low_links.insert(txid, txid);
        stack.push(txid);
        set.insert(txid);

        if let Some(out_edges) = edges.get(&txid) {
            for out in out_edges.keys() {
                if low_links.contains_key(out) {
                    continue;
                }

                visit(*out, edges, stack, set, low_links);

                if set.contains(out) && low_links.get(out) < low_links.get(&txid) {
                    low_links.insert(txid, *(low_links.get(out).unwrap()));
                }
            }
        }

        if low_links.get(&txid) == Some(&txid) {
            while let Some(other_txid) = stack.pop() {
                set.remove(&other_txid);
                if txid == other_txid {
                    break;
                }
            }
        }
    }

    if let Some(txid) = edges.keys().next() {
        visit(*txid, edges, &mut stack, &mut set, &mut low_links);
    }

    let mut sccs = HashMap::new();
    for (txid, low_link) in low_links {
        sccs.entry(low_link)
            .or_insert_with(HashSet::new)
            .insert(txid);
    }

    let mut result = vec![];
    for (_, scc) in sccs.into_iter() {
        if scc.len() > 1 {
            result.push(scc);
        }
    }
    result
}

fn small_cycle(
    component: &HashSet<Txid>,
    edges: &HashMap<Txid, HashMap<Txid, EdgeType>>,
) -> Vec<Txid> {
    // Keys are each vertex visited.
    // Values are the previous vertex.
    let mut visited = HashMap::new();

    // This is a breadth-first-search to find the shortest cycle we can.
    //
    // component is already a strongly-connected-component discovered by Tarjan's algorithm, which
    // means that it's both guaranteed to contain a cycle and every vertex is reachable from every
    // other, so starting from any random vertex will work.
    let mut queue = VecDeque::new();
    if let Some(txid) = component.iter().next() {
        queue.push_back(*txid);
    }

    while let Some(txid) = queue.pop_front() {
        for other_txid in edges.get(&txid).unwrap().keys() {
            if visited.contains_key(other_txid) {
                let mut result = vec![];
                let mut curr = *other_txid;
                loop {
                    result.push(curr);
                    curr = *(visited.get(&curr).unwrap());
                    if curr == *other_txid {
                        break;
                    }
                }
                result.reverse();
                return result;
            }
            visited.insert(txid, *other_txid);
            queue.push_back(*other_txid);
        }
    }
    return vec![];
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
#[derive(Clone, Copy)]
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

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
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
