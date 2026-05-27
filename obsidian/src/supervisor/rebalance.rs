use std::cmp::Reverse;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;

use obsidian_common::ColoGroupId;
use priority_queue::DoublePriorityQueue;
use priority_queue::PriorityQueue;

use crate::Range;
use crate::ShardId;
use crate::TabletId;

#[derive(Debug)]
enum TransferPlan {
    Merge(TabletId, TabletId, ShardId),
    Split(TabletId, ShardId, ShardId),
    Move(TabletId, ShardId),
}

const RANGE_TARGET_SIZE: u64 = 5_000_000_000;
const RANGE_MERGE_SIZE: u64 = RANGE_TARGET_SIZE / 2;
const RANGE_SPLIT_SIZE: u64 = RANGE_TARGET_SIZE * 2;
// Only bother merging ranges if there are more than this many per shard for a given colo group.
const MIN_RANGES_PER_SHARD: usize = 8;
// Only bother merging ranges if there are more than this many for a given colo group.
// Used to prevent e.g. immediately merging together the ranges made in create_colo_group().
const MIN_RANGES: usize = 1024;

const SHARD_CAPACITY: u64 = 1_000_000_000_000;
// Only bother moving ranges off of a shard if its utilization is above this amount.
const MIN_SHARD_SIZE_FOR_MOVE: u64 = SHARD_CAPACITY * 7 / 10;

/// Plan transfers to rebalance the system.
///
/// The main goal is to keep the shards roughly balanced in size, which we do by moving ranges from
/// the fullest shards to the emptiest.
///
/// Secondarily, we want to keep the ranges roughly the same size. This makes the above more
/// straightforward (we can just move any range since they're all similar sized, rather than doing
/// complicated bin-packing), and inexpensive (moves scale in the size of the range). In addition,
/// keeping splitting ranges to prevent them from becoming too large reduces contention of
/// tablet-sized resources like the sequencers. Keeping ranges from becoming too small reduces the
/// size of the routing table that every node needs to hold.
fn plan_rebalance(
    active_tablets: HashMap<TabletId, (ColoGroupId, Range<Vec<u8>>, u64)>,
    shard_sizes: HashMap<ShardId, u64>,
    mut in_progress_shards: HashSet<ShardId>,
) -> Vec<TransferPlan> {
    let mut plan = Vec::new();

    let n_shards = shard_sizes.len();
    let mut eligible_shards_by_size: DoublePriorityQueue<_, _> = shard_sizes.into_iter().collect();
    for shard_id in &in_progress_shards {
        eligible_shards_by_size.remove(shard_id);
    }

    let split_candidates = {
        let mut split_candidates = PriorityQueue::new();
        for (tablet_id, (_, _, size)) in &active_tablets {
            if in_progress_shards.contains(&tablet_id.0) {
                continue;
            }

            if *size < RANGE_SPLIT_SIZE {
                continue;
            }

            split_candidates.push(*tablet_id, *size);
        }
        split_candidates
    };
    for (tablet_id, _) in split_candidates.into_iter() {
        // Prefer to split in-place because it'll be almost free - the data is already in local
        // cache. If there's still imbalance after it's finished we can move one.
        in_progress_shards.insert(tablet_id.0);
        eligible_shards_by_size.remove(&tablet_id.0);
        plan.push(TransferPlan::Split(tablet_id, tablet_id.0, tablet_id.0));
    }

    let tablets_per_colo_group = count_by(
        active_tablets
            .iter()
            .map(|(_, (colo_group_id, _, _))| colo_group_id),
    );
    let mergeable_colo_groups: HashSet<_> = tablets_per_colo_group
        .iter()
        .filter(|(_, n_tablets)| {
            **n_tablets > n_shards * MIN_RANGES_PER_SHARD && **n_tablets > MIN_RANGES
        })
        .map(|(colo_group_id, _)| *colo_group_id)
        .collect();

    // Prioritize merging the smallest slices possible by putting candidates in a priority queue by
    // size.
    let merge_candidates = {
        let mut merge_candidates = PriorityQueue::new();
        for (tablet_id, (colo_group_id, _, size)) in &active_tablets {
            if in_progress_shards.contains(&tablet_id.0) {
                continue;
            }
            // We only bother to merge if two adjacent tablets are less than RANGE_MERGE_SIZE,
            // which implies that at least one of them is less than half of that.
            if *size > RANGE_MERGE_SIZE / 2 {
                continue;
            }
            if !mergeable_colo_groups.contains(&colo_group_id) {
                continue;
            }
            merge_candidates.push(*tablet_id, Reverse(*size));
        }
        merge_candidates
    };
    let mut tablet_ids_by_lower = HashMap::new();
    let mut tablet_ids_by_upper = HashMap::new();
    for (tablet_id, (colo_group_id, range, _)) in &active_tablets {
        tablet_ids_by_lower.insert((*colo_group_id, &range.lower), *tablet_id);
        tablet_ids_by_upper.insert((*colo_group_id, &range.upper), *tablet_id);
    }

    for (tablet_id, _) in merge_candidates.into_iter() {
        let (colo_group_id, range, size) = active_tablets.get(&tablet_id).unwrap();
        if in_progress_shards.contains(&tablet_id.0) {
            continue;
        }

        let adjacent_tablet_id = match (
            tablet_ids_by_upper.get(&(*colo_group_id, &range.lower)),
            tablet_ids_by_lower.get(&(*colo_group_id, &range.upper)),
        ) {
            (Some(prev_tablet_id), Some(next_tablet_id)) => {
                let prev_tablet_size = active_tablets.get(prev_tablet_id).unwrap().2;
                let next_tablet_size = active_tablets.get(next_tablet_id).unwrap().2;

                if prev_tablet_size < next_tablet_size {
                    *prev_tablet_id
                } else {
                    *next_tablet_id
                }
            }
            (Some(prev_tablet_id), None) => *prev_tablet_id,
            (None, Some(next_tablet_id)) => *next_tablet_id,
            (None, None) => continue,
        };

        if in_progress_shards.contains(&adjacent_tablet_id.0) {
            continue;
        }

        let adjacent_tablet_size = active_tablets.get(&adjacent_tablet_id).unwrap().2;

        if size + adjacent_tablet_size >= RANGE_MERGE_SIZE {
            continue;
        }

        let shard_id = if let Some((shard_id, _)) = eligible_shards_by_size.pop_min() {
            shard_id
        } else {
            break;
        };

        in_progress_shards.insert(tablet_id.0);
        in_progress_shards.insert(adjacent_tablet_id.0);
        in_progress_shards.insert(shard_id);
        plan.push(TransferPlan::Merge(tablet_id, adjacent_tablet_id, shard_id));
    }

    // For moves, we're going to do a series of moving any tablet from the largest eligible shard to
    // the smallest eligible shard.
    //
    // For that, we need to get any tablet on the largest shard. Because we never have more than
    // one transfer per shard in flight at once, we only need to have one tablet ID handy, and it
    // doesn't matter which one.
    let tablet_by_shard = {
        let mut tablet_by_shard: HashMap<ShardId, TabletId> = HashMap::new();
        for tablet_id in active_tablets.keys() {
            tablet_by_shard.insert(tablet_id.0, *tablet_id);
        }
        tablet_by_shard
    };

    loop {
        let (min_shard_id, min_shard_size, max_shard_id, max_shard_size) = match (
            eligible_shards_by_size.peek_min(),
            eligible_shards_by_size.peek_max(),
        ) {
            (Some((min_shard_id, min_shard_size)), Some((max_shard_id, max_shard_size))) => (
                *min_shard_id,
                *min_shard_size,
                *max_shard_id,
                *max_shard_size,
            ),
            _ => {
                break;
            }
        };

        if max_shard_size < MIN_SHARD_SIZE_FOR_MOVE {
            break;
        }

        // Only bother moving if there's enough imbalance that it'll matter, otherwise it's
        // just churn for no reason.
        if max_shard_size - min_shard_size < 2 * RANGE_TARGET_SIZE {
            break;
        }

        let tablet_id = if let Some(tablet_id) = tablet_by_shard.get(&max_shard_id) {
            tablet_id
        } else {
            break;
        };

        eligible_shards_by_size.pop_min();
        eligible_shards_by_size.pop_max();
        in_progress_shards.insert(min_shard_id);
        in_progress_shards.insert(max_shard_id);
        plan.push(TransferPlan::Move(*tablet_id, min_shard_id));
    }

    plan
}

fn count_by<I, K>(iter: I) -> HashMap<K, usize>
where
    I: Iterator<Item = K>,
    K: Eq + Hash,
{
    let mut counts = HashMap::new();
    for key in iter {
        *counts.entry(key).or_default() += 1;
    }
    counts
}

#[cfg(test)]
mod tests {
    use std::cmp::max;
    use std::cmp::min;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::hash::Hash;
    use std::time::Instant;

    use obsidian_common::Bound;
    use obsidian_common::ColoGroupId;
    use obsidian_common::Range;
    use obsidian_common::ShardId;
    use obsidian_common::TabletId;
    use obsidian_util::shortest_between;
    use rand::seq::IndexedRandom as _;
    use rand_distr::Distribution as _;
    use rand_distr::Zipf;

    use super::plan_rebalance;
    use super::TransferPlan;
    use super::SHARD_CAPACITY;

    struct Tablet {
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
        size: u64,
        active: bool,
    }

    #[test]
    #[ignore]
    fn test_plan_rebalance_sim() {
        let mut shards = Shards::new();

        shards.add_shard();
        shards.create_colo_group();
        shards.create_colo_group();

        let tablets = shards.active_tablets();
        println!("starting tablets:");
        for (tablet_id, (colo_group_id, range, size)) in tablets {
            println!("{:?} {:?} {:?} {}", tablet_id, colo_group_id, range, size);
        }

        const N_ITERATIONS: usize = 5000;

        let mut transfers_in_progress = Vec::new();
        let mut n_merges = 0;
        let mut n_splits = 0;
        let mut n_moves = 0;

        for i in 0..N_ITERATIONS {
            if i % 5 == 0 {
                for transfer in transfers_in_progress.drain(..) {
                    shards.finish_transfer(transfer);
                }
            } else if !transfers_in_progress.is_empty() {
                for _ in 0..=rand::random_range(0..transfers_in_progress.len()) {
                    let transfer = unordered_random_remove(&mut transfers_in_progress).unwrap();
                    shards.finish_transfer(transfer);
                }
            }

            if shards
                .shard_sizes()
                .values()
                .all(|size| *size < SHARD_CAPACITY * 9 / 10)
            {
                let tablets_to_grow = max(1, shards.n_active_tablets() / 20);
                for _ in 0..tablets_to_grow {
                    let tablet_id = shards.choose_tablet_zipf().unwrap();
                    shards.grow_tablet(tablet_id, rand::random_range(100_000_000..500_000_000));
                }
            }

            if shards.n_active_tablets() > 10 {
                let tablets_to_shrink = shards.n_active_tablets() / 40;
                for _ in 0..tablets_to_shrink {
                    let tablet_id = shards.choose_tablet_uniform().unwrap();
                    shards.shrink_tablet(tablet_id, rand::random_range(100_000_000..200_000_000));
                }
            }

            let mut shard_sizes = shards.shard_sizes();
            if shard_sizes.values().copied().sum::<u64>()
                > (shard_sizes.len() as u64) * SHARD_CAPACITY * 8 / 10
            {
                shards.add_shard();
                shard_sizes = shards.shard_sizes();
            }

            if i % 100 == 0 {
                println!("------------------");
                let shard_ids: BTreeSet<_> = shard_sizes.keys().collect();
                for shard_id in shard_ids {
                    println!(
                        "{:?}: {:.2}%",
                        shard_id,
                        100f64 * (*shard_sizes.get(shard_id).unwrap() as f64)
                            / (SHARD_CAPACITY as f64)
                    );
                }
            }

            let start = Instant::now();
            let active_tablets = shards.active_tablets();
            let n_active_tablets = active_tablets.len();
            let n_shards = shard_sizes.len();
            let plan = plan_rebalance(active_tablets, shard_sizes, shards.in_progress_shards());
            if i % 100 == 0 {
                println!(
                    "plan_rebalance took {:?} for {:?} tablets and {:?} shards",
                    Instant::now().duration_since(start),
                    n_active_tablets,
                    n_shards,
                );
            }

            for transfer in plan {
                match transfer {
                    TransferPlan::Merge(_, _, _) => n_merges += 1,
                    TransferPlan::Split(_, _, _) => n_splits += 1,
                    TransferPlan::Move(_, _) => n_moves += 1,
                }
                transfers_in_progress.push(shards.start_transfer(transfer));
            }
        }

        let tablets = shards.active_tablets();
        println!("{} merges", n_merges);
        println!("{} splits", n_splits);
        println!("{} moves", n_moves);
        let mut size_bucket_counts: BTreeMap<u64, usize> = BTreeMap::new();
        for (_, (_, _, size)) in tablets {
            *size_bucket_counts
                .entry((size / 500_000_000) * 500_000_000)
                .or_default() += 1;
        }
        let max_count = size_bucket_counts.values().copied().max().unwrap();
        for (size_bucket, count) in size_bucket_counts {
            println!(
                "{:>10.2} {}",
                (size_bucket as f64) / 1_000_000_000f64,
                "*".to_string().repeat(count * 100 / max_count)
            );
        }
    }

    struct Shards {
        shards: HashMap<ShardId, HashMap<TabletId, Tablet>>,
        shard_ids: RandSet<ShardId>,
        active_tablet_ids: RandSet<TabletId>,
        next_tablet_id: u64,
        next_colo_group: ColoGroupId,
        in_progress: HashSet<TransferIds>,
        in_progress_shards: RefCounts<ShardId>,
    }

    impl Shards {
        fn new() -> Self {
            Self {
                shards: HashMap::new(),
                active_tablet_ids: RandSet::new(),
                next_tablet_id: 1,
                shard_ids: RandSet::new(),
                next_colo_group: ColoGroupId(1),
                in_progress: HashSet::new(),
                in_progress_shards: RefCounts::new(),
            }
        }

        fn create_colo_group(&mut self) {
            let shard_id = self.shard_ids.choose_uniform().unwrap();
            let colo_group_id = self.next_colo_group;
            self.next_colo_group.0 += 1;
            self.create_tablet(
                shard_id,
                Tablet {
                    colo_group_id: colo_group_id,
                    range: Range::all(),
                    size: 0,
                    active: true,
                },
            );
        }

        fn add_shard(&mut self) {
            let shard_id = ShardId((self.shards.len() + 1) as u32);
            self.shards.insert(shard_id, HashMap::new());
            self.shard_ids.insert(shard_id);
        }

        fn create_tablet(&mut self, shard_id: ShardId, tablet: Tablet) -> TabletId {
            if tablet.range.is_empty() {
                panic!("tablet with empty range {:?}", tablet.range);
            }
            let tablets = self.shards.get_mut(&shard_id).unwrap();
            let tablet_id = TabletId(shard_id, self.next_tablet_id);
            self.next_tablet_id += 1;
            if tablet.active {
                self.active_tablet_ids.insert(tablet_id);
            }
            tablets.insert(tablet_id, tablet);
            tablet_id
        }

        fn start_transfer(&mut self, transfer: TransferPlan) -> TransferIds {
            let transfer_ids = match transfer {
                TransferPlan::Merge(src0_tablet_id, src1_tablet_id, dst_shard_id) => {
                    let src0 = self.tablet(src0_tablet_id).unwrap();
                    let src1 = self.tablet(src1_tablet_id).unwrap();
                    if src0.colo_group_id != src1.colo_group_id {
                        panic!(
                            "can't merge tablets not in the same colo_group: {:?} != {:?}",
                            src0.colo_group_id, src1.colo_group_id
                        );
                    }
                    if !src0.range.adjacent(&src1.range) {
                        panic!(
                            "can't merge non-adjacent ranges {:?} {:?}",
                            src0.range, src1.range
                        );
                    }

                    let dst_range = Range {
                        lower: min(src0.range.lower.borrow(), src1.range.lower.borrow()),
                        upper: max(src0.range.upper.borrow(), src1.range.upper.borrow()),
                    };

                    let dst_tablet_id = self.create_tablet(
                        dst_shard_id,
                        Tablet {
                            colo_group_id: src0.colo_group_id,
                            range: dst_range.to_vec(),
                            size: src0.size + src1.size,
                            active: false,
                        },
                    );

                    TransferIds {
                        srcs: vec![src0_tablet_id, src1_tablet_id],
                        dsts: vec![dst_tablet_id],
                    }
                }
                TransferPlan::Split(src_tablet_id, dst0_shard_id, dst1_shard_id) => {
                    let src = self.tablet(src_tablet_id).unwrap();
                    let (dst0_range, dst1_range) = split_range(src.range.clone());
                    let colo_group_id = src.colo_group_id;

                    let dst0_size = rand::random_range(src.size * 3 / 10..src.size * 7 / 10);
                    let dst1_size = src.size - dst0_size;

                    let dst0_tablet_id = self.create_tablet(
                        dst0_shard_id,
                        Tablet {
                            colo_group_id,
                            range: dst0_range,
                            size: dst0_size,
                            active: false,
                        },
                    );
                    let dst1_tablet_id = self.create_tablet(
                        dst1_shard_id,
                        Tablet {
                            colo_group_id,
                            range: dst1_range,
                            size: dst1_size,
                            active: false,
                        },
                    );

                    TransferIds {
                        srcs: vec![src_tablet_id],
                        dsts: vec![dst0_tablet_id, dst1_tablet_id],
                    }
                }
                TransferPlan::Move(src_tablet_id, dst_shard_id) => {
                    let src = self.tablet(src_tablet_id).unwrap();
                    let dst_tablet_id = self.create_tablet(
                        dst_shard_id,
                        Tablet {
                            colo_group_id: src.colo_group_id,
                            range: src.range.clone(),
                            size: src.size,
                            active: false,
                        },
                    );

                    TransferIds {
                        srcs: vec![src_tablet_id],
                        dsts: vec![dst_tablet_id],
                    }
                }
            };

            self.in_progress.insert(transfer_ids.clone());
            for tablet_id in &transfer_ids.srcs {
                self.in_progress_shards.incr(tablet_id.0);
            }
            for tablet_id in &transfer_ids.dsts {
                self.in_progress_shards.incr(tablet_id.0);
            }

            transfer_ids
        }

        fn finish_transfer(&mut self, transfer: TransferIds) {
            if !self.in_progress.remove(&transfer) {
                panic!(
                    "finish_transfer for not-in-progress transfer {:?}",
                    transfer
                );
            }
            for tablet_id in transfer.srcs {
                self.shards
                    .get_mut(&tablet_id.0)
                    .unwrap()
                    .remove(&tablet_id);
                self.active_tablet_ids.remove(&tablet_id);
                self.in_progress_shards.decr(&tablet_id.0);
            }
            for tablet_id in transfer.dsts {
                self.tablet_mut(tablet_id).unwrap().active = true;
                self.active_tablet_ids.insert(tablet_id);
                self.in_progress_shards.decr(&tablet_id.0);
            }
        }

        fn choose_tablet_zipf(&self) -> Option<TabletId> {
            self.active_tablet_ids.choose_zipf()
        }

        fn choose_tablet_uniform(&self) -> Option<TabletId> {
            self.active_tablet_ids.choose_uniform()
        }

        fn n_active_tablets(&self) -> usize {
            self.active_tablet_ids.len()
        }

        fn tablet(&self, tablet_id: TabletId) -> Option<&Tablet> {
            self.shards
                .get(&tablet_id.0)
                .map(|tablets| tablets.get(&tablet_id))
                .flatten()
        }

        fn tablet_mut(&mut self, tablet_id: TabletId) -> Option<&mut Tablet> {
            self.shards
                .get_mut(&tablet_id.0)
                .map(|tablets| tablets.get_mut(&tablet_id))
                .flatten()
        }

        fn grow_tablet(&mut self, tablet_id: TabletId, add: u64) {
            self.shards
                .get_mut(&tablet_id.0)
                .unwrap()
                .get_mut(&tablet_id)
                .unwrap()
                .size += add;
        }

        fn shrink_tablet(&mut self, tablet_id: TabletId, sub: u64) {
            let tablet = self
                .shards
                .get_mut(&tablet_id.0)
                .unwrap()
                .get_mut(&tablet_id)
                .unwrap();

            tablet.size = tablet.size.saturating_sub(sub);
        }

        fn shard_sizes(&self) -> HashMap<ShardId, u64> {
            let mut shard_sizes = HashMap::new();
            for (shard_id, tablets) in &self.shards {
                shard_sizes.entry(*shard_id).or_default();
                for (_, tablet) in tablets {
                    *shard_sizes.entry(*shard_id).or_default() += tablet.size;
                }
            }
            shard_sizes
        }

        fn active_tablets(&self) -> HashMap<TabletId, (ColoGroupId, Range<Vec<u8>>, u64)> {
            let mut active_tablets = HashMap::new();
            for (_, tablets) in &self.shards {
                for (tablet_id, tablet) in tablets {
                    if !tablet.active {
                        continue;
                    }
                    active_tablets.insert(
                        *tablet_id,
                        (tablet.colo_group_id, tablet.range.clone(), tablet.size),
                    );
                }
            }
            active_tablets
        }

        fn in_progress_shards(&self) -> HashSet<ShardId> {
            self.shards
                .keys()
                .filter(|shard_id| self.in_progress_shards.contains_key(shard_id))
                .copied()
                .collect()
        }
    }

    #[derive(Clone, Debug, Eq, Hash, PartialEq)]
    struct TransferIds {
        srcs: Vec<TabletId>,
        dsts: Vec<TabletId>,
    }

    struct RandSet<T> {
        indexes: HashMap<T, usize>,
        vec: Vec<T>,
    }

    impl<T> RandSet<T>
    where
        T: Hash + Eq + Clone,
    {
        fn new() -> Self {
            Self {
                indexes: HashMap::new(),
                vec: Vec::new(),
            }
        }

        fn len(&self) -> usize {
            self.indexes.len()
        }

        fn insert(&mut self, item: T) {
            if self.indexes.contains_key(&item) {
                return;
            }
            self.indexes.insert(item.clone(), self.vec.len());
            self.vec.push(item);
        }

        fn remove(&mut self, item: &T) {
            if let Some(index) = self.indexes.remove(&item) {
                let other_item = self.vec.pop().unwrap();
                if &other_item == item {
                    return;
                }
                self.vec[index] = other_item.clone();
                self.indexes.insert(other_item, index);
            }
        }

        fn choose_uniform(&self) -> Option<T> {
            self.vec.choose(&mut rand::rng()).cloned()
        }

        fn choose_zipf(&self) -> Option<T> {
            if self.vec.is_empty() {
                return None;
            }
            let mut rng = rand::rng();
            let index: f64 = Zipf::new(self.vec.len() as f64, 1.0f64)
                .unwrap()
                .sample(&mut rng)
                - 1f64; // generates in [1, n] instead of [0, n)
            Some(self.vec[index as usize].clone())
        }
    }

    struct RefCounts<K> {
        counts: HashMap<K, usize>,
    }

    impl<K> RefCounts<K>
    where
        K: Eq + Hash,
    {
        fn new() -> Self {
            Self {
                counts: HashMap::new(),
            }
        }

        fn incr(&mut self, key: K) {
            *self.counts.entry(key).or_default() += 1;
        }

        fn decr(&mut self, key: &K) {
            let remove = if let Some(count) = self.counts.get_mut(key) {
                *count -= 1;
                *count == 0
            } else {
                false
            };
            if remove {
                self.counts.remove(key);
            }
        }

        fn contains_key(&self, key: &K) -> bool {
            self.counts.contains_key(key)
        }
    }

    fn unordered_random_remove<T>(v: &mut Vec<T>) -> Option<T> {
        if v.is_empty() {
            return None;
        }
        let index = rand::random_range(0..v.len());
        let last_index = v.len() - 1;
        v.swap(index, last_index);
        v.pop()
    }

    fn split_range(range: Range<Vec<u8>>) -> (Range<Vec<u8>>, Range<Vec<u8>>) {
        let lower_bytes = match range.lower {
            Bound::BeforeAll => vec![],
            Bound::Before(ref key) => key.clone(),
            Bound::After(ref key) => key.clone(),
            Bound::AfterPrefix(_) => unimplemented!(),
            Bound::AfterAll => unimplemented!(),
        };
        let upper_bytes = match range.upper {
            Bound::BeforeAll => unimplemented!(),
            Bound::Before(ref key) => key.clone(),
            Bound::After(ref key) => key.clone(),
            Bound::AfterPrefix(_) => unimplemented!(),
            Bound::AfterAll => vec![
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            ],
        };

        let split_key = shortest_between(&lower_bytes, &upper_bytes).unwrap();

        (
            Range {
                lower: range.lower,
                upper: Bound::Before(split_key.clone()),
            },
            Range {
                lower: Bound::Before(split_key),
                upper: range.upper,
            },
        )
    }
}
