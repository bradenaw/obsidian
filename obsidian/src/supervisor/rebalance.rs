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
pub(super) enum TransferPlan {
    Merge(TabletId, TabletId, ShardId),
    Split(TabletId, ShardId, ShardId),
    Move(TabletId, ShardId),
}

#[derive(Clone)]
pub(super) struct RebalanceOptions {
    // The target size for each range.
    pub(super) range_target_size: u64,
    // The total storage capacity, in bytes, of each shard.
    pub(super) shard_capacity: u64,
    // Only bother merging ranges if there are more than this many per shard for a given colo group.
    pub(super) merge_min_ranges_per_shard: usize,
    // Only bother merging ranges if there are more than this many for a given colo group.
    // Used to prevent e.g. immediately merging together the ranges made in create_colo_group().
    pub(super) merge_min_ranges: usize,
    // Only bother moving ranges off of a shard if its utilization is above this amount.
    pub(super) min_shard_size_for_move: u64,
}

impl RebalanceOptions {
    // If two adjacent ranges together are smaller than this, they can be merged.
    fn range_merge_size(&self) -> u64 {
        self.range_target_size / 2
    }

    // If a range is larger than this, it can be split.
    fn range_split_size(&self) -> u64 {
        self.range_target_size * 2
    }

    // A shard has to be this much larger than another in order to bother moving a range between
    // them to rebalance.
    fn move_min_imbalance(&self) -> u64 {
        self.range_target_size * 3
    }
}

impl Default for RebalanceOptions {
    fn default() -> Self {
        Self {
            range_target_size: 5_000_000_000,
            shard_capacity: 1_000_000_000_000,
            min_shard_size_for_move: 700_000_000_000,
            merge_min_ranges_per_shard: 8,
            merge_min_ranges: 1024,
        }
    }
}

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
pub(super) fn plan_rebalance(
    options: RebalanceOptions,
    active_tablets: HashMap<TabletId, (ColoGroupId, Range<Vec<u8>>, u64)>,
    eligible_shard_sizes: HashMap<ShardId, u64>,
) -> Vec<TransferPlan> {
    let mut plan = Vec::new();

    let n_shards = eligible_shard_sizes.len();
    let mut eligible_shards_by_size: DoublePriorityQueue<_, _> =
        eligible_shard_sizes.into_iter().collect();

    let split_candidates = {
        let mut split_candidates = PriorityQueue::new();
        for (tablet_id, (_, _, size)) in &active_tablets {
            if !eligible_shards_by_size.contains(&tablet_id.0) {
                continue;
            }

            if *size < options.range_split_size() {
                continue;
            }

            split_candidates.push(*tablet_id, *size);
        }
        split_candidates
    };
    for (tablet_id, _) in split_candidates.into_iter() {
        // Prefer to split in-place because it'll be almost free - the data is already in local
        // cache. If there's still imbalance after it's finished we can move one.
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
            **n_tablets > n_shards * options.merge_min_ranges_per_shard
                && **n_tablets > options.merge_min_ranges
        })
        .map(|(colo_group_id, _)| *colo_group_id)
        .collect();

    // Prioritize merging the smallest slices possible by putting candidates in a priority queue by
    // size.
    let merge_candidates = {
        let mut merge_candidates = PriorityQueue::new();
        for (tablet_id, (colo_group_id, _, size)) in &active_tablets {
            if !eligible_shards_by_size.contains(&tablet_id.0) {
                continue;
            }
            // We only bother to merge if two adjacent tablets are less than RANGE_MERGE_SIZE,
            // which implies that at least one of them is less than half of that.
            if *size > options.range_merge_size() / 2 {
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
        if !eligible_shards_by_size.contains(&tablet_id.0) {
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

        if !eligible_shards_by_size.contains(&adjacent_tablet_id.0) {
            continue;
        }

        let adjacent_tablet_size = active_tablets.get(&adjacent_tablet_id).unwrap().2;

        if size + adjacent_tablet_size >= options.range_merge_size() {
            continue;
        }

        let shard_id = if let Some((shard_id, _)) = eligible_shards_by_size.pop_min() {
            shard_id
        } else {
            break;
        };

        eligible_shards_by_size.remove(&tablet_id.0);
        eligible_shards_by_size.remove(&adjacent_tablet_id.0);
        eligible_shards_by_size.remove(&shard_id);
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

        if max_shard_size < options.min_shard_size_for_move {
            break;
        }

        // Only bother moving if there's enough imbalance that it'll matter, otherwise it's
        // just churn for no reason.
        if max_shard_size - min_shard_size < options.move_min_imbalance() {
            break;
        }

        let tablet_id = if let Some(tablet_id) = tablet_by_shard.get(&max_shard_id) {
            tablet_id
        } else {
            break;
        };

        eligible_shards_by_size.pop_min();
        eligible_shards_by_size.pop_max();
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
    use std::assert_matches;
    use std::cmp::max;
    use std::cmp::min;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::hash::Hash;

    use anyhow::anyhow;
    use obsidian_common::Bound;
    use obsidian_common::ColoGroupId;
    use obsidian_common::Range;
    use obsidian_common::RangeSet;
    use obsidian_common::ShardId;
    use obsidian_common::TabletId;
    use obsidian_util::shortest_between;
    use obsidian_util::KeyCounts;
    use rand::seq::SliceRandom;

    use super::plan_rebalance;
    use super::TransferPlan;
    use crate::supervisor::rebalance::RebalanceOptions;

    fn make_tablets(lower_to_size: BTreeMap<Bound<Vec<u8>>, u64>) -> HashMap<TabletId, Tablet> {
        let mut iter = lower_to_size.into_iter().peekable();
        let mut tablets = HashMap::new();
        while let Some((lower, size)) = iter.next() {
            let upper = iter
                .peek()
                .map(|(lower, _)| lower.clone())
                .unwrap_or_else(|| Bound::AfterAll);
            tablets.insert(
                TabletId(ShardId(1), (tablets.len() as u64) + 1),
                Tablet {
                    colo_group_id: ColoGroupId(1),
                    range: Range { lower, upper },
                    size,
                    active: true,
                },
            );
        }
        tablets
    }

    #[test]
    fn test_plan_rebalance_merge() {
        let options = {
            let mut options = RebalanceOptions::default();
            options.merge_min_ranges = 0;
            options.merge_min_ranges_per_shard = 0;
            options.min_shard_size_for_move = 0;
            options.range_target_size = 5_000_000_000;
            options
        };

        let shards = Shards::from_tablets(make_tablets(BTreeMap::from([
            (Bound::BeforeAll, options.range_target_size),
            (Bound::Before(vec![0x08, 0x1b]), 1267109177),
            (Bound::Before(vec![0x08, 0x1e]), 845756719),
            (Bound::Before(vec![0x08, 0x20]), options.range_target_size),
        ])));

        let transfers = plan_rebalance(
            options.clone(),
            shards.active_tablets(),
            shards.eligible_shard_sizes(),
        );
        assert_matches!(
            &transfers[..],
            &[TransferPlan::Merge(
                TabletId(ShardId(1), 3),
                TabletId(ShardId(1), 2),
                ShardId(1),
            )]
        );
    }

    #[test]
    fn test_plan_rebalance_split() {
        let options = {
            let mut options = RebalanceOptions::default();
            options.merge_min_ranges = 0;
            options.merge_min_ranges_per_shard = 0;
            options.min_shard_size_for_move = 0;
            options.range_target_size = 5_000_000_000;
            options
        };

        let shards = Shards::from_tablets(make_tablets(BTreeMap::from([
            (Bound::BeforeAll, options.range_target_size),
            (Bound::Before(vec![0x04]), options.range_split_size() + 5),
            (Bound::Before(vec![0x09]), options.range_target_size),
        ])));

        let transfers = plan_rebalance(
            options.clone(),
            shards.active_tablets(),
            shards.eligible_shard_sizes(),
        );
        assert_matches!(
            &transfers[..],
            &[TransferPlan::Split(
                TabletId(ShardId(1), 2),
                ShardId(1),
                ShardId(1)
            )]
        );
    }

    fn rebalance_until_converge(options: &RebalanceOptions, shards: &mut Shards) {
        loop {
            let plan = plan_rebalance(
                options.clone(),
                shards.active_tablets(),
                shards.eligible_shard_sizes(),
            );
            if plan.is_empty() {
                break;
            }
            for transfer_plan in plan {
                let transfer_ids = shards.start_transfer(transfer_plan);
                shards.finish_transfer(transfer_ids);
            }
        }
    }

    fn check_balance(options: &RebalanceOptions, shards: &Shards) {
        // Make sure we still have everything.
        assert_eq!(
            shards.tablets().filter(|(_, tablet)| !tablet.active).next(),
            None,
        );
        let mut range_set = RangeSet::new();
        for (_, tablet) in shards.tablets() {
            assert!(!range_set.intersects_range(&tablet.range));
            range_set.add_range(tablet.range.clone());
        }
        assert_eq!(range_set.contiguous(), Some(Range::all()));

        // Make sure we actually did rebalance - there aren't any outlier shards.
        let shard_sizes = shards.shard_sizes();
        let total_bytes = shard_sizes.values().sum::<u64>();
        let avg_shard_size = total_bytes / (shard_sizes.len() as u64);
        let max_expected_shard_size = max(
            // We should get every shard under min_shard_size_for_move if there's enough capacity.
            options.min_shard_size_for_move,
            // Otherwise they should end up roughly at the average.
            avg_shard_size + options.move_min_imbalance(),
        );

        assert_eq!(
            shard_sizes
                .iter()
                .filter(|(_, size)| **size > max_expected_shard_size)
                .next(),
            None,
        );

        // Make sure there aren't any tablets that should've been split.
        assert_eq!(
            shards
                .tablets()
                .filter(|(_, tablet)| tablet.size > options.range_split_size())
                .next(),
            None,
        );

        // Make sure there aren't any tablets that should've been merged.
        let tablets_by_lower: HashMap<_, _> = shards
            .tablets()
            .map(|(tablet_id, tablet)| (tablet.range.lower.clone(), (tablet_id, tablet)))
            .collect();
        for (tablet_id, tablet) in shards.tablets() {
            if let Some((next_tablet_id, next_tablet)) = tablets_by_lower.get(&tablet.range.upper) {
                assert!(
                    tablet.size + next_tablet.size > options.range_merge_size(),
                    "{:?} ({:?}, {}B) should have been merged with {:?} ({:?}, {}B)",
                    tablet_id,
                    tablet.range,
                    tablet.size,
                    next_tablet_id,
                    next_tablet.range,
                    next_tablet.size,
                );
            }
        }
    }

    #[test]
    fn test_plan_rebalance_converges() {
        let options = RebalanceOptions::default();

        let mut tablets = vec![Tablet {
            colo_group_id: ColoGroupId(1),
            range: Range {
                lower: Bound::BeforeAll,
                upper: Bound::Before(vec![0x00, 0x00]),
            },
            size: 0,
            active: true,
        }];
        let max_prefix = u16::MAX / 6;
        for prefix in 0..max_prefix {
            tablets.push(Tablet {
                colo_group_id: ColoGroupId(1),
                range: Range {
                    lower: Bound::Before(prefix.to_be_bytes().to_vec()),
                    upper: Bound::Before((prefix + 1).to_be_bytes().to_vec()),
                },
                // Low prefixes are small and mergeable, large prefixes are large and splittable.
                size: (prefix as u64) * options.range_split_size() * 12
                    / (10 * (max_prefix as u64)),
                active: true,
            });
        }
        tablets.push(Tablet {
            colo_group_id: ColoGroupId(1),
            range: Range {
                lower: tablets.last().unwrap().range.upper.clone(),
                upper: Bound::AfterAll,
            },
            size: 0,
            active: true,
        });
        tablets.shuffle(&mut rand::rng());

        let mut tablets_by_id = HashMap::new();
        // Create imbalance by filling up shards in order with a shrinking target_size, lower shard
        // numbers are more full.
        let mut target_fill = options.min_shard_size_for_move * 105 / 100;
        let mut current_size = 0u64;
        let mut current_shard_id = ShardId(1);
        let mut next_tablet_seq = 1u64;
        for tablet in tablets {
            if current_size + tablet.size > options.shard_capacity || current_size > target_fill {
                current_shard_id.0 += 1;
                current_size = 0;
                target_fill = max(
                    target_fill - options.shard_capacity / 1000,
                    options.shard_capacity * 65 / 100,
                );
            }
            let tablet_id = TabletId(current_shard_id, next_tablet_seq);
            next_tablet_seq += 1;
            current_size += tablet.size;
            tablets_by_id.insert(tablet_id, tablet);
        }

        let mut shards = Shards::from_tablets(tablets_by_id);
        println!("starting shard sizes ------------");
        shards.print_shard_sizes(options.shard_capacity);
        println!("starting tablet size distribution ------------");
        shards.print_tablet_size_dist();

        rebalance_until_converge(&options, &mut shards);

        println!("ending shard sizes ------------");
        shards.print_shard_sizes(options.shard_capacity);
        println!("ending tablet size distribution ------------");
        shards.print_tablet_size_dist();

        check_balance(&options, &shards);
    }

    #[derive(Debug, Eq, PartialEq)]
    struct Tablet {
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
        size: u64,
        active: bool,
    }

    struct Shards {
        shards: HashMap<ShardId, HashMap<TabletId, Tablet>>,
        shard_ids: HashSet<ShardId>,
        active_tablet_ids: HashSet<TabletId>,
        next_tablet_id: u64,
        next_colo_group: ColoGroupId,
        in_progress: HashSet<TransferIds>,
        in_progress_shards: KeyCounts<ShardId>,
    }

    impl Shards {
        fn new() -> Self {
            Self {
                shards: HashMap::new(),
                active_tablet_ids: HashSet::new(),
                next_tablet_id: 1,
                shard_ids: HashSet::new(),
                next_colo_group: ColoGroupId(1),
                in_progress: HashSet::new(),
                in_progress_shards: KeyCounts::new(),
            }
        }

        fn from_tablets(tablets: HashMap<TabletId, Tablet>) -> Self {
            let mut shards = Shards::new();
            for (tablet_id, tablet) in tablets {
                if tablet.active {
                    shards.active_tablet_ids.insert(tablet_id);
                }
                shards.shard_ids.insert(tablet_id.0);
                shards.next_colo_group = max(
                    shards.next_colo_group,
                    ColoGroupId(tablet.colo_group_id.0 + 1),
                );
                shards.next_tablet_id = max(shards.next_tablet_id, tablet_id.1 + 1);
                shards
                    .shards
                    .entry(tablet_id.0)
                    .or_default()
                    .insert(tablet_id, tablet);
            }
            shards
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
                    let src = self
                        .tablet(src_tablet_id)
                        .ok_or_else(|| anyhow!("missing {:?}", src_tablet_id))
                        .unwrap();
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
            for tablet_id in &transfer.srcs {
                if self
                    .shards
                    .get_mut(&tablet_id.0)
                    .unwrap()
                    .remove(tablet_id)
                    .is_none()
                {
                    panic!("tried to remove non-existent {:?}", tablet_id);
                }
                self.active_tablet_ids.remove(tablet_id);
                self.in_progress_shards.decr(&tablet_id.0);
            }
            for tablet_id in &transfer.dsts {
                self.tablet_mut(*tablet_id).unwrap().active = true;
                self.active_tablet_ids.insert(*tablet_id);
                self.in_progress_shards.decr(&tablet_id.0);
            }
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

        fn eligible_shard_sizes(&self) -> HashMap<ShardId, u64> {
            let mut shard_sizes = self.shard_sizes();
            for shard_id in self.in_progress_shards() {
                shard_sizes.remove(&shard_id);
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

        fn tablets(&self) -> impl Iterator<Item = (TabletId, &Tablet)> {
            self.shards
                .values()
                .flat_map(|tablets| tablets.iter())
                .map(|(tablet_id, tablet)| (*tablet_id, tablet))
        }

        fn print_shard_sizes(&self, shard_capacity: u64) {
            let shard_sizes = self.shard_sizes();
            let shard_ids: BTreeSet<_> = shard_sizes.keys().collect();
            for shard_id in shard_ids {
                println!(
                    "{:?}: {:.2}%",
                    shard_id,
                    100f64 * (*shard_sizes.get(shard_id).unwrap() as f64) / (shard_capacity as f64)
                );
            }
        }

        fn print_tablet_size_dist(&self) {
            let mut size_bucket_counts: BTreeMap<u64, usize> = BTreeMap::new();
            const BUCKET_SIZE: u64 = 500_000_000;
            for (_, tablet) in self.tablets() {
                *size_bucket_counts
                    .entry((tablet.size / BUCKET_SIZE) * BUCKET_SIZE)
                    .or_default() += 1;
            }
            let max_count = size_bucket_counts.values().copied().max().unwrap();
            if let (Some((min_bucket, _)), Some((max_bucket, _))) = (
                size_bucket_counts.first_key_value(),
                size_bucket_counts.last_key_value(),
            ) {
                for size_bucket in (*min_bucket..*max_bucket).step_by(BUCKET_SIZE as usize) {
                    let count = size_bucket_counts.get(&size_bucket).copied().unwrap_or(0);
                    println!(
                        "{:>10.2}GB {}",
                        (size_bucket as f64) / 1_000_000_000f64,
                        "*".to_string().repeat(count * 100 / max_count)
                    );
                }
            }
        }
    }

    #[derive(Clone, Debug, Eq, Hash, PartialEq)]
    struct TransferIds {
        srcs: Vec<TabletId>,
        dsts: Vec<TabletId>,
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
