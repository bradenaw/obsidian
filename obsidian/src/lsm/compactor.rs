use std::cmp;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use anyhow::anyhow;
use rand::Rng;

use crate::lsm::index::Index;
use crate::lsm::Memtable;
use crate::lsm::Run;
use crate::range::Bound;
use crate::range::Range;
use crate::storage::FileReader;

struct Compactor<R> {
    index: Arc<Index<R>>,
}

impl<R> Compactor<R>
where
    R: FileReader + Clone,
{
    async fn compact_l0<F, Fut>(&self, f: F) -> anyhow::Result<()>
    where
        F: FnOnce(&[&Memtable], &[&Run<R>]) -> Fut,
        Fut: Future<Output = anyhow::Result<Vec<Run<R>>>>,
    {
        let snapshot = self.index.snapshot();
        if snapshot.l0_sealed.is_empty() {
            return Ok(());
        }

        let l0_sealed: Vec<_> = snapshot
            .l0_sealed
            .iter()
            .map(|arc_memtable| &**arc_memtable)
            .collect();

        let memtable_bounding_range =
            bounding_range(l0_sealed.iter().map(|memtable| memtable.range()));

        let intersecting_runs = snapshot.levels[1].range(memtable_bounding_range);

        let intersecting_runs_refs: Vec<&Run<_>> =
            intersecting_runs.iter().map(|arc_run| &**arc_run).collect();

        let add = f(&l0_sealed, &intersecting_runs_refs).await?;

        let remove = l0_sealed.iter().map(|memtable| memtable.id()).collect();

        self.index.replace(add, remove)?;

        Ok(())
    }

    async fn compact_from<F, Fut>(&self, level: usize, f: F) -> anyhow::Result<()>
    where
        F: FnOnce(&Run<R>, &[&Run<R>]) -> Fut,
        Fut: Future<Output = anyhow::Result<Vec<Run<R>>>>,
    {
        let snapshot = self.index.snapshot();

        if level == 0 {
            return Err(anyhow!("can't compact_from(0), use compact_l0()"));
        }
        if level >= snapshot.levels.len() {
            return Err(anyhow!(
                "can't compact_from a level past max levels: {} >= {}",
                level,
                snapshot.levels.len()
            ));
        }

        if snapshot.levels[level].runs.is_empty() {
            // Nothing to compact out of this level.
            return Ok(());
        }

        let idx = rand::thread_rng().gen_range(0..snapshot.levels[level].runs.len());
        let chosen = &snapshot.levels[level].runs[idx];

        let intersecting_runs = if level < snapshot.levels.len() - 1 {
            snapshot.levels[level + 1].range(chosen.range())
        } else {
            &[][..]
        };

        let intersecting_runs_refs: Vec<&Run<_>> =
            intersecting_runs.iter().map(|arc_run| &**arc_run).collect();

        let add = f(chosen, &intersecting_runs_refs).await?;

        let remove = HashSet::from([chosen.id()]);
        self.index.replace(add, remove)?;

        Ok(())
    }
}

fn bounding_range(ranges: impl Iterator<Item = Range<Vec<u8>>>) -> Range<Vec<u8>> {
    let mut lower = Bound::AfterAll;
    let mut upper = Bound::BeforeAll;
    for range in ranges {
        lower = cmp::min(lower, range.lower);
        upper = cmp::max(upper, range.upper);
    }
    Range { lower, upper }
}
