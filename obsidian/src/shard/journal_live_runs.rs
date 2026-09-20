use std::collections::VecDeque;

use obsidian_common::JournalSeq;
use obsidian_common::RunId;
use obsidian_util::KeyCounts;

pub(super) struct JournalLiveRuns {
    by_seq: VecDeque<(JournalSeq, RunId)>,
    counts: KeyCounts<RunId>,
}

impl JournalLiveRuns {
    pub fn new() -> Self {
        Self {
            by_seq: VecDeque::new(),
            counts: KeyCounts::new(),
        }
    }

    pub fn trim(&mut self, seq: JournalSeq) {
        while let Some((_, run_id)) = self.by_seq.pop_front_if(|(other_seq, _)| *other_seq < seq) {
            self.counts.decr(&run_id);
        }
    }

    pub fn insert(&mut self, seq: JournalSeq, run_id: RunId) {
        self.counts.incr(run_id);
        self.by_seq.push_back((seq, run_id));
    }

    pub fn run_ids(&self) -> impl Iterator<Item = RunId> + '_ {
        self.counts.keys().copied()
    }
}
