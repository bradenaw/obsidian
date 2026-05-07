use std::ops::Deref;

use obsidian_olf::OlfFile;

use crate::RunId;

pub(super) struct Run {
    inner: OlfFile,
}

impl Run {
    pub fn new(inner: OlfFile) -> Self {
        Self { inner }
    }

    pub fn run_id(&self) -> RunId {
        self.inner.id().into()
    }
}

impl Deref for Run {
    type Target = OlfFile;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
