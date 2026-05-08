use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use obsidian_lsm::Lsm;

use crate::runtime::Tablet;
use crate::tablet::journaled_lsm::JournaledLsm;
use crate::tablet::tablet_inner::TabletInner;
use crate::tablet::tablet_journal_writer::TabletJournalWriter;
use crate::Bound;
use crate::ColoGroupId;
use crate::Direction;
use crate::HistoryRange;
use crate::InternalError;
use crate::Key;
use crate::KeyspaceId;
use crate::Manifest;
use crate::Mutation;
use crate::Precondition;
use crate::Range;
use crate::Record;
use crate::Revision;
use crate::TabletId;
use crate::Timestamp;
use crate::Txid;

/// MetaTablets are special from LsmTablets in two necessary ways:
///
/// 1. They always have TabletState::Active, and never transitions, because they can't participate
///    in transfers. This is important because all other tablets default to having no RW
///    permissions until they observe their own TabletMetadata. Obviously that doesn't work for the
///    tablet hosting TabletId::META, because it needs to receive a write to make any
///    TabletMetadata at all.
/// 2. They cannot participate in 2PC. For the sake of the simplicity of the interface, it
///    implements those methods, but always errors.
pub(crate) struct MetaTablet {
    inner: TabletInner<JournaledLsm>,
}

impl MetaTablet {
    pub(crate) fn new(lsm: Lsm, journal: Arc<dyn TabletJournalWriter>) -> Self {
        lsm.create_keyspace(KeyspaceId::META);

        Self {
            inner: TabletInner::new(
                TabletId::META,
                ColoGroupId::META,
                Range::all(),
                JournaledLsm::new(lsm, journal),
            ),
        }
    }
}

#[async_trait]
impl Tablet for MetaTablet {
    async fn get_multi(
        &self,
        ts: Timestamp,
        keys: BTreeSet<Key>,
    ) -> Result<BTreeMap<Key, Record>, InternalError> {
        self.inner.get_multi(ts, keys).await
    }

    async fn get_latest_multi(
        &self,
        keys: BTreeSet<Key>,
    ) -> Result<(Timestamp, BTreeMap<Key, Record>), InternalError> {
        self.inner.get_latest_multi(keys).await
    }

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> Result<Timestamp, InternalError> {
        self.inner.latest_snapshot(keys).await
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<Range<Vec<u8>>>), InternalError> {
        self.inner
            .scan_page(ts, keyspace_id, range, direction, limit)
            .await
    }

    async fn history_page(
        &self,
        key: Key,
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Revision>, Option<HistoryRange>), InternalError> {
        self.inner.history_page(key, range, direction, limit).await
    }

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        self.inner.write(preconds, muts).await
    }

    async fn prepare(
        &self,
        _txid: Txid,
        _preconds: Vec<Precondition>,
        _muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        Err(anyhow!("MetaTablet::prepare not allowed").into())
    }

    async fn cleanup_committed(
        &self,
        _txid: Txid,
        _ts: Timestamp,
        _precond_keys: BTreeSet<Key>,
        _mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        Err(anyhow!("MetaTablet::cleanup_committed not allowed").into())
    }

    async fn manifest(&self) -> anyhow::Result<Manifest> {
        Ok(self.inner.manifest())
    }

    async fn wait_mostly_hydrated(&self) -> anyhow::Result<()> {
        Err(anyhow!("MetaTablet::wait_mostly_hydrated not allowed").into())
    }

    async fn catchup(&self) -> anyhow::Result<()> {
        Err(anyhow!("MetaTablet::catchup not allowed").into())
    }

    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>> {
        Err(anyhow!("MetaTablet::find_split not allowed").into())
    }
}
