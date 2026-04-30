use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;

use crate::lsm::Lsm;
use crate::lsm::LsmOptions;
use crate::lsm::Manifest;
use crate::runtime;
use crate::runtime::Shards;
use crate::runtime::Storage;
use crate::tablet::active_tablet::ActiveTablet;
use crate::tablet::frozen_tablet::FrozenTablet;
use crate::tablet::hydrating_tablet::HydratingTablet;
use crate::tablet::journaled_lsm::JournaledLsm;
use crate::tablet::TabletJournalWriter;
use crate::util::StateMachine;
use crate::Bound;
use crate::ColoGroupId;
use crate::Direction;
use crate::HistoryRange;
use crate::InternalError;
use crate::Key;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Precondition;
use crate::Range;
use crate::Record;
use crate::Revision;
use crate::TabletId;
use crate::Timestamp;
use crate::Txid;

pub(crate) struct DataTablet {
    tablet_id: TabletId,
    colo_group_id: ColoGroupId,
    state_machine: StateMachine<TabletState>,
}

impl DataTablet {
    pub fn new_hydrating(
        tablet_id: TabletId,
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
        lsm_options: LsmOptions,
        storage: Arc<dyn Storage>,
        shards: Arc<dyn Shards>,
        journal: Arc<dyn TabletJournalWriter>,
        srcs: Vec<TabletId>,
    ) -> Self {
        Self {
            tablet_id: tablet_id,
            colo_group_id: colo_group_id,
            state_machine: StateMachine::new(TabletState::Hydrating(HydratingTablet::new(
                tablet_id,
                colo_group_id,
                range,
                lsm_options,
                storage,
                shards,
                journal,
                srcs,
            ))),
        }
    }

    pub fn new_active(
        tablet_id: TabletId,
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
        lsm: Lsm,
        journal: Arc<dyn TabletJournalWriter>,
        storage: Arc<dyn Storage>,
        shards: Arc<dyn Shards>,
    ) -> Self {
        Self {
            tablet_id: tablet_id,
            colo_group_id: colo_group_id,
            state_machine: StateMachine::new(TabletState::Active(ActiveTablet::new(
                tablet_id,
                colo_group_id,
                range,
                JournaledLsm::new(lsm, journal),
                storage,
                shards,
            ))),
        }
    }

    fn new_frozen(tablet: FrozenTablet) -> Self {
        Self {
            tablet_id: tablet.tablet_id(),
            colo_group_id: tablet.colo_group_id(),
            state_machine: StateMachine::new(TabletState::Frozen(tablet)),
        }
    }

    pub fn colo_group_id(&self) -> ColoGroupId {
        self.colo_group_id
    }

    pub async fn set_splits(&self, splits: Vec<Bound<Vec<u8>>>) {
        let _ = self
            .state_machine
            .with_state(async |state| {
                if let TabletState::Active(tablet) = state {
                    tablet.set_splits(splits);
                }
                Ok::<_, anyhow::Error>(())
            })
            .await;
    }

    pub async fn transition_frozen(&self) -> anyhow::Result<()> {
        self.state_machine
            .transition(async |state| {
                let next_state = match state.take() {
                    TabletState::Hydrating(hydrating) => {
                        // TODO: Do we try again from here? Or just let this tablet go defunct and
                        // retry the whole transfer?
                        TabletState::Frozen(hydrating.finish().await?)
                    }
                    TabletState::Frozen(frozen_tablet) => TabletState::Frozen(frozen_tablet),
                    TabletState::Active(active_tablet) => {
                        TabletState::Frozen(active_tablet.freeze().await)
                    }
                    TabletState::Defunct => {
                        return Err(anyhow!(
                            "cannot transition to frozen: no tablet state present"
                        ));
                    }
                };
                *state = next_state;
                Ok(())
            })
            .await
    }

    pub async fn transition_defunct(&self) -> anyhow::Result<()> {
        self.state_machine
            .transition(async |state| {
                if let TabletState::Active(tablet) = state.take() {
                    *state = TabletState::Active(tablet);
                    return Err(anyhow!("cannot transition from active to defunct"));
                }
                Ok(())
            })
            .await
    }

    pub async fn transition_active(
        &self,
        journal: Arc<dyn TabletJournalWriter>,
    ) -> anyhow::Result<()> {
        self.state_machine
            .transition(async |state| {
                let next_state = match state.take() {
                    TabletState::Hydrating(hydrating) => {
                        *state = TabletState::Hydrating(hydrating);
                        return Err(anyhow!("cannot transition from hydrating to active"));
                    }
                    TabletState::Frozen(frozen_tablet) => {
                        TabletState::Active(frozen_tablet.make_active(journal))
                    }
                    TabletState::Active(active_tablet) => TabletState::Active(active_tablet),
                    TabletState::Defunct => {
                        return Err(anyhow!(
                            "cannot transition to active: no tablet state present"
                        ));
                    }
                };
                *state = next_state;
                Ok(())
            })
            .await
    }

    pub async fn is_hydrating(&self) -> bool {
        // This loop is a little ugly but we should only ever get an error from with_state if
        // there's a concurrent transition in a particular and unlikely critical section.
        loop {
            if let Ok(out) = self
                .state_machine
                .with_state(async |state| {
                    Ok::<_, anyhow::Error>(matches!(state, TabletState::Hydrating(_)))
                })
                .await
            {
                return out;
            }
        }
    }

    pub async fn flush(&self) -> anyhow::Result<()> {
        self.state_machine
            .with_state(async |state| {
                if let TabletState::Active(active_tablet) = state {
                    return active_tablet.flush().await;
                }

                return Err(anyhow!(
                    "{:?} in wrong state for flush {}",
                    self.tablet_id,
                    state.name(),
                ));
            })
            .await
    }

    pub async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        self.state_machine
            .with_state(async |state| {
                match state {
                    // Tablets can't ever go from Defunct to anything else so we can safely ignore
                    // this.
                    TabletState::Defunct => {}
                    TabletState::Hydrating(hydrating_tablet) => {
                        hydrating_tablet.create_keyspace(keyspace_id)?;
                    }
                    TabletState::Active(active_tablet) => {
                        active_tablet.create_keyspace(keyspace_id).await?;
                    }
                    TabletState::Frozen(_) => {
                        return Err(anyhow!(
                            "{:?} in wrong state for create_keyspace {}",
                            self.tablet_id,
                            state.name(),
                        ));
                    }
                }

                Ok(())
            })
            .await
    }
}

#[async_trait]
impl runtime::Tablet for DataTablet {
    async fn get_multi(
        &self,
        ts: Timestamp,
        keys: BTreeSet<Key>,
    ) -> Result<BTreeMap<Key, Record>, InternalError> {
        self.state_machine
            .with_state(async |state| state.get_multi(ts, keys).await)
            .await
    }

    async fn get_latest_multi(
        &self,
        keys: BTreeSet<Key>,
    ) -> Result<(Timestamp, BTreeMap<Key, Record>), InternalError> {
        self.state_machine
            .with_state(async |state| state.get_latest_multi(keys).await)
            .await
    }

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> Result<Timestamp, InternalError> {
        self.state_machine
            .with_state(async |state| state.latest_snapshot(keys).await)
            .await
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<Range<Vec<u8>>>), InternalError> {
        self.state_machine
            .with_state(async |state| {
                state
                    .scan_page(ts, keyspace_id, range, direction, limit)
                    .await
            })
            .await
    }

    async fn history_page(
        &self,
        key: Key,
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Revision>, Option<HistoryRange>), InternalError> {
        self.state_machine
            .with_state(async |state| state.history_page(key, range, direction, limit).await)
            .await
    }

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        self.state_machine
            .with_state(async |state| state.write(preconds, muts).await)
            .await
    }

    async fn prepare(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        self.state_machine
            .with_state(async |state| state.prepare(txid, preconds, muts).await)
            .await
    }

    async fn cleanup_committed(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        self.state_machine
            .with_state(async |state| {
                state
                    .cleanup_committed(txid, ts, precond_keys, mut_keys)
                    .await
            })
            .await
    }

    async fn manifest(&self) -> anyhow::Result<Manifest> {
        self.state_machine
            .with_state(async |state| state.manifest().await)
            .await
    }

    async fn wait_mostly_hydrated(&self) -> anyhow::Result<()> {
        self.state_machine
            .with_state(async |state| state.wait_mostly_hydrated().await)
            .await
    }

    async fn catchup(&self) -> anyhow::Result<()> {
        self.state_machine
            .with_state(async |state| state.catchup().await)
            .await
    }

    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>> {
        self.state_machine
            .with_state(async |state| state.find_split().await)
            .await
    }
}

enum TabletState {
    Defunct,
    Hydrating(HydratingTablet),
    Active(ActiveTablet),
    Frozen(FrozenTablet),
}

impl TabletState {
    fn name(&self) -> &str {
        match self {
            TabletState::Defunct => "DEFUNCT",
            TabletState::Hydrating(_) => "HYDRATING",
            TabletState::Active(_) => "ACTIVE",
            TabletState::Frozen(_) => "FROZEN",
        }
    }

    fn take(&mut self) -> TabletState {
        std::mem::replace(self, TabletState::Defunct)
    }
}

#[async_trait]
impl runtime::Tablet for TabletState {
    async fn get_multi(
        &self,
        ts: Timestamp,
        keys: BTreeSet<Key>,
    ) -> Result<BTreeMap<Key, Record>, InternalError> {
        match self {
            TabletState::Active(active_tablet) => active_tablet.get_multi(ts, keys).await,
            TabletState::Frozen(frozen_tablet) => frozen_tablet.get_multi(ts, keys).await,
            _ => Err(anyhow!("wrong state {}", self.name()).into()),
        }
    }

    async fn get_latest_multi(
        &self,
        keys: BTreeSet<Key>,
    ) -> Result<(Timestamp, BTreeMap<Key, Record>), InternalError> {
        match self {
            TabletState::Active(active_tablet) => active_tablet.get_latest_multi(keys).await,
            TabletState::Frozen(frozen_tablet) => frozen_tablet.get_latest_multi(keys).await,
            _ => Err(anyhow!("in wrong state {}", self.name()).into()),
        }
    }

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> Result<Timestamp, InternalError> {
        match self {
            TabletState::Active(active_tablet) => active_tablet.latest_snapshot(keys).await,
            TabletState::Frozen(frozen_tablet) => frozen_tablet.latest_snapshot(keys).await,
            _ => Err(anyhow!("wrong state {}", self.name()).into()),
        }
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<Range<Vec<u8>>>), InternalError> {
        match self {
            TabletState::Active(active_tablet) => {
                active_tablet
                    .scan_page(ts, keyspace_id, range, direction, limit)
                    .await
            }
            TabletState::Frozen(frozen_tablet) => {
                frozen_tablet
                    .scan_page(ts, keyspace_id, range, direction, limit)
                    .await
            }
            _ => Err(anyhow!("wrong state {}", self.name()).into()),
        }
    }

    async fn history_page(
        &self,
        key: Key,
        range: HistoryRange,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Revision>, Option<HistoryRange>), InternalError> {
        match self {
            TabletState::Active(active_tablet) => {
                active_tablet
                    .history_page(key, range, direction, limit)
                    .await
            }
            TabletState::Frozen(frozen_tablet) => {
                frozen_tablet
                    .history_page(key, range, direction, limit)
                    .await
            }
            _ => Err(anyhow!("wrong state {}", self.name()).into()),
        }
    }

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        match self {
            TabletState::Active(active_tablet) => active_tablet.write(preconds, muts).await,
            _ => Err(anyhow!("wrong state {}", self.name()).into()),
        }
    }

    async fn prepare(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        match self {
            TabletState::Active(active_tablet) => active_tablet.prepare(txid, preconds, muts).await,
            _ => Err(anyhow!("wrong state {}", self.name()).into()),
        }
    }

    async fn cleanup_committed(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        match self {
            TabletState::Active(active_tablet) => {
                active_tablet
                    .cleanup_committed(txid, ts, precond_keys, mut_keys)
                    .await
            }
            _ => Err(anyhow!("wrong state {}", self.name()).into()),
        }
    }

    async fn manifest(&self) -> anyhow::Result<Manifest> {
        match self {
            TabletState::Hydrating(hydrating_tablet) => hydrating_tablet.manifest().await,
            TabletState::Active(active_tablet) => active_tablet.manifest().await,
            TabletState::Frozen(frozen_tablet) => frozen_tablet.manifest().await,
            _ => Err(anyhow!("wrong state {}", self.name()).into()),
        }
    }

    async fn wait_mostly_hydrated(&self) -> anyhow::Result<()> {
        match self {
            TabletState::Hydrating(hydrating_tablet) => {
                hydrating_tablet.wait_mostly_hydrated().await
            }
            _ => Err(anyhow!("wrong state {}", self.name()).into()),
        }
    }

    async fn catchup(&self) -> anyhow::Result<()> {
        match self {
            TabletState::Hydrating(hydrating_tablet) => hydrating_tablet.catchup().await,
            _ => Err(anyhow!("wrong state {}", self.name()).into()),
        }
    }

    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>> {
        match self {
            TabletState::Active(active_tablet) => active_tablet.find_split().await,
            _ => Err(anyhow!("wrong state {}", self.name()).into()),
        }
    }
}
