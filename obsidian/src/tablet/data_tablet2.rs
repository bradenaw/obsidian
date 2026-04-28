use std::collections::BTreeMap;
use std::collections::BTreeSet;

use anyhow::anyhow;
use async_trait::async_trait;

use crate::lsm::Manifest;
use crate::runtime;
use crate::tablet::hydrating_tablet::HydratingTablet;
use crate::tablet::DataTablet;
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

pub(crate) struct DataTablet2 {
    tablet_id: TabletId,
    colo_group_id: ColoGroupId,
    state_machine: StateMachine<TabletState>,
}

impl DataTablet2 {
    pub fn new_hydrating(tablet: HydratingTablet) -> Self {
        Self {
            tablet_id: tablet.tablet_id(),
            colo_group_id: tablet.colo_group_id(),
            state_machine: StateMachine::new(TabletState::Hydrating(tablet)),
        }
    }

    pub fn new_active(tablet: DataTablet) -> Self {
        Self {
            tablet_id: tablet.tablet_id(),
            colo_group_id: tablet.colo_group_id(),
            state_machine: StateMachine::new(TabletState::Active(tablet)),
        }
    }

    pub fn new_frozen(tablet: DataTablet) -> Self {
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
                    TabletState::Frozen(tablet) => TabletState::Frozen(tablet),
                    TabletState::Active(tablet) => TabletState::Frozen(tablet),
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

    pub async fn transition_active(&self) -> anyhow::Result<()> {
        self.state_machine
            .transition(async |state| {
                let next_state = match state.take() {
                    TabletState::Hydrating(hydrating) => {
                        *state = TabletState::Hydrating(hydrating);
                        return Err(anyhow!("cannot transition from hydrating to active"));
                    }
                    TabletState::Frozen(tablet) => TabletState::Active(tablet),
                    TabletState::Active(tablet) => TabletState::Active(tablet),
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
                if let TabletState::Active(tablet) | TabletState::Frozen(tablet) = state {
                    return tablet.flush().await;
                }
                return Err(anyhow!(
                    "{:?} in wrong state for flush {}",
                    self.tablet_id,
                    state.name()
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
                    TabletState::Active(data_tablet) => {
                        data_tablet.create_keyspace(keyspace_id)?;
                    }
                    TabletState::Frozen(data_tablet) => {
                        data_tablet.create_keyspace(keyspace_id)?;
                    }
                }

                Ok(())
            })
            .await
    }
}

#[async_trait]
impl runtime::Tablet for DataTablet2 {
    async fn get_multi(
        &self,
        ts: Timestamp,
        keys: BTreeSet<Key>,
    ) -> Result<BTreeMap<Key, Record>, InternalError> {
        self.state_machine
            .with_state(async |state| {
                if let TabletState::Active(tablet) | TabletState::Frozen(tablet) = state {
                    return tablet.get_multi(ts, keys).await;
                }
                Err(InternalError::Other(anyhow!(
                    "{:?} in wrong state for get_multi {}",
                    self.tablet_id,
                    state.name(),
                )))
            })
            .await
    }

    async fn get_latest_multi(
        &self,
        keys: BTreeSet<Key>,
    ) -> Result<(Timestamp, BTreeMap<Key, Record>), InternalError> {
        self.state_machine
            .with_state(async |state| {
                if let TabletState::Active(tablet) | TabletState::Frozen(tablet) = state {
                    return tablet.get_latest_multi(keys).await;
                }
                Err(InternalError::Other(anyhow!(
                    "{:?} in wrong state for get_latest_multi {}",
                    self.tablet_id,
                    state.name(),
                )))
            })
            .await
    }

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> Result<Timestamp, InternalError> {
        self.state_machine
            .with_state(async |state| {
                if let TabletState::Active(tablet) | TabletState::Frozen(tablet) = state {
                    return tablet.latest_snapshot(keys).await;
                }
                Err(InternalError::Other(anyhow!(
                    "{:?} in wrong state for latest_snapshot {}",
                    self.tablet_id,
                    state.name(),
                )))
            })
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
                if let TabletState::Active(tablet) | TabletState::Frozen(tablet) = state {
                    return tablet
                        .scan_page(ts, keyspace_id, range, direction, limit)
                        .await;
                }
                Err(InternalError::Other(anyhow!(
                    "{:?} in wrong state for scan_page {}",
                    self.tablet_id,
                    state.name(),
                )))
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
            .with_state(async |state| {
                if let TabletState::Active(tablet) | TabletState::Frozen(tablet) = state {
                    return tablet.history_page(key, range, direction, limit).await;
                }
                Err(InternalError::Other(anyhow!(
                    "{:?} in wrong state for history_page {}",
                    self.tablet_id,
                    state.name(),
                )))
            })
            .await
    }

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        self.state_machine
            .with_state(async |state| {
                if let TabletState::Active(tablet) = state {
                    return tablet.write(preconds, muts).await;
                }
                Err(InternalError::Other(anyhow!(
                    "{:?} in wrong state for write {}",
                    self.tablet_id,
                    state.name(),
                )))
            })
            .await
    }

    async fn prepare(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        self.state_machine
            .with_state(async |state| {
                if let TabletState::Active(tablet) = state {
                    return tablet.prepare(txid, preconds, muts).await;
                }
                Err(InternalError::Other(anyhow!(
                    "{:?} in wrong state for prepare {}",
                    self.tablet_id,
                    state.name(),
                )))
            })
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
                if let TabletState::Active(tablet) = state {
                    return tablet
                        .cleanup_committed(txid, ts, precond_keys, mut_keys)
                        .await;
                }
                Err(anyhow!(
                    "{:?} in wrong state for cleanup_committed {}",
                    self.tablet_id,
                    state.name(),
                ))
            })
            .await
    }

    async fn manifest(&self) -> anyhow::Result<Manifest> {
        self.state_machine
            .with_state(async |state| match state {
                TabletState::Hydrating(hydrating_tablet) => hydrating_tablet.manifest().await,
                TabletState::Active(data_tablet) | TabletState::Frozen(data_tablet) => {
                    data_tablet.manifest().await
                }
                _ => {
                    return Err(anyhow!(
                        "{:?} in wrong state for manifest {}",
                        self.tablet_id,
                        state.name(),
                    ))
                }
            })
            .await
    }

    async fn wait_mostly_hydrated(&self) -> anyhow::Result<()> {
        self.state_machine
            .with_state(async |state| {
                if let TabletState::Hydrating(tablet) = state {
                    return tablet.wait_mostly_hydrated().await;
                }
                Err(anyhow!(
                    "{:?} in wrong state for wait_mostly_hydrated {}",
                    self.tablet_id,
                    state.name(),
                ))
            })
            .await
    }

    async fn catchup(&self) -> anyhow::Result<()> {
        self.state_machine
            .with_state(async |state| {
                if let TabletState::Hydrating(tablet) = state {
                    return tablet.catchup().await;
                }
                Err(anyhow!(
                    "{:?} in wrong state for catchup {}",
                    self.tablet_id,
                    state.name(),
                ))
            })
            .await
    }

    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>> {
        self.state_machine
            .with_state(async |state| {
                if let TabletState::Active(tablet) = state {
                    return tablet.find_split().await;
                }
                Err(anyhow!(
                    "{:?} in wrong state for find_split {}",
                    self.tablet_id,
                    state.name(),
                ))
            })
            .await
    }
}

enum TabletState {
    Defunct,
    Hydrating(HydratingTablet),
    Active(DataTablet),
    Frozen(DataTablet),
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
