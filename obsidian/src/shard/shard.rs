use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use crossbeam::sync::ShardedLock;
use obsidian_common::ranges_to_splits;
use obsidian_external::Storage;
use obsidian_lsm::Lsm;
use obsidian_lsm::LsmOptions;
use obsidian_util::Owned;
use obsidian_util::Retry;
use obsidian_util::WeakView;
use obsidian_util::WithBackground;

use crate::meta::MetaKey;
use crate::meta::MetaReader;
use crate::meta::MetaState;
use crate::meta::MetaSubscriber;
use crate::meta::MetaSynced;
use crate::meta::MetaSyncedSnapshot;
use crate::meta::SyncType;
use crate::meta::TabletState;
use crate::runtime;
use crate::runtime::Meta;
use crate::runtime::Shards;
use crate::runtime::Tablet;
use crate::tablet::DataTablet;
use crate::tablet::MetaTablet;
use crate::tablet::ShardMetaTablet;
use crate::tablet::TabletJournalWriter;
use crate::Bound;
use crate::ColoGroupId;
use crate::Direction;
use crate::HistoryRange;
use crate::InternalError;
use crate::JournalEntry;
use crate::Key;
use crate::KeyspaceId;
use crate::Manifest;
use crate::Mutation;
use crate::Precondition;
use crate::Range;
use crate::Record;
use crate::Revision;
use crate::ShardId;
use crate::TabletId;
use crate::TabletJournalEntry;
use crate::Timestamp;
use crate::TxOutcome;
use crate::Txid;

pub(crate) struct Shard(WithBackground<ShardInner>);

impl Shard {
    pub(crate) async fn new(
        shard_id: ShardId,
        storage: Arc<dyn Storage>,
        meta: Arc<dyn Meta>,
        shards: Arc<dyn Shards>,
        lsm_options: LsmOptions,
        mut lsms: HashMap<TabletId, Lsm>,
        journal: Arc<dyn ShardJournalWriter>,
    ) -> anyhow::Result<Self> {
        // TODO: We need to make sure we don't rewind MetaSynced's snapshot on promotion. We can
        // choose to either:
        // 1. Persist the synced snapshot into ShardMetaTablet before responding to wait_meta_sync
        // 2. Sync to latest during promotion
        //
        // (2) requires meta to be available to promote anything, which isn't great.
        let meta_synced = Arc::new(MetaSynced::new(Arc::clone(&meta)));

        let shard_meta_lsm = match lsms.remove(&TabletId::shard_meta(shard_id)) {
            Some(shard_meta_lsm) => shard_meta_lsm,
            None => Lsm::empty(lsm_options.clone(), Arc::clone(&storage)),
        };
        let shard_meta_tablet = ShardMetaTablet::new(
            shard_id,
            shard_meta_lsm,
            Arc::new(ShardTabletJournalWriter::new(
                TabletId::shard_meta(shard_id),
                Arc::clone(&journal),
            )),
            meta_synced.clone(),
            shards.clone(),
        );

        let meta_tablet = if shard_id == TabletId::META.0 {
            let meta_lsm = match lsms.remove(&TabletId::META) {
                Some(meta_lsm) => meta_lsm,
                None => Lsm::empty(lsm_options.clone(), Arc::clone(&storage)),
            };
            Some(Owned::new(MetaTablet::new(
                meta_lsm,
                Arc::new(ShardTabletJournalWriter::new(
                    TabletId::META,
                    Arc::clone(&journal),
                )),
            )))
        } else {
            None
        };

        let inner = ShardInner {
            id: shard_id,
            storage,
            meta,
            meta_tablet,
            meta_synced: meta_synced.clone(),
            shards,
            shard_meta_tablet: Owned::new(shard_meta_tablet),
            tablets: ShardedLock::new(HashMap::new()),
            lsm_options,
            journal,
        };

        let snapshot = meta_synced.snapshot();
        for (tablet_id, lsm) in lsms.into_iter() {
            let tablet_metadata = ShardInner::shard_tablet_metadata(tablet_id, &snapshot).await?;
            inner.add_data_tablet(tablet_id, tablet_metadata, lsm)?;
        }

        let shard = Shard(WithBackground::new(inner));

        meta_synced.subscribe(&shard.0);

        Ok(shard)
    }
}

#[async_trait]
impl crate::runtime::Shard for Shard {
    fn id(&self) -> ShardId {
        self.0.id
    }

    fn tablet(&self, tablet_id: TabletId) -> anyhow::Result<Arc<dyn Tablet>> {
        if tablet_id == TabletId::META {
            let meta_tablet = self
                .0
                .meta_tablet
                .as_ref()
                .ok_or_else(|| anyhow!("{:?} not a member of {:?}", tablet_id, self.0.id))?;
            return Ok(Owned::weak(meta_tablet) as Arc<dyn Tablet>);
        }
        if tablet_id == TabletId::shard_meta(self.0.id) {
            return Ok(Owned::weak(&self.0.shard_meta_tablet) as Arc<dyn Tablet>);
        }

        let tablets = self.0.tablets.read().unwrap();
        Ok(Owned::weak(
            tablets
                .get(&tablet_id)
                .ok_or_else(|| anyhow!("{:?} not found", tablet_id))?,
        ))
    }

    async fn wait_meta_sync(&self, ts: Timestamp) -> anyhow::Result<()> {
        self.0.meta_synced.wait(ts).await?;

        Ok(())
    }

    async fn tx_try_commit(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<TxOutcome> {
        self.0
            .shard_meta_tablet
            .tx_try_commit(txid, ts, precond_keys, mut_keys)
            .await
    }

    async fn tx_try_abort(&self, txid: Txid) -> anyhow::Result<TxOutcome> {
        self.0.shard_meta_tablet.tx_try_abort(txid).await
    }

    async fn tx_wait(&self, txid: Txid) -> Result<TxOutcome, InternalError> {
        self.0.shard_meta_tablet.tx_wait(txid).await
    }
}

struct ShardInner {
    id: ShardId,
    storage: Arc<dyn Storage>,
    meta: Arc<dyn Meta>,
    meta_synced: Arc<MetaSynced>,
    shards: Arc<dyn Shards>,
    journal: Arc<dyn ShardJournalWriter>,
    lsm_options: LsmOptions,

    shard_meta_tablet: Owned<ShardMetaTablet>,
    meta_tablet: Option<Owned<MetaTablet>>, // Present only if id==ShardId::META.
    // Careful: these are wrapped in an Arc to simplify interacting with this lock - there's a
    // bunch of things that need to interact with the tablets inside but don't need to hold the
    // lock while doing it. We need to not leak this Arc outside of this type, since we want to be
    // able to brick tablets by dropping.
    tablets: ShardedLock<HashMap<TabletId, Arc<Owned<DataTablet>>>>,
}

impl ShardInner {
    async fn ensure_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        let tablets = {
            let tablets = self.tablets.read().unwrap();
            tablets
                .values()
                .map(|tablet| Arc::clone(&tablet))
                .collect::<Vec<_>>()
        };
        for tablet in tablets {
            if tablet.colo_group_id() == keyspace_id.0 {
                tablet.create_keyspace(keyspace_id).await?;
            }
        }
        Ok(())
    }

    async fn try_sync_meta(
        &self,
        sync_type: &SyncType,
        snapshot: &MetaSyncedSnapshot,
    ) -> anyhow::Result<()> {
        match sync_type {
            SyncType::Initial => {
                let owned_tablet_ids = snapshot.shard_tablet_ids(self.id).await?;
                for tablet_id in owned_tablet_ids {
                    let tablet_metadata = Self::shard_tablet_metadata(tablet_id, snapshot).await?;
                    self.create_or_transition_tablet(tablet_id, tablet_metadata)
                        .await?;
                }
                let keyspace_ids = snapshot.keyspace_ids().await?;
                for keyspace_id in keyspace_ids {
                    self.ensure_keyspace(keyspace_id).await?;
                }
            }
            SyncType::Tx(meta_keys) => {
                for meta_key in meta_keys {
                    match meta_key {
                        MetaKey::Tablet(tablet_id) => {
                            if tablet_id.0 != self.id {
                                continue;
                            }

                            let tablet_metadata =
                                Self::shard_tablet_metadata(*tablet_id, snapshot).await?;
                            self.create_or_transition_tablet(*tablet_id, tablet_metadata)
                                .await?;
                        }
                        MetaKey::Keyspace(keyspace_id) => {
                            self.ensure_keyspace(*keyspace_id).await?;
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    fn add_data_tablet(
        &self,
        tablet_id: TabletId,
        tablet_metadata: ShardTabletMetadata,
        lsm: Lsm,
    ) -> anyhow::Result<()> {
        let mut tablets = self.tablets.write().unwrap();
        tablets.insert(
            tablet_id,
            Arc::new(Owned::new(self.new_data_tablet(
                tablet_id,
                tablet_metadata,
                lsm,
            )?)),
        );
        Ok(())
    }

    fn new_data_tablet(
        &self,
        tablet_id: TabletId,
        tablet_metadata: ShardTabletMetadata,
        lsm: Lsm,
    ) -> anyhow::Result<DataTablet> {
        Ok(match tablet_metadata.state {
            TabletState::Active => DataTablet::new_active(
                tablet_id,
                tablet_metadata.colo_group_id,
                tablet_metadata.range,
                lsm,
                Arc::new(ShardTabletJournalWriter::new(
                    tablet_id,
                    Arc::clone(&self.journal),
                )),
                Arc::clone(&self.storage),
                Arc::clone(&self.shards),
            ),
            TabletState::Hydrating => {
                let srcs = if let Some(TabletTransfer::Dst { srcs }) = tablet_metadata.transfer {
                    srcs
                } else {
                    return Err(anyhow!(
                        "{:?} is in state {:?} but does not have any sources",
                        tablet_id,
                        TabletState::Hydrating
                    ));
                };

                DataTablet::new_hydrating(
                    tablet_id,
                    tablet_metadata.colo_group_id,
                    tablet_metadata.range,
                    self.lsm_options.clone(),
                    Arc::clone(&self.storage),
                    Arc::clone(&self.shards),
                    Arc::new(ShardTabletJournalWriter::new(
                        tablet_id,
                        Arc::clone(&self.journal),
                    )),
                    srcs,
                )
            }
            _ => {
                todo!()
            }
        })
    }

    async fn create_or_transition_tablet(
        &self,
        tablet_id: TabletId,
        tablet_metadata: ShardTabletMetadata,
    ) -> anyhow::Result<()> {
        if tablet_id.0 != self.id {
            return Err(anyhow!(
                "can't create/transition {:?}: wrong shard {:?}",
                tablet_id,
                self.id
            ));
        }

        if tablet_id == TabletId::shard_meta(self.id) {
            return Err(anyhow!(
                "can't create/transition {:?}: shard meta always exists and never transitions",
                tablet_id
            ));
        }

        if let Some(tablet) = {
            let tablets = self.tablets.read().unwrap();
            tablets.get(&tablet_id).map(Arc::clone)
        } {
            log::info!(
                "{:?} possibly transitioning {:?} to {:?}",
                self.id,
                tablet_id,
                tablet_metadata.state,
            );
            match tablet_metadata.state {
                TabletState::Defunct => {
                    tablet.transition_defunct().await?;
                }
                TabletState::Hydrating => {
                    if !tablet.is_hydrating().await {
                        return Err(anyhow!(
                            "{:?}'s state is intended to be {:?}, but cannot transition to it",
                            tablet_id,
                            TabletState::Hydrating
                        ));
                    }
                }
                TabletState::Active => {
                    tablet
                        .transition_active(Arc::new(ShardTabletJournalWriter::new(
                            tablet_id,
                            Arc::clone(&self.journal),
                        )))
                        .await?;
                }
                TabletState::Frozen => {
                    tablet.transition_frozen().await?;
                }
            }
            if let Some(TabletTransfer::Src { splits }) = tablet_metadata.transfer {
                tablet.set_splits(splits).await;
            } else {
                tablet.set_splits(vec![]).await;
            }
            log::info!(
                "{:?} possibly transitioning {:?} to {:?} -> done",
                self.id,
                tablet_id,
                tablet_metadata.state,
            );
            return Ok(());
        }

        log::info!(
            "creating empty {:?} for {:?}/{:?}",
            tablet_id,
            tablet_metadata.colo_group_id,
            tablet_metadata.range
        );
        let mut tablets = self.tablets.write().unwrap();
        tablets.insert(
            tablet_id,
            Arc::new(Owned::new(self.new_data_tablet(
                tablet_id,
                tablet_metadata,
                Lsm::empty(self.lsm_options.clone(), Arc::clone(&self.storage)),
            )?)),
        );

        Ok(())
    }

    async fn shard_tablet_metadata(
        tablet_id: TabletId,
        snapshot: &MetaSyncedSnapshot,
    ) -> anyhow::Result<ShardTabletMetadata> {
        let tablet_metadata = snapshot.tablet_metadata(tablet_id).await?;

        let apparent_tablet_state = match tablet_metadata.state {
            MetaState::Stable(state) => state,
            MetaState::Transitioning(_, next_state) => next_state,
        };

        let tablet_transfer = if let Some(transfer_id) = tablet_metadata.transfer_id {
            let transfer_metadata = snapshot.transfer_metadata(transfer_id).await?;

            Some(if transfer_metadata.dsts.contains(&tablet_id) {
                TabletTransfer::Dst {
                    srcs: transfer_metadata.srcs.clone(),
                }
            } else if transfer_metadata.srcs.contains(&tablet_id) {
                let mut dst_ranges = vec![];
                for dst_tablet_id in transfer_metadata.dsts {
                    let dst_tablet_metadata = snapshot.tablet_metadata(dst_tablet_id).await?;
                    dst_ranges.push(dst_tablet_metadata.range);
                }

                let splits = ranges_to_splits(dst_ranges)?;
                TabletTransfer::Src { splits }
            } else {
                return Err(anyhow!(
                    "{:?} is marked with {:?} but is neither src nor dst",
                    tablet_id,
                    transfer_id
                ));
            })
        } else {
            None
        };

        Ok(ShardTabletMetadata {
            colo_group_id: tablet_metadata.colo_group_id,
            range: tablet_metadata.range.clone(),
            state: apparent_tablet_state,
            transfer: tablet_transfer,
        })
    }
}

#[async_trait]
impl MetaSubscriber for ShardInner {
    async fn sync_meta(&self, sync_type: SyncType, snapshot: MetaSyncedSnapshot) {
        Retry::new()
            .indefinitely(&async || -> anyhow::Result<()> {
                self.try_sync_meta(&sync_type, &snapshot).await
            })
            .await;
    }
}

#[async_trait]
pub(crate) trait ShardJournalWriter: Send + Sync + 'static {
    async fn append(&self, entry: JournalEntry) -> anyhow::Result<()>;
}

struct ShardTabletJournalWriter {
    tablet_id: TabletId,
    inner: Arc<dyn ShardJournalWriter>,
}

impl ShardTabletJournalWriter {
    fn new(tablet_id: TabletId, inner: Arc<dyn ShardJournalWriter>) -> ShardTabletJournalWriter {
        ShardTabletJournalWriter { tablet_id, inner }
    }
}

#[async_trait]
impl TabletJournalWriter for ShardTabletJournalWriter {
    async fn append(&self, entry: TabletJournalEntry) -> anyhow::Result<()> {
        self.inner
            .append(JournalEntry {
                tablet_id: self.tablet_id,
                entry,
            })
            .await
    }
}

struct ShardTabletMetadata {
    colo_group_id: ColoGroupId,
    range: Range<Vec<u8>>,
    state: TabletState,
    transfer: Option<TabletTransfer>,
}

enum TabletTransfer {
    Src { splits: Vec<Bound<Vec<u8>>> },
    Dst { srcs: Vec<TabletId> },
}

#[async_trait]
impl<T> runtime::Tablet for WeakView<T>
where
    T: runtime::Tablet,
{
    async fn get_multi(
        &self,
        ts: Timestamp,
        keys: BTreeSet<Key>,
    ) -> Result<BTreeMap<Key, Record>, InternalError> {
        self.or_closed(async |tablet| tablet.get_multi(ts, keys).await)
            .await
    }

    async fn get_latest_multi(
        &self,
        keys: BTreeSet<Key>,
    ) -> Result<(Timestamp, BTreeMap<Key, Record>), InternalError> {
        self.or_closed(async |tablet| tablet.get_latest_multi(keys).await)
            .await
    }

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> Result<Timestamp, InternalError> {
        self.or_closed(async |tablet| tablet.latest_snapshot(keys).await)
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
        self.or_closed(async |tablet| {
            tablet
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
        self.or_closed(async |tablet| tablet.history_page(key, range, direction, limit).await)
            .await
    }

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        self.or_closed(async |tablet| tablet.write(preconds, muts).await)
            .await
    }

    async fn prepare(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        self.or_closed(async |tablet| tablet.prepare(txid, preconds, muts).await)
            .await
    }

    async fn cleanup_committed(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        self.or_closed(async |tablet| {
            tablet
                .cleanup_committed(txid, ts, precond_keys, mut_keys)
                .await
        })
        .await
    }

    async fn manifest(&self) -> anyhow::Result<Manifest> {
        self.or_closed(async |tablet| tablet.manifest().await).await
    }

    async fn wait_mostly_hydrated(&self) -> anyhow::Result<()> {
        self.or_closed(async |tablet| tablet.wait_mostly_hydrated().await)
            .await
    }

    async fn catchup(&self) -> anyhow::Result<()> {
        self.or_closed(async |tablet| tablet.catchup().await).await
    }

    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>> {
        self.or_closed(async |tablet| tablet.find_split().await)
            .await
    }
}
