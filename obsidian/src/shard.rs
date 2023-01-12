use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::anyhow;
use async_trait::async_trait;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::meta::Meta;
use crate::meta::TabletState;
use crate::range::Range;
use crate::types::ColoGroupId;
use crate::types::InternalError;
use crate::types::ShardId;
use crate::types::TabletId;
use crate::types::Timestamp;

#[async_trait]
pub(crate) trait Shard {
    async fn create_tablet(
        &self,
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
    ) -> anyhow::Result<TabletId>;

    async fn transition_tablet(
        &self,
        tablet_id: TabletId,
        new_state: TabletState,
    ) -> anyhow::Result<()>;
}

struct ShardImpl {
    shard_id: ShardId,
    meta: Arc<dyn Meta + Sync + Send>,
    inner: Arc<Mutex<ShardInner>>,
}

#[async_trait]
impl Shard for ShardImpl {
    async fn create_tablet(
        &self,
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
    ) -> anyhow::Result<TabletId> {
        let mut inner = self.inner.lock().unwrap();
        let highest_in_use = inner
            .tablets
            .first_key_value()
            .map(|(tablet_id, _)| tablet_id.1)
            .unwrap_or(0);
        let tablet_id = TabletId(self.shard_id, highest_in_use + 1);

        inner.tablets.insert(
            tablet_id,
            (
                colo_group_id,
                range,
                Timestamp::ZERO,
                TabletState::Empty,
                None,
            ),
        );

        Ok(tablet_id)
    }

    async fn transition_tablet(
        &self,
        tablet_id: TabletId,
        new_state: TabletState,
    ) -> anyhow::Result<()> {
        let handle = {
            let mut inner = self.inner.lock().unwrap();

            let (keyspace_id, range, expected_ts, curr_state, maybe_next_state) = inner
                .tablets
                .get(&tablet_id)
                .ok_or_else(|| anyhow!("{:?} not found", tablet_id))?
                .clone();

            if new_state == curr_state {
                return Ok(());
            } else if maybe_next_state == Some(new_state) {
                if let Some((_, handle)) = inner.transition_tasks.get(&tablet_id) {
                    handle.clone()
                } else {
                    self.spawn_transition(
                        &mut inner,
                        tablet_id,
                        keyspace_id,
                        range,
                        expected_ts,
                        curr_state,
                        new_state,
                    )
                }
            } else if maybe_next_state.is_some() {
                return Err(anyhow!("{:?} already transitioning", tablet_id));
            } else {
                self.spawn_transition(
                    &mut inner,
                    tablet_id,
                    keyspace_id,
                    range,
                    expected_ts,
                    curr_state,
                    new_state,
                )
            }
        };
        handle.wait().await.map_err(|e| anyhow!("{}", e))
    }
}

impl ShardImpl {
    fn spawn_transition(
        &self,
        inner: &mut ShardInner,
        tablet_id: TabletId,
        colo_group_id: ColoGroupId,
        range: Range<Vec<u8>>,
        expected_ts: Timestamp,
        curr_state: TabletState,
        new_state: TabletState,
    ) -> MaybeFilled<Result<(), String>> {
        let (sender, receiver) = watch::channel(None);
        let inner_lock = self.inner.clone();
        let meta_ = self.meta.clone();
        let handle = tokio::spawn(async move {
            let meta = meta_;

            // TODO: use retry
            loop {
                let result = meta
                    .transition(
                        tablet_id,
                        colo_group_id,
                        range.clone(),
                        expected_ts,
                        new_state,
                    )
                    .await;

                let (ts, end_state) = match result {
                    Ok(new_ts) => (new_ts, new_state),
                    Err(InternalError::TransitionRejected(_))
                    | Err(InternalError::TransitionFatal(_)) => (expected_ts, curr_state),
                    Err(_) => continue,
                };

                let mut inner = inner_lock.lock().unwrap();
                inner.transition_tasks.remove(&tablet_id);
                inner
                    .tablets
                    .insert(tablet_id, (colo_group_id, range, ts, end_state, None));

                // Errors when receiver is dropped. We don't care.
                _ = sender.send(Some(result.map(|_| ()).map_err(|e| e.to_string())));

                return;
            }
        });
        let spawn_result = MaybeFilled { r: receiver };
        inner
            .transition_tasks
            .insert(tablet_id, (handle, spawn_result.clone()));
        spawn_result
    }
}

struct ShardInner {
    transition_tasks: BTreeMap<TabletId, (JoinHandle<()>, MaybeFilled<Result<(), String>>)>,
    tablets: BTreeMap<
        TabletId,
        (
            ColoGroupId,
            Range<Vec<u8>>,
            Timestamp,
            TabletState,
            Option<TabletState>,
        ),
    >,
}

impl Drop for ShardInner {
    fn drop(&mut self) {
        for (_, (handle, _)) in &self.transition_tasks {
            handle.abort();
        }
    }
}

#[derive(Clone)]
struct MaybeFilled<T> {
    r: watch::Receiver<Option<T>>,
}

impl<T: Clone> MaybeFilled<T> {
    async fn wait(&self) -> T {
        let mut r = self.r.clone();
        loop {
            if let Some(t) = r.borrow_and_update().deref() {
                return t.clone();
            }
            // Errors when sender is dropped.
            r.changed().await.unwrap();
        }
    }
}
