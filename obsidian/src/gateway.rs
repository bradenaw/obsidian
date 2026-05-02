//! Gateway implements the public-facing API of Obsidian.

use std::cmp;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::future;
use futures::future::try_join_all;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use futures::TryStreamExt;
use rand::seq::SliceRandom;

use crate::meta::MetaReader;
use crate::meta::MetaSynced;
use crate::runtime::Meta;
use crate::runtime::Shards;
use crate::util::sleep_for_retry;
use crate::util::Retry;
use crate::util::RetryResult;
use crate::Bound;
use crate::ColoGroupId;
use crate::Direction;
use crate::InternalError;
use crate::Key;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Obsidian;
use crate::Precondition;
use crate::Range;
use crate::Record;
use crate::ShardId;
use crate::TabletId;
use crate::Timestamp;
use crate::TxOutcome;
use crate::Txid;
use crate::WriteError;

pub(crate) struct Gateway {
    meta: Arc<dyn Meta>,
    meta_synced: MetaSynced,
    shards: Arc<dyn Shards>,
}

const MAX_CONFLICT_RETRIES: usize = 10;

#[async_trait]
impl Obsidian for Gateway {
    async fn get_multi(
        &self,
        ts: Timestamp,
        keys: BTreeSet<Key>,
    ) -> anyhow::Result<BTreeMap<Key, Record>> {
        let by_tablet = self.split_keys(keys)?;
        let results = try_join_all(by_tablet.into_iter().map(
            |(tablet_id, tablet_keys)| async move {
                self.with_resolve_conflicts(|| {
                    let tablet_keys = tablet_keys.clone();
                    async move {
                        let tablet = self.shards.tablet(tablet_id)?;
                        tablet.get_multi(ts, tablet_keys).await
                    }
                })
                .await
            },
        ))
        .await?;
        Ok(results
            .into_iter()
            .map(|tablet_results| tablet_results.into_iter())
            .flatten()
            .collect())
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<Record>, Option<Range<Vec<u8>>>)> {
        self.with_resolve_conflicts(|| async move {
            let start_bound = match direction {
                Direction::Asc => range.lower,
                Direction::Desc => range.upper,
            };
            let tablet_id =
                self.meta_synced
                    .tablet_id_for_bound(keyspace_id.0, start_bound, direction)?;

            let tablet = self.shards.tablet(tablet_id)?;
            tablet
                .scan_page(ts, keyspace_id, range, direction, limit)
                .await
        })
        .await
    }

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> anyhow::Result<Timestamp> {
        let by_tablet = self.split_keys(keys)?;
        let mut futures = FuturesUnordered::new();
        for (tablet_id, keys) in by_tablet.into_iter() {
            // TODO: with a little more information, we could get away with at most *one* round of
            // conflict resolution.
            futures.push(self.with_resolve_conflicts(move || {
                let tablet = self.shards.tablet(tablet_id);
                let keys = keys.clone();
                async move { tablet?.latest_snapshot(keys).await }
            }));
        }
        let mut result = Timestamp::ZERO;
        while let Some(ts) = futures.try_next().await? {
            result = cmp::max(ts, result);
        }

        Ok(result)
    }

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, WriteError> {
        if muts.is_empty() {
            return Err(anyhow!("empty write").into());
        }

        let write_by_tablet = self.split_write(preconds.clone(), muts.clone())?;

        let owner_shard_id =
            choose_shard(write_by_tablet.keys().copied()).ok_or_else(|| anyhow!("empty write"))?;

        let mut txid = Txid::new(owner_shard_id);

        if write_by_tablet.len() == 1 {
            let (tablet_id, (preconds, muts)) = write_by_tablet.into_iter().next().unwrap();
            return self
                .with_resolve_conflicts(|| {
                    let preconds = preconds.clone();
                    let muts = muts.clone();
                    async move { self.shards.tablet(tablet_id)?.write(preconds, muts).await }
                })
                .await
                .map_err(|e| {
                    match e.downcast_ref::<InternalError>() {
                        Some(InternalError::PreconditionFailed) => {
                            return WriteError::PreconditionFailed;
                        }
                        _ => {}
                    }
                    e.into()
                });
        }

        let mut already_seen_conflicts = HashSet::new();
        let mut any_prepare_succeeded = false;
        for i in 0..MAX_CONFLICT_RETRIES {
            if i != 0 {
                sleep_for_retry(
                    i as usize,
                    Duration::from_millis(10),
                    Duration::from_millis(5000),
                )
                .await;
            }

            match self
                .write_2pc_one_try(
                    txid,
                    &write_by_tablet,
                    &mut already_seen_conflicts,
                    &mut any_prepare_succeeded,
                )
                .await
            {
                Ok(TxOutcome::Committed(commit_ts)) => return Ok(commit_ts),
                Ok(TxOutcome::Aborted) => {
                    txid = txid.next();
                    continue;
                }
                Err(e) => {
                    // If any prepare succeeded then there might be other operations waiting for
                    // this abort. It's a little wasteful for them to have to wait out the abort
                    // timeout since we are going to abandon this transaction.
                    if any_prepare_succeeded {
                        // Don't return any errors from doing this because we want to surface `e`,
                        // the actual cause.
                        if let Ok(owner) = self.shards.shard(txid.owner) {
                            let _ = owner.tx_try_abort(txid).await;
                        }
                    }
                    return Err(e);
                }
            }
        }
        Err(WriteError::Other(anyhow::anyhow!("too much contention")))
    }

    async fn create_colo_group(
        &self,
        colo_group_id: ColoGroupId,
        initial_splits: Vec<Bound<Vec<u8>>>,
    ) -> anyhow::Result<()> {
        self.meta
            .create_colo_group(colo_group_id, initial_splits)
            .await?;

        // TODO: if the colo group already exists, we want to still sync_meta here, so that the
        // caller doesn't get an "already exists" and then try to use it and immediately get back a
        // "doesn't exist" because that node hasn't learned about it yet.

        self.sync_meta().await?;

        Ok(())
    }

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        self.meta.create_keyspace(keyspace_id).await?;

        self.sync_meta().await?;

        Ok(())
    }
}

fn choose_shard<I: Iterator<Item = TabletId>>(iter: I) -> Option<ShardId> {
    let mut shard_ids: Vec<_> = iter.map(|tablet_id| tablet_id.0).collect();

    shard_ids.sort();
    shard_ids.dedup();

    shard_ids.choose(&mut rand::thread_rng()).copied()
}

impl Gateway {
    pub(crate) fn new(
        meta: Arc<dyn Meta>,
        meta_synced: MetaSynced,
        shards: Arc<dyn Shards>,
    ) -> Self {
        Self {
            meta,
            meta_synced,
            shards,
        }
    }

    fn split_keys(&self, keys: BTreeSet<Key>) -> anyhow::Result<BTreeMap<TabletId, BTreeSet<Key>>> {
        let mut by_tablet = BTreeMap::new();
        for (keyspace_id, key) in &keys {
            let tablet_id = self.meta_synced.tablet_id_for_key(keyspace_id.0, &key)?;
            by_tablet
                .entry(tablet_id)
                .or_insert_with(BTreeSet::new)
                .insert((*keyspace_id, key.clone()));
        }
        Ok(by_tablet)
    }

    fn split_write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> anyhow::Result<BTreeMap<TabletId, (Vec<Precondition>, BTreeMap<Key, Mutation>)>> {
        let mut result = BTreeMap::new();

        for precond in preconds {
            let tablet_id = self
                .meta_synced
                .tablet_id_for_key(precond.keyspace_id().0, precond.key())?;

            result
                .entry(tablet_id)
                .or_insert_with(|| (vec![], BTreeMap::new()))
                .0
                .push(precond);
        }
        for (key, m) in muts {
            let tablet_id = self.meta_synced.tablet_id_for_key(key.0 .0, &key.1)?;
            result
                .entry(tablet_id)
                .or_insert_with(|| (vec![], BTreeMap::new()))
                .1
                .insert(key, m);
        }

        Ok(result)
    }

    async fn with_resolve_conflicts<F, Fut, T>(&self, f: F) -> anyhow::Result<T>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, InternalError>>,
    {
        // We can use an 'arbitrary' txid here even with a nonexistent tablet because this
        // never surfaces anywhere else, we just use it to decide if we can preempt other
        // transactions.
        let txid = Txid::new(ShardId(0));

        let already_seen_conflicts = Mutex::new(HashSet::new());

        Retry::new()
            .n_attempts(MAX_CONFLICT_RETRIES + 1)
            .with_retry(|| async {
                match f().await {
                    Ok(v) => return RetryResult::Ok(v),
                    Err(InternalError::Conflict(other_txid)) => {
                        // If we've already seen this txid as a conflict that means we already
                        // wait/aborted it and we're still just waiting for it to get cleaned up,
                        // so just take another turn around the retry loop.
                        if already_seen_conflicts.lock().unwrap().contains(&other_txid) {
                            return RetryResult::Retry(InternalError::Conflict(other_txid));
                        }

                        let other_txid_owner_shard = match self.shards.shard(other_txid.owner) {
                            Ok(shard) => shard,
                            Err(e) => {
                                return RetryResult::Err(InternalError::Other(e));
                            }
                        };
                        if txid.can_preempt(&other_txid) {
                            log::debug!("{:?} preempting {:?}", txid, other_txid);
                            if let Err(e) = other_txid_owner_shard.tx_try_abort(other_txid).await {
                                return RetryResult::Err(InternalError::Other(e));
                            }
                        } else {
                            log::debug!("{:?} waiting for {:?}", txid, other_txid);
                            match other_txid_owner_shard.tx_wait(other_txid).await {
                                // TxOutcomeMissing happens if we raced with the cleanup that
                                // removed it, but that means the pending/preconds are gone so
                                // retrying will work.
                                Ok(_) | Err(InternalError::TxOutcomeMissing) => {}
                                Err(e) => {
                                    return RetryResult::Err(e.into());
                                }
                            }
                        }

                        already_seen_conflicts.lock().unwrap().insert(other_txid);
                        RetryResult::Retry(InternalError::Conflict(other_txid))
                    }
                    Err(e) => {
                        return RetryResult::Err(e.into());
                    }
                }
            })
            .await
    }

    async fn wait_all(&self, txids: &BTreeSet<Txid>) -> Result<(), InternalError> {
        let mut futures = FuturesUnordered::new();
        for txid in txids {
            futures.push(async move {
                let shard = self.shards.shard(txid.owner)?;
                shard.tx_wait(*txid).await
            });
        }
        while let Some(result) = futures.next().await {
            match result {
                // TxOutcomeMissing happens when the cleanup has already finished before we get
                // there, which means we got what we wanted.
                Ok(_) | Err(InternalError::TxOutcomeMissing) => {}
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    async fn write_2pc_one_try(
        &self,
        txid: Txid,
        write_by_tablet: &BTreeMap<
            TabletId,
            (Vec<Precondition>, BTreeMap<(KeyspaceId, Vec<u8>), Mutation>),
        >,
        already_seen_conflicts: &mut HashSet<Txid>,
        any_prepare_succeeded: &mut bool,
    ) -> Result<TxOutcome, WriteError> {
        let mut pending_tablets: BTreeSet<_> = write_by_tablet.keys().collect();
        let mut max_prepare_ts = Timestamp::ZERO;
        let tablets = write_by_tablet
            .keys()
            .copied()
            .chain(std::iter::once(TabletId::shard_meta(txid.owner)))
            .map(|tablet_id| {
                self.shards
                    .tablet(tablet_id)
                    .map(|tablet| (tablet_id, tablet))
            })
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;

        let mut j = 0;
        while !pending_tablets.is_empty() {
            let mut prepare_futures = Vec::with_capacity(write_by_tablet.len());
            for tablet_id in &pending_tablets {
                let (tablet_preconds, tablet_muts) = write_by_tablet.get(tablet_id).unwrap();
                let tablet_id = *tablet_id;
                let tablet = tablets.get(&tablet_id).unwrap();
                prepare_futures.push(async move {
                    (
                        tablet_id,
                        tablet
                            .prepare(txid, tablet_preconds.to_vec(), tablet_muts.clone())
                            .await,
                    )
                });
            }
            let prepare_results = future::join_all(prepare_futures).await;
            let mut preempt_conflicts = BTreeSet::new();
            let mut wait_conflicts = BTreeSet::new();
            let mut saw_an_already_seen = false;
            *any_prepare_succeeded =
                *any_prepare_succeeded || prepare_results.iter().any(|(_, result)| result.is_ok());
            for (tablet_id, prepare_result) in prepare_results {
                match prepare_result {
                    Ok(prepare_ts) => {
                        pending_tablets.remove(&tablet_id);
                        max_prepare_ts = cmp::max(max_prepare_ts, prepare_ts);
                    }
                    Err(InternalError::Conflict(other_txid)) => {
                        if already_seen_conflicts.contains(&other_txid) {
                            saw_an_already_seen = true;
                        } else if txid.can_preempt(&other_txid) {
                            preempt_conflicts.insert(other_txid);
                        } else {
                            wait_conflicts.insert(other_txid);
                        }
                    }
                    // TODO: If any of the prepares succeeded we should go ahead and abort
                    // ourselves so that we don't have other transactions waiting around for
                    // the timeout.
                    Err(e) => return Err(WriteError::Other(e.into())),
                }
            }
            if !wait_conflicts.is_empty() {
                self.wait_all(&wait_conflicts)
                    .await
                    .map_err(|e| WriteError::Other(e.into()))?;
                for other_txid in wait_conflicts {
                    already_seen_conflicts.insert(other_txid);
                }
            }
            if !preempt_conflicts.is_empty() {
                future::try_join_all(preempt_conflicts.iter().cloned().map(
                    |other_txid| async move {
                        let shard = self.shards.shard(other_txid.owner)?;
                        shard.tx_try_abort(other_txid).await
                    },
                ))
                .await
                .map_err(|e| WriteError::Other(e.into()))?;
                for other_txid in preempt_conflicts {
                    already_seen_conflicts.insert(other_txid);
                }
            }
            if saw_an_already_seen {
                sleep_for_retry(j, Duration::from_millis(10), Duration::from_millis(5000)).await;
            }
            j += 1
        }
        // We have to commit at a _higher_ timestamp so that the resolution of the pending
        // records is at a higher timestamp than the pending records themselves.
        let commit_ts = max_prepare_ts.plus_one();

        let precond_keys: BTreeSet<_> = write_by_tablet
            .values()
            .map(|(preconds, _)| {
                preconds
                    .iter()
                    .map(|precond| (precond.keyspace_id(), precond.key().to_vec()))
            })
            .flatten()
            .collect();

        let mut_keys: BTreeSet<_> = write_by_tablet
            .values()
            .map(|(_, muts)| muts.keys())
            .flatten()
            .cloned()
            .collect();

        Ok(self
            .shards
            .shard(txid.owner)?
            .tx_try_commit(txid, commit_ts, precond_keys, mut_keys)
            .await?)
    }

    async fn sync_meta(&self) -> anyhow::Result<()> {
        let ts = self.meta.latest_snapshot().await?;

        self.meta_synced.wait(ts).await?;

        let snapshot = self.meta_synced.snapshot();
        let shard_ids = snapshot.shard_ids().await?;

        for shard_id in &shard_ids {
            self.shards.shard(*shard_id)?.wait_meta_sync(ts).await?;
        }

        let tablet_ids = {
            let mut tablet_ids = snapshot.tablet_ids().await?;
            tablet_ids.shuffle(&mut rand::thread_rng());
            tablet_ids
        };

        log::info!("sync_meta() to {:?} for {:?} tablets", ts, tablet_ids.len());

        futures::stream::iter(shard_ids.into_iter())
            .map(|shard_id| async move {
                self.shards.shard(shard_id)?.wait_meta_sync(ts).await?;
                Ok::<_, anyhow::Error>(())
            })
            .buffer_unordered(64)
            .try_collect::<Vec<_>>()
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::test::obsidian_test_suite;

    obsidian_test_suite!({
        use std::sync::Arc;

        use crate::test::ObsidianForTestBuilder;

        async || {
            let obs = ObsidianForTestBuilder::new().n_shards(2).build().await?;
            Ok::<_, anyhow::Error>(Arc::clone(&obs.gateway))
        }
    });
}
