use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;

use crate::meta::MetaImpl;
use crate::meta::MetaKey;
use crate::meta::MetaReader;
use crate::meta::MetaSnapshot;
use crate::meta::MetaState;
use crate::meta::TabletMetadata;
use crate::meta::TabletState;
use crate::meta::TransferMetadata;
use crate::meta::TransferState;
use crate::meta::TransferTabletTransition;
use crate::meta::Value;
use crate::obsidian::Shards;
use crate::obsidian::TabletId;
use crate::range::Range;
use crate::range::RangeSet;
use crate::tablet::Tablet;
use crate::types::Mutation;
use crate::types::ShardId;
use crate::types::TransferId;

pub(crate) struct Coordinator<T> {
    meta: Arc<MetaImpl<T>>,
    shards: Box<dyn Shards + Send + Sync>,
}

impl<T> Coordinator<T>
where
    T: Tablet + Sync + Send,
{
    pub(crate) fn new(meta: Arc<MetaImpl<T>>, shards: Box<dyn Shards + Send + Sync>) -> Self {
        Self { meta, shards }
    }

    async fn start_move(&self, src: TabletId, dst: ShardId) -> anyhow::Result<()> {
        let snapshot = self.meta.latest_snapshot_().await?;
        let src_metadata = snapshot.tablet_metadata(src).await?;

        self.start_transfer(vec![src], vec![(dst, src_metadata.range)])
            .await?;

        Ok(())
    }

    async fn start_split(&self, _src: TabletId, _dsts: Vec<ShardId>) -> anyhow::Result<()> {
        // ask tablet for ranges
        todo!();
    }

    async fn start_merge(&self, srcs: Vec<TabletId>, dst: ShardId) -> anyhow::Result<()> {
        let snapshot = self.meta.latest_snapshot_().await?;
        let mut src_range_set = RangeSet::new();
        for src in &srcs {
            let src_metadata = snapshot.tablet_metadata(*src).await?;
            src_range_set.add_range(src_metadata.range);
        }

        let dst_range = src_range_set.contiguous().ok_or_else(|| {
            anyhow!(
                "can't start merge: source tablets not contiguous: {:?} own {:?}",
                srcs,
                src_range_set
            )
        })?;

        self.start_transfer(srcs, vec![(dst, dst_range)]).await?;

        Ok(())
    }

    async fn start_transfer(
        &self,
        srcs: Vec<TabletId>,
        dsts: Vec<(ShardId, Range<Vec<u8>>)>,
    ) -> anyhow::Result<()> {
        let snapshot = self.meta.latest_snapshot_().await?;

        if !((srcs.len() == 1 && dsts.len() >= 1) || (srcs.len() >= 1 && dsts.len() == 1)) {
            return Err(anyhow!(
                "can't do m:n transfers, only 1:1 move, 1:n split, and n:1 merge"
            ));
        }

        let transfer_id = TransferId::new();

        let mut muts = HashMap::new();

        let mut src_range_set = RangeSet::new();
        let mut colo_group_id = None;
        for src in &srcs {
            let mut src_metadata = snapshot.tablet_metadata(*src).await?;

            if let Some(existing_transfer_id) = src_metadata.transfer_id {
                return Err(anyhow!(
                    "can't start transfer: {:?} already participating in {:?}",
                    src,
                    existing_transfer_id,
                ));
            }
            if let MetaState::Transitioning(_, _) = src_metadata.state {
                return Err(anyhow!(
                    "can't start transfer: {:?} already transitioning but not part of another transfer",
                    src,
                ));
            }
            src_range_set.add_range(src_metadata.range.clone());

            src_metadata.transfer_id = Some(transfer_id);
            muts.insert(
                MetaKey::Tablet(*src),
                Mutation::Put(src_metadata.clone().encode_to_vec()),
            );

            if let Some(colo_group_id) = colo_group_id {
                if src_metadata.colo_group_id != colo_group_id {
                    return Err(anyhow!(
                        "can't start transfer: not all tablets from the same colo group: {:?} != {:?}",
                        colo_group_id,
                        src_metadata.colo_group_id,
                    ));
                }
            }
            colo_group_id = Some(src_metadata.colo_group_id);
        }

        let src_range = src_range_set.contiguous().ok_or_else(|| {
            anyhow!(
                "can't start transfer: can only merge contiguous ranges, requested {:?}",
                src_range_set,
            )
        })?;

        let mut dst_tablet_ids = vec![];
        let dst_range_set = RangeSet::from_iter(dsts.iter().map(|(_, range)| range.clone()));
        let dst_range = dst_range_set.contiguous().ok_or_else(|| {
            anyhow!(
                "can't start transfer: can only split into contiguous ranges, requested {:?}",
                dst_range_set,
            )
        })?;

        if src_range != dst_range {
            return Err(anyhow!(
                "can't start transfer: source and destination are different ranges: {:?} != {:?}",
                src_range,
                dst_range,
            ));
        }

        for (dst_shard_id, dst_range) in &dsts {
            let dst_tablet_id = snapshot.next_tablet_id(*dst_shard_id).await?;
            dst_tablet_ids.push(dst_tablet_id);
            muts.insert(
                MetaKey::Tablet(dst_tablet_id),
                Mutation::Put(
                    TabletMetadata {
                        colo_group_id: colo_group_id.unwrap(),
                        range: dst_range.clone(),
                        state: MetaState::Stable(TabletState::Hydrating),
                        transfer_id: None,
                    }
                    .encode_to_vec(),
                ),
            );
        }

        muts.insert(
            MetaKey::Transfer(transfer_id),
            Mutation::Put(
                TransferMetadata {
                    state: MetaState::Stable(TransferState::Copy),
                    srcs: srcs,
                    dsts: dst_tablet_ids,
                }
                .encode_to_vec(),
            ),
        );

        self.meta.write_syncable(snapshot, muts).await?;

        // TODO: spawn a task to carry it out

        Ok(())
    }

    async fn transfer_step(&self, transfer_id: TransferId) -> anyhow::Result<bool> {
        let snapshot = self.meta.latest_snapshot_().await?;
        let transfer_metadata = snapshot.transfer_metadata(transfer_id).await?;

        let transfer_state = match transfer_metadata.state {
            MetaState::Transitioning(_, _) => {
                self.roll_transition_forward(transfer_id).await?;
                return Ok(true);
            }
            MetaState::Stable(state) => state,
        };

        match transfer_state {
            TransferState::Copy => {
                // wait for destinations to nearly finish

                self.transition_transfer(snapshot, transfer_id, TransferState::Catchup)
                    .await?;
            }
            TransferState::Catchup => {
                // wait for sources to flush and destinations to fully catch up
                //
                // compare the source manifests and destination manifests
                //
                // if something is wrong then transition to aborting

                self.transition_transfer(snapshot, transfer_id, TransferState::Synced)
                    .await?;
            }
            TransferState::Synced => {
                // No action, we just have to transit this.
                self.transition_transfer(snapshot, transfer_id, TransferState::Handoff)
                    .await?;
            }
            TransferState::Handoff => {
                // No action, we just have to transit this.
                self.transition_transfer(snapshot, transfer_id, TransferState::Complete)
                    .await?;
            }
            TransferState::Complete => return Ok(false),

            TransferState::Aborting => {
                // No action, we just have to transit this.
                self.transition_transfer(snapshot, transfer_id, TransferState::Aborted)
                    .await?;
            }
            TransferState::Aborted => return Ok(false),
        }

        Ok(true)
    }

    async fn transition_transfer<'a>(
        &'a self,
        snapshot: MetaSnapshot<'a, T>,
        transfer_id: TransferId,
        next_state: TransferState,
    ) -> anyhow::Result<()> {
        let transfer_metadata = snapshot.transfer_metadata(transfer_id).await?;

        let curr_state = match transfer_metadata.state {
            MetaState::Stable(curr) => curr,
            MetaState::Transitioning(curr, next) => {
                return Err(anyhow!(
                    "can't transition {:?}: already transitioning {:?} -> {:?}",
                    transfer_id,
                    curr,
                    next
                ));
            }
        };

        let (tablet_ids_to_transition, next_tablet_state) =
            match curr_state.tablet_transition(&next_state) {
                Some(TransferTabletTransition::Srcs(srcs_next_state)) => {
                    (&transfer_metadata.srcs, srcs_next_state)
                }
                Some(TransferTabletTransition::Dsts(dsts_next_state)) => {
                    (&transfer_metadata.dsts, dsts_next_state)
                }
                None => {
                    return Err(anyhow!(
                        "can't transition {:?}: {:?} -> {:?} not allowed by state machine",
                        transfer_id,
                        curr_state,
                        next_state
                    ));
                }
            };

        let mut muts = HashMap::new();
        let next_transfer_metadata = {
            let mut next_transfer_metadata = transfer_metadata.clone();
            next_transfer_metadata.state = MetaState::Transitioning(curr_state, next_state);
            next_transfer_metadata
        };
        muts.insert(
            MetaKey::Transfer(transfer_id),
            Mutation::Put(next_transfer_metadata.encode_to_vec()),
        );

        for tablet_id in tablet_ids_to_transition {
            let mut tablet_metadata = snapshot.tablet_metadata(*tablet_id).await?;

            let curr_tablet_state = match tablet_metadata.state {
                MetaState::Stable(curr_state) => curr_state,
                MetaState::Transitioning(_, _) => {
                    return Err(anyhow!(
                        "can't transition {:?}: participant {:?} already transitioning",
                        transfer_id,
                        tablet_id
                    ));
                }
            };

            tablet_metadata.state = MetaState::Transitioning(curr_tablet_state, next_tablet_state);

            muts.insert(
                MetaKey::Tablet(*tablet_id),
                Mutation::Put(tablet_metadata.encode_to_vec()),
            );
        }

        let _ = self.meta.write_syncable(snapshot, muts).await?;

        self.roll_transition_forward(transfer_id).await?;

        Ok(())
    }

    async fn roll_transition_forward(&self, transfer_id: TransferId) -> anyhow::Result<()> {
        let snapshot = self.meta.latest_snapshot_().await?;
        let transfer_metadata = snapshot.transfer_metadata(transfer_id).await?;

        let (curr_state, next_state) = match transfer_metadata.state {
            // Already done.
            MetaState::Stable(_) => return Ok(()),
            MetaState::Transitioning(curr_state, next_state) => (curr_state, next_state),
        };

        let (tablet_ids_to_transition, next_tablet_state) =
            match curr_state.tablet_transition(&next_state) {
                Some(TransferTabletTransition::Srcs(srcs_next_state)) => {
                    (&transfer_metadata.srcs, srcs_next_state)
                }
                Some(TransferTabletTransition::Dsts(dsts_next_state)) => {
                    (&transfer_metadata.dsts, dsts_next_state)
                }
                None => {
                    return Err(anyhow!(
                        "can't transition {:?}: {:?} -> {:?} not allowed by state machine",
                        transfer_id,
                        curr_state,
                        next_state
                    ));
                }
            };

        // IMPORTANT: Must make sure that all of the participating tablets are aware of the
        // transitioning state before continuing.
        for tablet_id in tablet_ids_to_transition {
            // This means they're at least aware of what we're seeing in `snapshot`, and
            // `write_syncable` below will fail if it turns out we were out of date.
            self.shards
                .tablet(*tablet_id)?
                .wait_meta_sync(snapshot.ts())
                .await?;
        }

        let mut muts = HashMap::new();
        let next_transfer_metadata = {
            let mut next_transfer_metadata = transfer_metadata.clone();
            next_transfer_metadata.state = MetaState::Stable(next_state);
            next_transfer_metadata
        };
        muts.insert(
            MetaKey::Transfer(transfer_id),
            Mutation::Put(next_transfer_metadata.encode_to_vec()),
        );

        for tablet_id in tablet_ids_to_transition {
            let mut tablet_metadata = snapshot.tablet_metadata(*tablet_id).await?;

            match tablet_metadata.state {
                MetaState::Stable(curr_state) => {
                    return Err(anyhow!(
                        "can't roll forward transition for {:?}: participant {:?} already stable in {:?}",
                        transfer_id,
                        tablet_id,
                        curr_state,
                    ));
                }
                MetaState::Transitioning(_, attempting_transition_to) => {
                    if next_tablet_state != attempting_transition_to {
                        return Err(anyhow!(
                            "can't roll forward transition for {:?}: participant {:?} trying to transition to {:?} instead of {:?}",
                            transfer_id,
                            tablet_id,
                            attempting_transition_to,
                            next_tablet_state,
                        ));
                    }
                }
            }

            tablet_metadata.state = MetaState::Stable(next_tablet_state);
            muts.insert(
                MetaKey::Tablet(*tablet_id),
                Mutation::Put(tablet_metadata.encode_to_vec()),
            );
        }

        let _ = self.meta.write_syncable(snapshot, muts).await?;

        Ok(())
    }
}
