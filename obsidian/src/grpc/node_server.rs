use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use async_stream::stream;
use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;

use crate::grpc::util::get_req_results;
use crate::grpc::util::internal;
use crate::grpc::util::internal_err_from_status;
use crate::grpc::util::internal_err_to_status;
use crate::grpc::util::invalid_argument;
use crate::grpc::util::key_set;
use crate::grpc::util::parse_preconds_muts;
use crate::grpc::util::parse_scan_req;
use crate::grpc::util::required;
use crate::meta::MetaKey;
use crate::meta::MetaMutation;
use crate::meta::MetaValue;
use crate::pb;
use crate::runtime::Node;
use crate::runtime::ReplicaState;
use crate::util::hexlify;
use crate::Bound;
use crate::ColoGroupId;
use crate::Direction;
use crate::InternalError;
use crate::Key;
use crate::KeyspaceId;
use crate::Mutation;
use crate::NodeId;
use crate::Obsidian;
use crate::Precondition;
use crate::Range;
use crate::ShardId;
use crate::TabletId;
use crate::Timestamp;
use crate::Txid;

pub(crate) struct NodeServer {
    node: Arc<dyn Node>,
}

impl NodeServer {
    pub fn new(node: Arc<dyn Node>) -> Self {
        Self { node }
    }

    fn check_node_id(
        &self,
        maybe_node_id_pb: Option<pb::internal::NodeId>,
    ) -> Result<(), tonic::Status> {
        let node_id: NodeId = required("node_id", maybe_node_id_pb)?;
        if node_id != self.node.id() {
            return Err(tonic::Status::not_found(format!(
                "request for {:?} arrived at {:?}",
                node_id,
                self.node.id()
            )));
        }

        Ok(())
    }
}

#[async_trait]
impl pb::internal::node_server::Node for NodeServer {
    async fn tablet_get(
        &self,
        req: tonic::Request<pb::internal::TabletGetReq>,
    ) -> Result<tonic::Response<pb::GetResp>, tonic::Status> {
        let req_inner = req.into_inner();
        let tablet_id: TabletId = required("tablet_id", req_inner.tablet_id)?;
        let tablet = self.node.tablet(tablet_id).map_err(internal)?;
        let get_req = req_inner
            .inner
            .ok_or_else(|| invalid_argument(anyhow!("missing inner")))?;

        let (keys, results_builder) = get_req_results(get_req.keys).map_err(invalid_argument)?;
        let ts = Timestamp::from_micros(get_req.snapshot_ts);

        let records = tablet
            .get_multi(ts, keys)
            .await
            .map_err(internal_err_to_status)?;

        Ok(tonic::Response::new(pb::GetResp {
            results: results_builder.build(records).map_err(internal)?,
        }))
    }

    async fn tablet_scan_page(
        &self,
        req: tonic::Request<pb::internal::TabletScanPageReq>,
    ) -> Result<tonic::Response<pb::ScanResp>, tonic::Status> {
        let req_inner = req.into_inner();
        let tablet_id: TabletId = required("tablet_id", req_inner.tablet_id)?;
        let tablet = self.node.tablet(tablet_id).map_err(internal)?;
        let scan_req = req_inner
            .inner
            .ok_or_else(|| invalid_argument(anyhow!("missing inner")))?;

        let (snapshot_ts, keyspace_id, range, direction, limit) =
            parse_scan_req(scan_req).map_err(invalid_argument)?;

        let (records, maybe_continue_range) = tablet
            .scan_page(snapshot_ts, keyspace_id, range.borrow(), direction, limit)
            .await
            .map_err(internal_err_to_status)?;

        Ok(tonic::Response::new(pb::ScanResp {
            records: records.into_iter().map(pb::Record::from).collect(),
            remaining: maybe_continue_range.map(Range::into),
        }))
    }

    async fn tablet_get_latest(
        &self,
        req: tonic::Request<pb::internal::TabletGetLatestReq>,
    ) -> Result<tonic::Response<pb::GetLatestResp>, tonic::Status> {
        let req_inner = req.into_inner();
        let tablet_id: TabletId = required("tablet_id", req_inner.tablet_id)?;
        let tablet = self.node.tablet(tablet_id).map_err(internal)?;
        let get_req = req_inner
            .inner
            .ok_or_else(|| invalid_argument(anyhow!("missing inner")))?;

        let (keys, results_builder) = get_req_results(get_req.keys).map_err(invalid_argument)?;

        let (snapshot_ts, records) = tablet
            .get_latest_multi(keys)
            .await
            .map_err(internal_err_to_status)?;

        Ok(tonic::Response::new(pb::GetLatestResp {
            snapshot_ts: snapshot_ts.as_micros(),
            results: results_builder.build(records).map_err(internal)?,
        }))
    }

    async fn tablet_latest_snapshot(
        &self,
        req: tonic::Request<pb::internal::TabletGetLatestReq>,
    ) -> Result<tonic::Response<pb::LatestSnapshotResp>, tonic::Status> {
        let req_inner = req.into_inner();
        let tablet_id: TabletId = required("tablet_id", req_inner.tablet_id)?;
        let tablet = self.node.tablet(tablet_id).map_err(internal)?;
        let get_req = req_inner
            .inner
            .ok_or_else(|| invalid_argument(anyhow!("missing inner")))?;

        let (keys, _) = get_req_results(get_req.keys).map_err(invalid_argument)?;

        let snapshot_ts = tablet
            .latest_snapshot(keys)
            .await
            .map_err(internal_err_to_status)?;

        Ok(tonic::Response::new(pb::LatestSnapshotResp {
            snapshot_ts: snapshot_ts.as_micros(),
        }))
    }

    async fn tablet_write(
        &self,
        req: tonic::Request<pb::internal::TabletWriteReq>,
    ) -> Result<tonic::Response<pb::WriteResp>, tonic::Status> {
        let req_inner = req.into_inner();
        let tablet_id: TabletId = required("tablet_id", req_inner.tablet_id)?;
        let tablet = self.node.tablet(tablet_id).map_err(internal)?;
        let write_req = req_inner
            .inner
            .ok_or_else(|| invalid_argument(anyhow!("missing inner")))?;

        let (preconds, muts) =
            parse_preconds_muts(write_req.preconds, write_req.muts).map_err(invalid_argument)?;

        let ts = tablet
            .write(preconds, muts)
            .await
            .map_err(internal_err_to_status)?;

        Ok(tonic::Response::new(pb::WriteResp {
            write_ts: ts.as_micros(),
        }))
    }

    async fn tablet_prepare(
        &self,
        req: tonic::Request<pb::internal::TabletPrepareReq>,
    ) -> Result<tonic::Response<pb::internal::TabletPrepareResp>, tonic::Status> {
        let req_inner = req.into_inner();
        let tablet_id: TabletId = required("tablet_id", req_inner.tablet_id)?;
        let tablet = self.node.tablet(tablet_id).map_err(internal)?;

        let txid: Txid = required("txid", req_inner.txid)?;

        let (preconds, muts) =
            parse_preconds_muts(req_inner.preconds, req_inner.muts).map_err(invalid_argument)?;

        let ts = tablet
            .prepare(txid, preconds, muts)
            .await
            .map_err(internal_err_to_status)?;

        Ok(tonic::Response::new(pb::internal::TabletPrepareResp {
            prepare_ts: ts.as_micros(),
        }))
    }

    async fn tablet_try_commit(
        &self,
        req: tonic::Request<pb::internal::TabletTryCommitReq>,
    ) -> Result<tonic::Response<pb::internal::TxOutcomeResp>, tonic::Status> {
        let req_inner = req.into_inner();
        let tablet_id: TabletId = required("tablet_id", req_inner.tablet_id)?;
        let tablet = self.node.tablet(tablet_id).map_err(internal)?;

        let txid: Txid = required("txid", req_inner.txid)?;
        let commit_ts = Timestamp::from_micros(req_inner.ts);
        let precond_keys = key_set(req_inner.precond_keys)?;
        let mut_keys = key_set(req_inner.mut_keys)?;

        let tx_outcome = tablet
            .try_commit(txid, commit_ts, precond_keys, mut_keys)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(pb::internal::TxOutcomeResp {
            tx_outcome: Some(pb::internal::TxOutcome::from(tx_outcome)),
        }))
    }

    async fn tablet_try_abort(
        &self,
        req: tonic::Request<pb::internal::TabletTxidReq>,
    ) -> Result<tonic::Response<pb::internal::TxOutcomeResp>, tonic::Status> {
        let req_inner = req.into_inner();
        let tablet_id: TabletId = required("tablet_id", req_inner.tablet_id)?;
        let tablet = self.node.tablet(tablet_id).map_err(internal)?;

        let txid: Txid = required("txid", req_inner.txid)?;

        let tx_outcome = tablet
            .try_abort(txid)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(pb::internal::TxOutcomeResp {
            tx_outcome: Some(pb::internal::TxOutcome::from(tx_outcome)),
        }))
    }

    async fn tablet_wait(
        &self,
        req: tonic::Request<pb::internal::TabletTxidReq>,
    ) -> Result<tonic::Response<pb::internal::TxOutcomeResp>, tonic::Status> {
        let req_inner = req.into_inner();
        let tablet_id: TabletId = required("tablet_id", req_inner.tablet_id)?;
        let tablet = self.node.tablet(tablet_id).map_err(internal)?;

        let txid: Txid = required("txid", req_inner.txid)?;

        let tx_outcome = tablet
            .wait(txid)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(pb::internal::TxOutcomeResp {
            tx_outcome: Some(pb::internal::TxOutcome::from(tx_outcome)),
        }))
    }

    async fn tablet_cleanup_committed(
        &self,
        req: tonic::Request<pb::internal::TabletCleanupCommittedReq>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let req_inner = req.into_inner();
        let tablet_id: TabletId = required("tablet_id", req_inner.tablet_id)?;
        let tablet = self.node.tablet(tablet_id).map_err(internal)?;

        let txid: Txid = required("txid", req_inner.txid)?;
        let commit_ts = Timestamp::from_micros(req_inner.ts);
        let precond_keys = key_set(req_inner.precond_keys)?;
        let mut_keys = key_set(req_inner.mut_keys)?;

        tablet
            .cleanup_committed(txid, commit_ts, precond_keys, mut_keys)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(()))
    }

    async fn tablet_wait_meta_sync(
        &self,
        req: tonic::Request<pb::internal::TabletWaitMetaSyncReq>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let req_inner = req.into_inner();
        let tablet_id: TabletId = required("tablet_id", req_inner.tablet_id)?;
        let tablet = self.node.tablet(tablet_id).map_err(internal)?;

        let ts = Timestamp::from_micros(req_inner.ts);

        tablet
            .wait_meta_sync(ts)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(()))
    }

    async fn tablet_wait_mostly_hydrated(
        &self,
        req: tonic::Request<pb::internal::TabletEmptyReq>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let req_inner = req.into_inner();
        let tablet_id: TabletId = required("tablet_id", req_inner.tablet_id)?;
        let tablet = self.node.tablet(tablet_id).map_err(internal)?;

        tablet
            .wait_mostly_hydrated()
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(()))
    }

    async fn tablet_catchup(
        &self,
        req: tonic::Request<pb::internal::TabletEmptyReq>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let req_inner = req.into_inner();
        let tablet_id: TabletId = required("tablet_id", req_inner.tablet_id)?;
        let tablet = self.node.tablet(tablet_id).map_err(internal)?;

        tablet
            .catchup()
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(()))
    }

    async fn tablet_manifest(
        &self,
        req: tonic::Request<pb::internal::TabletEmptyReq>,
    ) -> Result<tonic::Response<pb::internal::TabletManifestResp>, tonic::Status> {
        let req_inner = req.into_inner();
        let tablet_id: TabletId = required("tablet_id", req_inner.tablet_id)?;
        let tablet = self.node.tablet(tablet_id).map_err(internal)?;

        let manifest = tablet
            .manifest()
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(pb::internal::TabletManifestResp {
            manifest: Some(manifest.into()),
        }))
    }

    async fn tablet_find_split(
        &self,
        req: tonic::Request<pb::internal::TabletEmptyReq>,
    ) -> Result<tonic::Response<pb::internal::TabletFindSplitResp>, tonic::Status> {
        let req_inner = req.into_inner();
        let tablet_id: TabletId = required("tablet_id", req_inner.tablet_id)?;
        let tablet = self.node.tablet(tablet_id).map_err(internal)?;

        let bound = tablet
            .find_split()
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(pb::internal::TabletFindSplitResp {
            bound: Some(bound.into()),
        }))
    }

    async fn shard_wait_meta_sync(
        &self,
        req: tonic::Request<pb::internal::ShardWaitMetaSyncReq>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let req_inner = req.into_inner();
        let shard_id = ShardId(req_inner.shard_id);
        let ts = Timestamp::from_micros(req_inner.ts);
        let shard = self.node.shard(shard_id).map_err(internal)?;

        shard.wait_meta_sync(ts).await.map_err(internal)?;

        Ok(tonic::Response::new(()))
    }

    async fn meta_add_shard(
        &self,
        req: tonic::Request<pb::internal::ShardIdReq>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let req_inner = req.into_inner();
        let shard_id = ShardId(req_inner.shard_id);

        self.node
            .meta()
            .map_err(|e| tonic::Status::failed_precondition(e.to_string()))?
            .add_shard(shard_id)
            .await
            .map_err(internal)?;

        Ok(tonic::Response::new(()))
    }

    async fn meta_add_node(
        &self,
        req: tonic::Request<pb::internal::NodeIdReq>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let req_inner = req.into_inner();
        let node_id: NodeId = required("node_id", req_inner.node_id)?;

        self.node
            .meta()
            .map_err(|e| tonic::Status::failed_precondition(e.to_string()))?
            .add_node(node_id)
            .await
            .map_err(internal)?;

        Ok(tonic::Response::new(()))
    }

    async fn meta_create_colo_group(
        &self,
        req: tonic::Request<pb::CreateColoGroupReq>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let req_inner = req.into_inner();
        let colo_group_id = ColoGroupId(req_inner.colo_group_id);
        let initial_splits = req_inner
            .initial_splits
            .into_iter()
            .map(Bound::try_from)
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(invalid_argument)?;

        self.node
            .meta()
            .map_err(|e| tonic::Status::failed_precondition(e.to_string()))?
            .create_colo_group(colo_group_id, initial_splits)
            .await
            .map_err(internal)?;

        Ok(tonic::Response::new(()))
    }

    async fn meta_create_keyspace(
        &self,
        req: tonic::Request<pb::CreateKeyspaceReq>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let req_inner = req.into_inner();
        let keyspace_id: KeyspaceId = required("keyspace_id", req_inner.keyspace_id)?;

        self.node
            .meta()
            .map_err(|e| tonic::Status::failed_precondition(e.to_string()))?
            .create_keyspace(keyspace_id)
            .await
            .map_err(internal)?;

        Ok(tonic::Response::new(()))
    }

    async fn meta_latest_snapshot(
        &self,
        _req: tonic::Request<()>,
    ) -> Result<tonic::Response<pb::internal::Timestamp>, tonic::Status> {
        let ts = self
            .node
            .meta()
            .map_err(|e| tonic::Status::failed_precondition(e.to_string()))?
            .latest_snapshot()
            .await
            .map_err(internal)?;

        Ok(tonic::Response::new(pb::internal::Timestamp {
            ts: ts.as_micros(),
        }))
    }

    async fn meta_wait_for_newer(
        &self,
        req: tonic::Request<pb::internal::Timestamp>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let req_inner = req.into_inner();
        let ts = Timestamp::from_micros(req_inner.ts);

        self.node
            .meta()
            .map_err(|e| tonic::Status::failed_precondition(e.to_string()))?
            .wait_for_newer(ts)
            .await
            .map_err(internal)?;

        Ok(tonic::Response::new(()))
    }

    async fn meta_scan_page(
        &self,
        req: tonic::Request<pb::internal::MetaScanPageReq>,
    ) -> Result<tonic::Response<pb::internal::MetaScanPageResp>, tonic::Status> {
        let req_inner = req.into_inner();
        let ts = Timestamp::from_micros(req_inner.ts);
        let range: Range<Vec<u8>> = required("range", req_inner.range)?;

        let (page, maybe_remaining) = self
            .node
            .meta()
            .map_err(|e| tonic::Status::failed_precondition(e.to_string()))?
            .scan_page(ts, range)
            .await
            .map_err(internal)?;

        Ok(tonic::Response::new(pb::internal::MetaScanPageResp {
            records: page.into_iter().map(pb::Record::from).collect(),
            remaining: maybe_remaining.map(|range| range.into()),
        }))
    }

    async fn meta_sync(
        &self,
        req: tonic::Request<pb::internal::Timestamp>,
    ) -> Result<tonic::Response<pb::internal::MetaSyncResp>, tonic::Status> {
        let req_inner = req.into_inner();
        let ts = Timestamp::from_micros(req_inner.ts);

        let (page, continue_ts) = self
            .node
            .meta()
            .map_err(|e| tonic::Status::failed_precondition(e.to_string()))?
            .sync(ts)
            .await
            .map_err(internal)?;

        Ok(tonic::Response::new(pb::internal::MetaSyncResp {
            revisions: page.into_iter().map(pb::Revision::from).collect(),
            ts: continue_ts.as_micros(),
        }))
    }

    async fn meta_write(
        &self,
        req: tonic::Request<pb::internal::MetaWriteReq>,
    ) -> Result<tonic::Response<pb::internal::Timestamp>, tonic::Status> {
        let req_inner = req.into_inner();
        let snapshot_ts = Timestamp::from_micros(req_inner.snapshot_ts);
        let mut mutations = HashMap::new();
        for meta_key_mut_pb in req_inner.mutations {
            let raw_key = meta_key_mut_pb.key;
            let key = MetaKey::decode(&raw_key).map_err(invalid_argument)?;
            if mutations.contains_key(&key) {
                return Err(tonic::Status::invalid_argument(format!(
                    "duplicate key in meta write [{}]",
                    hexlify(&raw_key)
                )));
            }

            let mutation = Mutation::try_from(meta_key_mut_pb.mutation.ok_or_else(|| {
                tonic::Status::invalid_argument(format!("missing mutation on MetaKeyMutation"))
            })?)
            .map_err(invalid_argument)?;
            let meta_mutation = match mutation {
                Mutation::Put(raw_value) => {
                    MetaMutation::Put(MetaValue::decode(&raw_value[..]).map_err(invalid_argument)?)
                }
                Mutation::Delete => MetaMutation::Delete,
            };

            mutations.insert(key, meta_mutation);
        }

        let ts = self
            .node
            .meta()
            .map_err(|e| tonic::Status::failed_precondition(e.to_string()))?
            .write(snapshot_ts, mutations)
            .await
            .map_err(internal_err_to_status)?;

        Ok(tonic::Response::new(pb::internal::Timestamp {
            ts: ts.as_micros(),
        }))
    }

    type ShardsSubscribeStream = Box<
        dyn Stream<Item = Result<pb::internal::ShardsSubscribeResp, tonic::Status>>
            + Send
            + Unpin
            + 'static,
    >;

    async fn shards_subscribe(
        &self,
        _req: tonic::Request<()>,
    ) -> Result<tonic::Response<Self::ShardsSubscribeStream>, tonic::Status> {
        Ok(tonic::Response::new(Box::new(Box::pin(
            shards_subscribe_owned(Arc::clone(&self.node)).map(|maybe_shards| {
                let shards = match maybe_shards {
                    Ok(shards) => shards,
                    Err(e) => return Err(tonic::Status::internal(e.to_string())),
                };

                let shards_pb = pb::internal::ShardsSubscribeResp {
                    shards: shard_states_to_pb(shards),
                };

                Ok(shards_pb)
            }),
        ))
            as Self::ShardsSubscribeStream))
    }
}

fn shard_states_to_pb(
    shards: HashMap<ShardId, ReplicaState>,
) -> Vec<pb::internal::ShardReplicaState> {
    shards
        .into_iter()
        .map(
            |(shard_id, replica_state)| pb::internal::ShardReplicaState {
                shard_id: shard_id.0,
                replica_state: Some(pb::internal::ReplicaState {
                    replica_state: Some(match replica_state {
                        ReplicaState::Leader(seq) => {
                            pb::internal::replica_state::ReplicaState::Leader(seq.0)
                        }
                        ReplicaState::Follower => {
                            pb::internal::replica_state::ReplicaState::Follower(())
                        }
                    }),
                }),
            },
        )
        .collect()
}

fn shards_subscribe_owned(
    node: Arc<dyn Node>,
) -> Box<dyn Stream<Item = anyhow::Result<HashMap<ShardId, ReplicaState>>> + Send + Unpin + 'static>
{
    Box::new(Box::pin(stream! {
        let mut s = node.shards_subscribe();
        while let Some(shards) = s.next().await {
            yield shards;
        }
    }))
}
