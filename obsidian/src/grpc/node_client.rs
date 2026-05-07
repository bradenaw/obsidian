use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;
use obsidian_common::key_to_proto;
use obsidian_pb as pb;
use obsidian_util::Retry;

use crate::grpc::util::internal_err_from_status;
use crate::grpc::util::preconds_muts_to_proto;
use crate::grpc::util::scan_req_to_proto;
use crate::grpc::util::Pool;
use crate::meta::MetaKey;
use crate::meta::MetaMutation;
use crate::runtime;
use crate::runtime::Meta;
use crate::runtime::ReplicaState;
use crate::runtime::Shard;
use crate::runtime::Supervisor;
use crate::runtime::Tablet;
use crate::Bound;
use crate::ColoGroupId;
use crate::Direction;
use crate::HistoryRange;
use crate::InternalError;
use crate::JournalSeq;
use crate::Key;
use crate::KeyspaceId;
use crate::Manifest;
use crate::Mutation;
use crate::NodeId;
use crate::Precondition;
use crate::Range;
use crate::Record;
use crate::Revision;
use crate::ShardId;
use crate::TabletId;
use crate::Timestamp;
use crate::TxOutcome;
use crate::Txid;

pub(crate) struct NodeClient {
    node_id: NodeId,
    grpc_client: pb::internal::node_client::NodeClient<tonic::transport::Channel>,
}

impl NodeClient {
    pub fn new(
        node_id: NodeId,
        grpc_client: pb::internal::node_client::NodeClient<tonic::transport::Channel>,
    ) -> Self {
        Self {
            node_id,
            grpc_client,
        }
    }

    fn shards_subscribe_inner(
        grpc_client: pb::internal::node_client::NodeClient<tonic::transport::Channel>,
    ) -> impl Stream<Item = anyhow::Result<HashMap<ShardId, ReplicaState>>> + Send + Unpin {
        Box::pin(try_stream! {
            let mut s = grpc_client.clone().shards_subscribe(()).await?.into_inner();
            while let Some(msg) = s.message().await? {
                let mut shards = HashMap::new();
                for shard_replica_state_pb in msg.shards {
                    let shard_id = ShardId(shard_replica_state_pb.shard_id);
                    if shards.contains_key(&shard_id) {
                        Err(anyhow!("duplicate key {:?}", shard_id))?;
                    }
                    let replica_state = match shard_replica_state_pb
                        .replica_state
                        .ok_or_else(|| anyhow!("missing replica_state"))?
                        .replica_state
                        .ok_or_else(|| anyhow!("missing replica_state"))?
                    {
                        pb::internal::replica_state::ReplicaState::Leader(seq_raw) => {
                            ReplicaState::Leader(JournalSeq(seq_raw))
                        },
                        pb::internal::replica_state::ReplicaState::Follower(()) => {
                            ReplicaState::Follower
                        },
                    };

                    shards.insert(shard_id, replica_state);
                }

                yield shards;
            }
            Err(anyhow!("stream ended unexpectedly"))?;
        })
    }
}

impl runtime::Node for NodeClient {
    fn id(&self) -> NodeId {
        self.node_id
    }

    fn shard(&self, shard_id: ShardId) -> anyhow::Result<Arc<dyn Shard>> {
        Ok(Arc::new(ShardProxy {
            node_id: self.node_id,
            shard_id,
            grpc_client: self.grpc_client.clone(),
        }))
    }

    fn tablet(&self, tablet_id: TabletId) -> anyhow::Result<Arc<dyn Tablet>> {
        Ok(Arc::new(TabletProxy {
            node_id: self.node_id,
            tablet_id,
            grpc_client: self.grpc_client.clone(),
        }))
    }

    fn meta(&self) -> anyhow::Result<Arc<dyn Meta>> {
        Ok(Arc::new(MetaProxy {
            grpc_client: self.grpc_client.clone(),
        }))
    }

    fn supervisor(&self) -> anyhow::Result<Arc<dyn Supervisor>> {
        todo!();
    }

    fn shards_subscribe(
        &self,
    ) -> Box<dyn Stream<Item = anyhow::Result<HashMap<ShardId, ReplicaState>>> + Send + Unpin + '_>
    {
        // TODO: Communicate the expected node ID and don't retry if it doesn't match.
        let grpc_client = self.grpc_client.clone();
        Box::new(
            Retry::new()
                .retry_stream_indefinitely(move || {
                    Self::shards_subscribe_inner(grpc_client.clone())
                })
                .map(|item| Ok(item)),
        )
    }
}

struct ShardProxy {
    node_id: NodeId,
    shard_id: ShardId,
    grpc_client: pb::internal::node_client::NodeClient<tonic::transport::Channel>,
}

#[async_trait]
impl runtime::Shard for ShardProxy {
    fn id(&self) -> ShardId {
        self.shard_id
    }

    fn tablet(&self, tablet_id: TabletId) -> anyhow::Result<Arc<dyn runtime::Tablet>> {
        if tablet_id.0 != self.shard_id {
            return Err(anyhow!(
                "wrong shard {:?} for {:?}",
                self.shard_id,
                tablet_id
            ));
        }
        Ok(Arc::new(TabletProxy {
            node_id: self.node_id,
            tablet_id,
            grpc_client: self.grpc_client.clone(),
        }))
    }

    async fn wait_meta_sync(&self, ts: Timestamp) -> anyhow::Result<()> {
        self.grpc_client
            .clone()
            .shard_wait_meta_sync(pb::internal::ShardWaitMetaSyncReq {
                shard_id: self.shard_id.0,
                ts: ts.as_micros(),
            })
            .await?;

        Ok(())
    }

    async fn tx_try_commit(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<TxOutcome> {
        let resp = self
            .grpc_client
            .clone()
            .shard_tx_try_commit(pb::internal::ShardTxTryCommitReq {
                shard_id: self.shard_id.0,
                txid: Some(pb::internal::Txid::from(txid)),
                ts: ts.as_micros(),
                precond_keys: precond_keys.into_iter().map(key_to_proto).collect(),
                mut_keys: mut_keys.into_iter().map(key_to_proto).collect(),
            })
            .await
            .map_err(internal_err_from_status)?
            .into_inner();

        let tx_outcome = TxOutcome::try_from(
            resp.tx_outcome
                .ok_or_else(|| anyhow!("missing tx_outcome"))?,
        )?;

        Ok(tx_outcome)
    }

    async fn tx_try_abort(&self, txid: Txid) -> anyhow::Result<TxOutcome> {
        let resp = self
            .grpc_client
            .clone()
            .shard_tx_try_abort(pb::internal::ShardTxidReq {
                shard_id: self.shard_id.0,
                txid: Some(pb::internal::Txid::from(txid)),
            })
            .await
            .map_err(internal_err_from_status)?
            .into_inner();

        let tx_outcome = TxOutcome::try_from(
            resp.tx_outcome
                .ok_or_else(|| anyhow!("missing tx_outcome"))?,
        )?;

        Ok(tx_outcome)
    }

    async fn tx_wait(&self, txid: Txid) -> Result<TxOutcome, InternalError> {
        let resp = self
            .grpc_client
            .clone()
            .shard_tx_wait(pb::internal::ShardTxidReq {
                shard_id: self.shard_id.0,
                txid: Some(pb::internal::Txid::from(txid)),
            })
            .await
            .map_err(internal_err_from_status)?
            .into_inner();

        let tx_outcome = TxOutcome::try_from(
            resp.tx_outcome
                .ok_or_else(|| anyhow!("missing tx_outcome"))?,
        )?;

        Ok(tx_outcome)
    }
}

struct TabletProxy {
    node_id: NodeId,
    tablet_id: TabletId,
    grpc_client: pb::internal::node_client::NodeClient<tonic::transport::Channel>,
}

#[async_trait]
impl runtime::Tablet for TabletProxy {
    async fn get_multi(
        &self,
        ts: Timestamp,
        keys: BTreeSet<Key>,
    ) -> Result<BTreeMap<Key, Record>, InternalError> {
        let resp = self
            .grpc_client
            .clone()
            .tablet_get(pb::internal::TabletGetReq {
                tablet_id: Some(pb::internal::TabletId::from(self.tablet_id)),
                inner: Some(pb::GetReq {
                    snapshot_ts: ts.as_micros(),
                    keys: keys.into_iter().map(key_to_proto).collect(),
                }),
            })
            .await
            .map_err(internal_err_from_status)?
            .into_inner();

        let results: BTreeMap<Key, Record> = resp
            .results
            .into_iter()
            .map(|result_pb| match result_pb.result_type {
                Some(pb::get_result::ResultType::Record(record_pb)) => {
                    let record = Record::try_from(record_pb)?;
                    Ok(Some((record.key.clone(), record)))
                }
                Some(pb::get_result::ResultType::NotFound(())) => Ok(None),
                None => Err(anyhow!("invalid response: GetResult.result_type missing")),
            })
            .filter_map(Result::transpose)
            .collect::<anyhow::Result<_>>()?;

        Ok(results)
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<Range<Vec<u8>>>), InternalError> {
        let resp = self
            .grpc_client
            .clone()
            .tablet_scan_page(pb::internal::TabletScanPageReq {
                tablet_id: Some(pb::internal::TabletId::from(self.tablet_id)),
                inner: Some(
                    scan_req_to_proto(ts, keyspace_id, range, direction, limit)
                        .map_err(|e| InternalError::Other(e.into()))?,
                ),
            })
            .await
            .map_err(internal_err_from_status)?
            .into_inner();

        let results: Vec<Record> = resp
            .records
            .into_iter()
            .map(Record::try_from)
            .collect::<anyhow::Result<Vec<_>>>()?;

        let maybe_continue_range = resp.remaining.map(Range::try_from).transpose()?;

        Ok((results, maybe_continue_range))
    }

    async fn get_latest_multi(
        &self,
        keys: BTreeSet<Key>,
    ) -> Result<(Timestamp, BTreeMap<Key, Record>), InternalError> {
        let resp = self
            .grpc_client
            .clone()
            .tablet_get_latest(pb::internal::TabletGetLatestReq {
                tablet_id: Some(pb::internal::TabletId::from(self.tablet_id)),
                inner: Some(pb::GetLatestReq {
                    keys: keys.into_iter().map(key_to_proto).collect(),
                }),
            })
            .await
            .map_err(internal_err_from_status)?
            .into_inner();

        let snapshot_ts = Timestamp::from_micros(resp.snapshot_ts);
        let results: BTreeMap<Key, Record> = resp
            .results
            .into_iter()
            .map(|result_pb| match result_pb.result_type {
                Some(pb::get_result::ResultType::Record(record_pb)) => {
                    let record = Record::try_from(record_pb)?;
                    Ok(Some((record.key.clone(), record)))
                }
                Some(pb::get_result::ResultType::NotFound(())) => Ok(None),
                None => Err(anyhow!("invalid response: GetResult.result_type missing")),
            })
            .filter_map(Result::transpose)
            .collect::<anyhow::Result<_>>()?;

        Ok((snapshot_ts, results))
    }

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> Result<Timestamp, InternalError> {
        let resp = self
            .grpc_client
            .clone()
            .tablet_latest_snapshot(pb::internal::TabletGetLatestReq {
                tablet_id: Some(pb::internal::TabletId::from(self.tablet_id)),
                inner: Some(pb::GetLatestReq {
                    keys: keys.into_iter().map(key_to_proto).collect(),
                }),
            })
            .await
            .map_err(internal_err_from_status)?
            .into_inner();

        let snapshot_ts = Timestamp::from_micros(resp.snapshot_ts);

        Ok(snapshot_ts)
    }

    async fn history_page(
        &self,
        _key: Key,
        _range: HistoryRange,
        _direction: Direction,
        _limit: usize,
    ) -> Result<(Vec<Revision>, Option<HistoryRange>), InternalError> {
        todo!()
    }

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        let (preconds_pb, key_muts_pb) = preconds_muts_to_proto(preconds, muts);

        let resp = self
            .grpc_client
            .clone()
            .tablet_write(pb::internal::TabletWriteReq {
                tablet_id: Some(pb::internal::TabletId::from(self.tablet_id)),
                inner: Some(pb::WriteReq {
                    preconds: preconds_pb,
                    muts: key_muts_pb,
                }),
            })
            .await
            .map_err(internal_err_from_status)?
            .into_inner();

        let write_ts = Timestamp::from_micros(resp.write_ts);

        Ok(write_ts)
    }

    async fn prepare(
        &self,
        txid: Txid,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        let (preconds_pb, key_muts_pb) = preconds_muts_to_proto(preconds, muts);

        let resp = self
            .grpc_client
            .clone()
            .tablet_prepare(pb::internal::TabletPrepareReq {
                tablet_id: Some(pb::internal::TabletId::from(self.tablet_id)),
                txid: Some(pb::internal::Txid::from(txid)),
                preconds: preconds_pb,
                muts: key_muts_pb,
            })
            .await
            .map_err(internal_err_from_status)?
            .into_inner();

        let prepare_ts = Timestamp::from_micros(resp.prepare_ts);

        Ok(prepare_ts)
    }

    async fn cleanup_committed(
        &self,
        txid: Txid,
        ts: Timestamp,
        precond_keys: BTreeSet<Key>,
        mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        self.grpc_client
            .clone()
            .tablet_cleanup_committed(pb::internal::TabletCleanupCommittedReq {
                tablet_id: Some(pb::internal::TabletId::from(self.tablet_id)),
                txid: Some(pb::internal::Txid::from(txid)),
                ts: ts.as_micros(),
                precond_keys: precond_keys.into_iter().map(key_to_proto).collect(),
                mut_keys: mut_keys.into_iter().map(key_to_proto).collect(),
            })
            .await
            .map_err(internal_err_from_status)?;

        Ok(())
    }

    async fn manifest(&self) -> anyhow::Result<Manifest> {
        let resp = self
            .grpc_client
            .clone()
            .tablet_manifest(pb::internal::TabletEmptyReq {
                tablet_id: Some(pb::internal::TabletId::from(self.tablet_id)),
            })
            .await
            .map_err(internal_err_from_status)?
            .into_inner();

        let manifest =
            Manifest::try_from(resp.manifest.ok_or_else(|| anyhow!("missing manifest"))?)?;

        Ok(manifest)
    }

    async fn wait_mostly_hydrated(&self) -> anyhow::Result<()> {
        self.grpc_client
            .clone()
            .tablet_wait_mostly_hydrated(pb::internal::TabletEmptyReq {
                tablet_id: Some(pb::internal::TabletId::from(self.tablet_id)),
            })
            .await
            .map_err(internal_err_from_status)?;

        Ok(())
    }

    async fn catchup(&self) -> anyhow::Result<()> {
        self.grpc_client
            .clone()
            .tablet_catchup(pb::internal::TabletEmptyReq {
                tablet_id: Some(pb::internal::TabletId::from(self.tablet_id)),
            })
            .await
            .map_err(internal_err_from_status)?;

        Ok(())
    }

    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>> {
        let resp = self
            .grpc_client
            .clone()
            .tablet_find_split(pb::internal::TabletEmptyReq {
                tablet_id: Some(pb::internal::TabletId::from(self.tablet_id)),
            })
            .await
            .map_err(internal_err_from_status)?
            .into_inner();

        let bound = Bound::try_from(resp.bound.ok_or_else(|| anyhow!("missing bound"))?)?;

        Ok(bound)
    }
}

struct MetaProxy {
    grpc_client: pb::internal::node_client::NodeClient<tonic::transport::Channel>,
}

#[async_trait]
impl runtime::Meta for MetaProxy {
    async fn add_shard(&self, shard_id: ShardId) -> anyhow::Result<()> {
        self.grpc_client
            .clone()
            .meta_add_shard(pb::internal::ShardIdReq {
                shard_id: shard_id.0,
            })
            .await
            .map_err(internal_err_from_status)?
            .into_inner();

        Ok(())
    }

    async fn add_node(&self, node_id: NodeId) -> anyhow::Result<()> {
        self.grpc_client
            .clone()
            .meta_add_node(pb::internal::NodeIdReq {
                node_id: Some(node_id.into()),
            })
            .await
            .map_err(internal_err_from_status)?
            .into_inner();

        Ok(())
    }

    async fn create_colo_group(
        &self,
        colo_group_id: ColoGroupId,
        initial_splits: Vec<Bound<Vec<u8>>>,
    ) -> anyhow::Result<()> {
        self.grpc_client
            .clone()
            .meta_create_colo_group(pb::CreateColoGroupReq {
                colo_group_id: colo_group_id.0,
                initial_splits: initial_splits
                    .into_iter()
                    .map(|bound| bound.into())
                    .collect(),
            })
            .await
            .map_err(internal_err_from_status)?
            .into_inner();

        Ok(())
    }

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        self.grpc_client
            .clone()
            .meta_create_keyspace(pb::CreateKeyspaceReq {
                keyspace_id: Some(keyspace_id.into()),
            })
            .await
            .map_err(internal_err_from_status)?
            .into_inner();

        Ok(())
    }

    async fn latest_snapshot(&self) -> anyhow::Result<Timestamp> {
        let resp = self
            .grpc_client
            .clone()
            .meta_latest_snapshot(())
            .await
            .map_err(internal_err_from_status)?
            .into_inner();

        let ts = Timestamp::from_micros(resp.ts);

        Ok(ts)
    }

    async fn wait_for_newer(&self, ts: Timestamp) -> anyhow::Result<()> {
        self.grpc_client
            .clone()
            .meta_wait_for_newer(pb::internal::Timestamp { ts: ts.as_micros() })
            .await
            .map_err(internal_err_from_status)?
            .into_inner();

        Ok(())
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        range: Range<Vec<u8>>,
    ) -> anyhow::Result<(Vec<Record>, Option<Range<Vec<u8>>>)> {
        let resp = self
            .grpc_client
            .clone()
            .meta_scan_page(pb::internal::MetaScanPageReq {
                ts: ts.as_micros(),
                range: Some(range.into()),
            })
            .await
            .map_err(internal_err_from_status)?
            .into_inner();

        let records = resp
            .records
            .into_iter()
            .map(Record::try_from)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let maybe_continue_range = resp
            .remaining
            .map(|range_pb| Range::try_from(range_pb))
            .transpose()?;

        Ok((records, maybe_continue_range))
    }

    async fn sync(&self, ts: Timestamp) -> anyhow::Result<(Vec<Revision>, Timestamp)> {
        let resp = self
            .grpc_client
            .clone()
            .meta_sync(pb::internal::Timestamp { ts: ts.as_micros() })
            .await
            .map_err(internal_err_from_status)?
            .into_inner();

        let revisions = resp
            .revisions
            .into_iter()
            .map(Revision::try_from)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let ts = Timestamp::from_micros(resp.ts);

        Ok((revisions, ts))
    }

    async fn tablet_ids(&self, _ts: Timestamp) -> anyhow::Result<Vec<TabletId>> {
        todo!();
    }

    async fn write(
        &self,
        snapshot_ts: Timestamp,
        muts: HashMap<MetaKey, MetaMutation>,
    ) -> Result<Timestamp, InternalError> {
        let resp = self
            .grpc_client
            .clone()
            .meta_write(pb::internal::MetaWriteReq {
                snapshot_ts: snapshot_ts.as_micros(),
                mutations: muts
                    .into_iter()
                    .map(|(meta_key, meta_mut)| pb::internal::MetaKeyMutation {
                        key: meta_key.encode(),
                        mutation: Some(
                            match meta_mut {
                                MetaMutation::Put(meta_value) => Mutation::Put(meta_value.encode()),
                                MetaMutation::Delete => Mutation::Delete,
                            }
                            .into(),
                        ),
                    })
                    .collect(),
            })
            .await
            .map_err(internal_err_from_status)?
            .into_inner();

        let ts = Timestamp::from_micros(resp.ts);

        Ok(ts)
    }
}
