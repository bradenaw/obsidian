use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::Stream;

use crate::grpc::util::preconds_muts_to_proto;
use crate::grpc::util::scan_req_to_proto;
use crate::grpc::util::Pool;
use crate::lsm::Manifest;
use crate::pb;
use crate::runtime;
use crate::runtime::Meta;
use crate::runtime::Shard;
use crate::runtime::Supervisor;
use crate::runtime::Tablet;
use crate::Bound;
use crate::Direction;
use crate::HistoryRange;
use crate::InternalError;
use crate::JournalSeq;
use crate::Key;
use crate::KeyspaceId;
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
    client_pool: Arc<Pool<pb::internal::node_client::NodeClient<tonic::transport::Channel>>>,
}

impl NodeClient {
    pub fn new(
        node_id: NodeId,
        inner: &pb::internal::node_client::NodeClient<tonic::transport::Channel>,
    ) -> Self {
        Self {
            node_id,
            client_pool: Arc::new(Pool::new(32, inner)),
        }
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
            client_pool: Arc::clone(&self.client_pool),
        }))
    }

    fn tablet(&self, tablet_id: TabletId) -> anyhow::Result<Arc<dyn Tablet>> {
        Ok(Arc::new(TabletProxy {
            node_id: self.node_id,
            tablet_id,
            client_pool: Arc::clone(&self.client_pool),
        }))
    }

    fn meta(&self) -> anyhow::Result<Arc<dyn Meta>> {
        todo!();
    }

    fn supervisor(&self) -> anyhow::Result<Arc<dyn Supervisor>> {
        todo!();
    }

    fn became_leader_at_subscribe(
        &self,
    ) -> Box<dyn Stream<Item = anyhow::Result<HashMap<ShardId, JournalSeq>>> + Send + Unpin + '_>
    {
        todo!();
    }
}

struct ShardProxy {
    node_id: NodeId,
    shard_id: ShardId,
    client_pool: Arc<Pool<pb::internal::node_client::NodeClient<tonic::transport::Channel>>>,
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
            client_pool: Arc::clone(&self.client_pool),
        }))
    }

    async fn wait_meta_sync(&self, _ts: Timestamp) -> anyhow::Result<()> {
        todo!();
    }
}

struct TabletProxy {
    node_id: NodeId,
    tablet_id: TabletId,
    client_pool: Arc<Pool<pb::internal::node_client::NodeClient<tonic::transport::Channel>>>,
}

#[async_trait]
impl runtime::Tablet for TabletProxy {
    async fn get_multi(
        &self,
        ts: Timestamp,
        keys: BTreeSet<Key>,
    ) -> Result<BTreeMap<Key, Record>, InternalError> {
        let resp = self
            .client_pool
            .acquire()
            .await
            .tablet_get(pb::internal::TabletGetReq {
                node_id: Some(pb::internal::NodeId::from(self.node_id)),
                tablet_id: Some(pb::internal::TabletId::from(self.tablet_id)),
                inner: Some(pb::GetReq {
                    snapshot_ts: ts.as_micros(),
                    keys: keys.into_iter().map(pb::Key::from).collect(),
                }),
            })
            .await
            .map_err(|e| InternalError::Other(e.into()))?
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
            .client_pool
            .acquire()
            .await
            .tablet_scan_page(pb::internal::TabletScanPageReq {
                node_id: Some(pb::internal::NodeId::from(self.node_id)),
                tablet_id: Some(pb::internal::TabletId::from(self.tablet_id)),
                inner: Some(
                    scan_req_to_proto(ts, keyspace_id, range, direction, limit)
                        .map_err(|e| InternalError::Other(e.into()))?,
                ),
            })
            .await
            .map_err(|e| InternalError::Other(e.into()))?
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
            .client_pool
            .acquire()
            .await
            .tablet_get_latest(pb::internal::TabletGetLatestReq {
                node_id: Some(pb::internal::NodeId::from(self.node_id)),
                tablet_id: Some(pb::internal::TabletId::from(self.tablet_id)),
                inner: Some(pb::GetLatestReq {
                    keys: keys.into_iter().map(pb::Key::from).collect(),
                }),
            })
            .await
            .map_err(|e| InternalError::Other(e.into()))?
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
            .client_pool
            .acquire()
            .await
            .tablet_latest_snapshot(pb::internal::TabletGetLatestReq {
                node_id: Some(pb::internal::NodeId::from(self.node_id)),
                tablet_id: Some(pb::internal::TabletId::from(self.tablet_id)),
                inner: Some(pb::GetLatestReq {
                    keys: keys.into_iter().map(pb::Key::from).collect(),
                }),
            })
            .await
            .map_err(|e| InternalError::Other(e.into()))?
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
        let (preconds_pb, keys_pb, muts_pb) = preconds_muts_to_proto(preconds, muts);

        let resp = self
            .client_pool
            .acquire()
            .await
            .tablet_write(pb::internal::TabletWriteReq {
                node_id: Some(pb::internal::NodeId::from(self.node_id)),
                tablet_id: Some(pb::internal::TabletId::from(self.tablet_id)),
                inner: Some(pb::WriteReq {
                    preconds: preconds_pb,
                    keys: keys_pb,
                    muts: muts_pb,
                }),
            })
            .await
            // TODO: make a proper WriteError.
            .map_err(anyhow::Error::from)?
            .into_inner();

        let write_ts = Timestamp::from_micros(resp.write_ts);

        Ok(write_ts)
    }

    async fn prepare(
        &self,
        _txid: Txid,
        _preconds: Vec<Precondition>,
        _muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, InternalError> {
        todo!()
    }

    async fn try_commit(
        &self,
        _txid: Txid,
        _ts: Timestamp,
        _precond_keys: BTreeSet<Key>,
        _mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<TxOutcome> {
        todo!()
    }

    async fn try_abort(&self, _txid: Txid) -> anyhow::Result<TxOutcome> {
        todo!()
    }

    async fn wait(&self, _txid: Txid) -> Result<TxOutcome, InternalError> {
        todo!()
    }

    async fn cleanup_committed(
        &self,
        _txid: Txid,
        _ts: Timestamp,
        _precond_keys: BTreeSet<Key>,
        _mut_keys: BTreeSet<Key>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn wait_meta_sync(&self, _ts: Timestamp) -> anyhow::Result<()> {
        todo!()
    }

    async fn manifest(&self) -> anyhow::Result<Manifest> {
        todo!()
    }

    async fn wait_mostly_hydrated(&self) -> anyhow::Result<()> {
        todo!()
    }

    async fn catchup(&self) -> anyhow::Result<()> {
        todo!()
    }

    async fn find_split(&self) -> anyhow::Result<Bound<Vec<u8>>> {
        todo!()
    }
}
