use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;

use crate::grpc::util::get_req_results;
use crate::grpc::util::internal;
use crate::grpc::util::internal_err_from_status;
use crate::grpc::util::internal_err_to_status;
use crate::grpc::util::invalid_argument;
use crate::grpc::util::parse_scan_req;
use crate::grpc::util::parse_write_req;
use crate::grpc::util::required;
use crate::pb;
use crate::runtime::Node;
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
use crate::TabletId;
use crate::Timestamp;

pub(crate) struct NodeServer {
    node: Arc<dyn Node>,
}

impl NodeServer {
    pub fn new(node: Arc<dyn Node>) -> Self {
        Self { node }
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

        let (preconds, muts) = parse_write_req(write_req).map_err(invalid_argument)?;

        let ts = tablet
            .write(preconds, muts)
            .await
            .map_err(internal_err_to_status)?;

        Ok(tonic::Response::new(pb::WriteResp {
            write_ts: ts.as_micros(),
        }))
    }
}
