use anyhow::anyhow;
use async_trait::async_trait;

use crate::grpc::util::get_req_results;
use crate::grpc::util::internal;
use crate::grpc::util::invalid_argument;
use crate::grpc::util::required;
use crate::node::Node;
use crate::pb;
use crate::runtime::Node as _;
use crate::Bound;
use crate::ColoGroupId;
use crate::Direction;
use crate::Key;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Obsidian;
use crate::Precondition;
use crate::Range;
use crate::TabletId;
use crate::Timestamp;

pub(crate) struct NodeServer {
    node: Node,
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
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(pb::GetResp {
            results: results_builder.build(records).map_err(internal)?,
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
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(pb::GetLatestResp {
            snapshot_ts: snapshot_ts.as_micros(),
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

        let snapshot_ts = Timestamp::from_micros(scan_req.snapshot_ts);
        let keyspace_id: KeyspaceId = required("keyspace_id", scan_req.keyspace_id)?;
        let range: Range<Vec<u8>> = required("range", scan_req.range)?;
        let direction: Direction = pb::Direction::from_i32(scan_req.direction)
            .ok_or_else(|| tonic::Status::invalid_argument("unknown direction"))?
            .try_into()
            .map_err(invalid_argument)?;
        let limit = usize::try_from(scan_req.limit)
            .map_err(|_| tonic::Status::invalid_argument("invalid limit"))?;

        let (records, maybe_continue_range) = tablet
            .scan_page(snapshot_ts, keyspace_id, range.borrow(), direction, limit)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(pb::ScanResp {
            records: records.into_iter().map(pb::Record::from).collect(),
            remaining: maybe_continue_range.map(Range::into),
        }))
    }

    async fn tablet_write(
        &self,
        req: tonic::Request<pb::internal::TabletWriteReq>,
    ) -> Result<tonic::Response<pb::WriteResp>, tonic::Status> {
        todo!();
    }
}
