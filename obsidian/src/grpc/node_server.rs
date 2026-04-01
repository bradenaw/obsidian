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
        todo!();
    }

    async fn tablet_scan_page(
        &self,
        req: tonic::Request<pb::internal::TabletScanPageReq>,
    ) -> Result<tonic::Response<pb::ScanResp>, tonic::Status> {
        todo!();
    }

    async fn tablet_write(
        &self,
        req: tonic::Request<pb::internal::TabletWriteReq>,
    ) -> Result<tonic::Response<pb::WriteResp>, tonic::Status> {
        todo!();
    }
}
