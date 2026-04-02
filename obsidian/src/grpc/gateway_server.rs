use std::collections::BTreeMap;
use std::iter;
use std::sync::Arc;

use async_trait::async_trait;

use crate::grpc::util::get_req_results;
use crate::grpc::util::internal;
use crate::grpc::util::invalid_argument;
use crate::grpc::util::parse_scan_req;
use crate::grpc::util::parse_write_req;
use crate::grpc::util::required;
use crate::pb;
use crate::Bound;
use crate::ColoGroupId;
use crate::Direction;
use crate::Key;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Obsidian;
use crate::Precondition;
use crate::Range;
use crate::Timestamp;

pub struct GatewayServer {
    inner: Arc<dyn Obsidian>,
}

impl GatewayServer {
    pub(crate) fn new(inner: Arc<dyn Obsidian>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl pb::obsidian_server::Obsidian for GatewayServer {
    async fn get(
        &self,
        req: tonic::Request<pb::GetReq>,
    ) -> Result<tonic::Response<pb::GetResp>, tonic::Status> {
        let req_inner = req.into_inner();

        let (keys, results_builder) = get_req_results(req_inner.keys).map_err(invalid_argument)?;
        let ts = Timestamp::from_micros(req_inner.snapshot_ts);

        let records = self
            .inner
            .get_multi(ts, keys)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(pb::GetResp {
            results: results_builder.build(records).map_err(internal)?,
        }))
    }

    async fn get_latest(
        &self,
        req: tonic::Request<pb::GetLatestReq>,
    ) -> Result<tonic::Response<pb::GetLatestResp>, tonic::Status> {
        // TODO: Just call Obsidian::get_latest_multi.
        let req_inner = req.into_inner();

        let (keys, results_builder) = get_req_results(req_inner.keys).map_err(invalid_argument)?;

        let ts = self
            .inner
            .latest_snapshot(keys.clone())
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        let records = self
            .inner
            .get_multi(ts, keys)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(pb::GetLatestResp {
            snapshot_ts: ts.as_micros(),
            results: results_builder.build(records).map_err(internal)?,
        }))
    }

    async fn scan(
        &self,
        req: tonic::Request<pb::ScanReq>,
    ) -> Result<tonic::Response<pb::ScanResp>, tonic::Status> {
        let req_inner = req.into_inner();

        let (snapshot_ts, keyspace_id, range, direction, limit) =
            parse_scan_req(req_inner).map_err(invalid_argument)?;

        let (records, maybe_continue_range) = self
            .inner
            .scan_page(snapshot_ts, keyspace_id, range.borrow(), direction, limit)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(pb::ScanResp {
            records: records.into_iter().map(pb::Record::from).collect(),
            remaining: maybe_continue_range.map(Range::into),
        }))
    }

    async fn write(
        &self,
        req: tonic::Request<pb::WriteReq>,
    ) -> Result<tonic::Response<pb::WriteResp>, tonic::Status> {
        let req_inner = req.into_inner();

        let (preconds, muts) = parse_write_req(req_inner).map_err(invalid_argument)?;

        let ts = self
            .inner
            .write(preconds, muts)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(pb::WriteResp {
            write_ts: ts.as_micros(),
        }))
    }

    async fn create_colo_group(
        &self,
        req: tonic::Request<pb::CreateColoGroupReq>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let req_inner = req.into_inner();
        let colo_group_id = ColoGroupId(req_inner.colo_group_id);
        let initial_splits = req_inner
            .initial_splits
            .into_iter()
            .map(Bound::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| invalid_argument(e.context("initial_splits")))?;

        self.inner
            .create_colo_group(colo_group_id, initial_splits)
            .await
            .map_err(internal)?;

        Ok(tonic::Response::new(()))
    }

    async fn create_keyspace(
        &self,
        req: tonic::Request<pb::CreateKeyspaceReq>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let req_inner = req.into_inner();
        let keyspace_id: KeyspaceId = required("keyspace_id", req_inner.keyspace_id)?;

        self.inner
            .create_keyspace(keyspace_id)
            .await
            .map_err(internal)?;

        Ok(tonic::Response::new(()))
    }
}
