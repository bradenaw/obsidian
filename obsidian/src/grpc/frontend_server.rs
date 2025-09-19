use std::collections::BTreeMap;
use std::iter;

use async_trait::async_trait;

use crate::grpc::util::internal;
use crate::grpc::util::invalid_argument;
use crate::grpc::util::key_set_from_pb;
use crate::grpc::util::options_to_get_results;
use crate::grpc::util::required;
use crate::pb;
use crate::Obsidian;
use crate::Bound;
use crate::ColoGroupId;
use crate::Direction;
use crate::Key;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Precondition;
use crate::Range;
use crate::Timestamp;

pub struct FrontendServer<O> {
    inner: O,
}

impl<O> FrontendServer<O> {
    pub(crate) fn new(inner: O) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<O: Obsidian + 'static> pb::obsidian_server::Obsidian for FrontendServer<O> {
    async fn get(
        &self,
        req: tonic::Request<pb::GetReq>,
    ) -> Result<tonic::Response<pb::GetResp>, tonic::Status> {
        let req_inner = req.into_inner();

        let (keys, key_idxs) = key_set_from_pb(req_inner.keys).map_err(invalid_argument)?;
        let ts = Timestamp::from_nanos(req_inner.snapshot_ts);

        if keys.len() != 1 {
            return Err(tonic::Status::invalid_argument(
                "TODO: Get() only allows one key",
            ));
        }

        let mut values = Vec::with_capacity(keys.len());
        for _ in &keys {
            values.push(None);
        }
        for key in keys {
            let maybe_value = self
                .inner
                .get(ts, &key)
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?;

            values[key_idxs[&key]] = maybe_value;
        }

        Ok(tonic::Response::new(pb::GetResp {
            results: options_to_get_results(values),
        }))
    }

    async fn get_latest(
        &self,
        req: tonic::Request<pb::GetLatestReq>,
    ) -> Result<tonic::Response<pb::GetLatestResp>, tonic::Status> {
        let req_inner = req.into_inner();

        let (keys, key_idxs) = key_set_from_pb(req_inner.keys).map_err(invalid_argument)?;

        let ts = self
            .inner
            .latest_snapshot(keys.clone())
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        let mut values = Vec::with_capacity(keys.len());
        for _ in &keys {
            values.push(None);
        }
        for key in keys {
            let maybe_value = self
                .inner
                .get(ts, &key)
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?;

            values[key_idxs[&key]] = maybe_value;
        }

        Ok(tonic::Response::new(pb::GetLatestResp {
            snapshot_ts: ts.as_nanos(),
            results: options_to_get_results(values),
        }))
    }

    async fn scan(
        &self,
        req: tonic::Request<pb::ScanReq>,
    ) -> Result<tonic::Response<pb::ScanResp>, tonic::Status> {
        let req_inner = req.into_inner();

        let snapshot_ts = Timestamp::from_nanos(req_inner.snapshot_ts);
        let keyspace_id: KeyspaceId = required("keyspace_id", req_inner.keyspace_id)?;
        let range: Range<Vec<u8>> = required("range", req_inner.range)?;
        let direction: Direction = pb::Direction::from_i32(req_inner.direction)
            .ok_or_else(|| tonic::Status::invalid_argument("unknown direction"))?
            .try_into()
            .map_err(invalid_argument)?;
        let limit = usize::try_from(req_inner.limit)
            .map_err(|_| tonic::Status::invalid_argument("invalid limit"))?;

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

        let preconds = req_inner
            .preconds
            .into_iter()
            .map(|x| x.try_into())
            .collect::<Result<Vec<Precondition>, _>>()
            .map_err(invalid_argument)?;
        let keys = req_inner
            .keys
            .into_iter()
            .map(|x| x.try_into())
            .collect::<Result<Vec<Key>, _>>()
            .map_err(invalid_argument)?;
        let muts = req_inner
            .muts
            .into_iter()
            .map(|x| x.try_into())
            .collect::<Result<Vec<Mutation>, _>>()
            .map_err(invalid_argument)?;

        let mut muts_map = BTreeMap::new();
        if keys.len() != muts.len() {
            return Err(tonic::Status::invalid_argument(
                "keys and muts must have the same number of elements",
            ));
        }
        for (key, m) in iter::zip(keys, muts) {
            if muts_map.contains_key(&key) {
                return Err(tonic::Status::invalid_argument(format!(
                    "duplicate key {:?}",
                    key
                )));
            }
            muts_map.insert(key, m);
        }

        let ts = self
            .inner
            .write(preconds, muts_map)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(pb::WriteResp {
            write_ts: ts.as_nanos(),
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
