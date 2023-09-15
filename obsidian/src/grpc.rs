use std::collections::BTreeSet;
use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::obsidian::Obsidian;
use crate::pb;
use crate::types::KeyspaceId;
use crate::types::Timestamp;
use crate::types::ColoGroupId;
use crate::types::Precondition;
use crate::types::Mutation;
use crate::types::WriteError;
use crate::types::Direction;
use crate::range::Range;
use crate::range::Bound;

pub struct FrontendClient {
    inner: pb::obsidian_client::ObsidianClient<tonic::transport::Channel>,
}

#[async_trait]
impl Obsidian for FrontendClient {
    async fn get(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        key: Vec<u8>,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        todo!()
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<(Vec<u8>, Timestamp, Vec<u8>)>, Option<Range<Vec<u8>>>)> {
        todo!()
    }

    async fn latest_snapshot(
        &self,
        keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
    ) -> anyhow::Result<Timestamp> {
        todo!()
    }

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<(KeyspaceId, Vec<u8>), Mutation>,
    ) -> Result<Timestamp, WriteError> {
        todo!()
    }

    async fn create_colo_group(
        &self,
        colo_group_id: ColoGroupId,
        initial_splits: Vec<Bound<Vec<u8>>>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        todo!()
    }
}

pub struct FrontendServer<O> {
    inner: O,
}

#[async_trait]
impl<O: Obsidian + Send + Sync + 'static> pb::obsidian_server::Obsidian for FrontendServer<O> {
    async fn get_latest(
        &self,
        req: tonic::Request<pb::GetLatestReq>,
    ) -> Result<tonic::Response<pb::GetLatestResp>, tonic::Status> {
        let req_inner = req.into_inner();

        let keys = {
            let mut keys = BTreeSet::new();
            for (i, key_pb) in req_inner.keys.into_iter().enumerate() {
                let keyspace_id = KeyspaceId::try_from(key_pb.keyspace_id.ok_or_else(|| {
                    tonic::Status::invalid_argument(format!("missing keyspace_id on key {}", i))
                })?)
                .map_err(|e| {
                    tonic::Status::invalid_argument(format!(
                        "invalid keyspace_id on key {}: {}",
                        i, e
                    ))
                })?;
                let key = (keyspace_id, key_pb.bytes);

                if keys.contains(&key) {
                    return Err(tonic::Status::invalid_argument(format!(
                        "duplicate key {:?}",
                        key
                    )));
                }

                keys.insert(key);
            }
            keys
        };

        let ts = self
            .inner
            .latest_snapshot(keys.clone())
            .await
            .map_err(|e| tonic::Status::unknown(e.to_string()))?;

        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            let maybe_value = self
                .inner
                .get(ts, key.0, key.1)
                .await
                .map_err(|e| tonic::Status::unknown(e.to_string()))?;

            values.push(maybe_value);
        }

        Ok(tonic::Response::new(pb::GetLatestResp {
            snapshot_ts: ts.as_nanos(),
            results: values
                .into_iter()
                .map(|maybe_value| match maybe_value {
                    Some(value) => pb::GetResult {
                        result_type: Some(pb::get_result::ResultType::Value(value)),
                    },
                    None => pb::GetResult {
                        result_type: Some(pb::get_result::ResultType::Missing(())),
                    },
                })
                .collect(),
        }))
    }

    async fn write(
        &self,
        request: tonic::Request<pb::WriteReq>,
    ) -> Result<tonic::Response<pb::WriteResp>, tonic::Status> {
        todo!();
    }
}
