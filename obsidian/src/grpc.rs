use std::collections::BTreeSet;

use async_trait::async_trait;

use crate::obsidian::Obsidian;
use crate::pb;
use crate::types::KeyspaceId;

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
            .map_err(|e| tonic::Status::unknown(format!("{}", e)))?;

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
        _request: tonic::Request<pb::WriteReq>,
    ) -> Result<tonic::Response<pb::WriteResp>, tonic::Status> {
        todo!();
    }
}
