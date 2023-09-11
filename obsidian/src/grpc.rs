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

        for key in keys {
            let value = self.inner
                .get(ts, key.0, key.1)
                .await
                .map_err(|e| tonic::Status::unknown(e.to_string()))?;
        }

        todo!();
        //Ok(tonic::Response::new(pb::GetLatestReq {

        //}))
    }

    async fn write(
        &self,
        _request: tonic::Request<pb::WriteReq>,
    ) -> Result<tonic::Response<pb::WriteResp>, tonic::Status> {
        todo!();
    }
}
