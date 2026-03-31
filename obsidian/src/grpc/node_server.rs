use anyhow::anyhow;
use async_trait::async_trait;

use crate::grpc::util::internal;
use crate::grpc::util::invalid_argument;
use crate::grpc::util::key_set_from_pb;
use crate::grpc::util::options_to_get_results;
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
        let req_inner_inner = req_inner
            .inner
            .ok_or_else(|| invalid_argument(anyhow!("missing inner")))?;
        let (keys, key_idxs) = key_set_from_pb(req_inner_inner.keys).map_err(invalid_argument)?;
        let ts = Timestamp::from_micros(req_inner_inner.snapshot_ts);

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
            let maybe_value = tablet
                .get(ts, &key)
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?;

            values[key_idxs[&key]] = maybe_value;
        }

        Ok(tonic::Response::new(pb::GetResp {
            results: options_to_get_results(values),
        }))
    }
}
