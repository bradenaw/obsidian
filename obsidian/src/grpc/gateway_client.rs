use std::collections::BTreeMap;
use std::collections::BTreeSet;

use anyhow::anyhow;
use async_trait::async_trait;
use obsidian_common::key_to_proto;
use obsidian_pb as pb;

use crate::grpc::util::preconds_muts_to_proto;
use crate::grpc::util::scan_req_to_proto;
use crate::grpc::util::Pool;
use crate::Bound;
use crate::ColoGroupId;
use crate::Direction;
use crate::Key;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Obsidian;
use crate::Precondition;
use crate::Range;
use crate::Record;
use crate::Timestamp;
use crate::WriteError;

pub struct GatewayClient {
    inner: Pool<pb::obsidian_client::ObsidianClient<tonic::transport::Channel>>,
}

impl GatewayClient {
    pub fn new(inner: &pb::obsidian_client::ObsidianClient<tonic::transport::Channel>) -> Self {
        Self {
            inner: Pool::new(32, inner),
        }
    }
}

#[async_trait]
impl Obsidian for GatewayClient {
    async fn get_multi(
        &self,
        ts: Timestamp,
        keys: BTreeSet<Key>,
    ) -> anyhow::Result<BTreeMap<Key, Record>> {
        let resp = self
            .inner
            .acquire()
            .await
            .get(pb::GetReq {
                snapshot_ts: ts.as_micros(),
                keys: keys.into_iter().map(key_to_proto).collect(),
            })
            .await?
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
    ) -> anyhow::Result<(Vec<Record>, Option<Range<Vec<u8>>>)> {
        let resp = self
            .inner
            .acquire()
            .await
            .scan(scan_req_to_proto(ts, keyspace_id, range, direction, limit)?)
            .await?
            .into_inner();

        let results: Vec<Record> = resp
            .records
            .into_iter()
            .map(Record::try_from)
            .collect::<anyhow::Result<Vec<_>>>()?;

        let maybe_continue_range = resp.remaining.map(Range::try_from).transpose()?;

        Ok((results, maybe_continue_range))
    }

    async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> anyhow::Result<Timestamp> {
        // TODO: Use the native one.
        let resp = self
            .inner
            .acquire()
            .await
            .get_latest(pb::GetLatestReq {
                keys: keys.into_iter().map(key_to_proto).collect(),
            })
            .await?
            .into_inner();

        Ok(Timestamp::from_micros(resp.snapshot_ts))
    }

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<Key, Mutation>,
    ) -> Result<Timestamp, WriteError> {
        let (preconds_pb, key_muts_pb) = preconds_muts_to_proto(preconds, muts);

        let resp = self
            .inner
            .acquire()
            .await
            .write(pb::WriteReq {
                preconds: preconds_pb,
                muts: key_muts_pb,
            })
            .await
            // TODO: make a proper WriteError.
            .map_err(anyhow::Error::from)?
            .into_inner();

        let write_ts = Timestamp::from_micros(resp.write_ts);

        Ok(write_ts)
    }

    async fn create_colo_group(
        &self,
        colo_group_id: ColoGroupId,
        initial_splits: Vec<Bound<Vec<u8>>>,
    ) -> anyhow::Result<()> {
        self.inner
            .acquire()
            .await
            .create_colo_group(pb::CreateColoGroupReq {
                colo_group_id: colo_group_id.0,
                initial_splits: initial_splits.into_iter().map(Bound::into).collect(),
            })
            .await
            .map_err(anyhow::Error::from)?;

        Ok(())
    }

    async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        self.inner
            .acquire()
            .await
            .create_keyspace(pb::CreateKeyspaceReq {
                keyspace_id: Some(keyspace_id.into()),
            })
            .await
            .map_err(anyhow::Error::from)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    use anyhow::anyhow;
    use async_trait::async_trait;
    use futures::FutureExt;
    use obsidian_pb as pb;
    use tokio::sync::oneshot;

    use crate::grpc::GatewayServer;
    use crate::test::obsidian_grpc_bridge;
    use crate::test::obsidian_test_suite;
    use crate::test::ObsidianForTestBuilder;
    use crate::ColoGroupId;
    use crate::Key;
    use crate::KeyspaceId;
    use crate::Mutation;
    use crate::Obsidian;
    use crate::Record;

    #[tokio::test]
    async fn test_write() -> anyhow::Result<()> {
        let obs = ObsidianForTestBuilder::new().n_shards(1).build().await?;
        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
        obs.gateway
            .create_colo_group(keyspace_id.0, vec![] /*splits*/)
            .await?;
        obs.gateway.create_keyspace(keyspace_id).await?;

        let client = obsidian_grpc_bridge(obs.gateway).await?;

        let key = (keyspace_id, b"abc".to_vec());

        let write_ts = client
            .write(
                vec![],
                BTreeMap::from([(key.clone(), Mutation::Put(b"def".to_vec()))]),
            )
            .await?;

        let snapshot_ts = client.latest_snapshot(BTreeSet::from([key])).await?;

        assert_eq!(write_ts, snapshot_ts);

        Ok(())
    }

    obsidian_test_suite!({
        use std::sync::Arc;

        use obsidian_pb as pb;

        use crate::grpc::GatewayClient;
        use crate::grpc::GatewayServer;
        use crate::test::obsidian_grpc_bridge;
        use crate::test::GrpcBridge;
        use crate::test::ObsidianForTestBuilder;
        use crate::Obsidian;

        async || {
            let obs = ObsidianForTestBuilder::new().n_shards(2).build().await?;
            let client = obsidian_grpc_bridge(obs.gateway).await?;
            Ok::<GrpcBridge<Arc<dyn Obsidian>>, anyhow::Error>(client)
        }
    });
}
