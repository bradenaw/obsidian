use std::collections::BTreeMap;
use std::collections::BTreeSet;

use anyhow::anyhow;
use async_trait::async_trait;

use crate::grpc::util::Pool;
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
use crate::Record;
use crate::Timestamp;
use crate::WriteError;

pub struct GatewayClient {
    inner: Pool<pb::obsidian_client::ObsidianClient<tonic::transport::Channel>>,
}

impl GatewayClient {
    fn new(inner: &pb::obsidian_client::ObsidianClient<tonic::transport::Channel>) -> Self {
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
                keys: keys.into_iter().map(pb::Key::from).collect(),
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
            .scan(pb::ScanReq {
                snapshot_ts: ts.as_micros(),
                keyspace_id: Some(keyspace_id.into()),
                range: Some(range.to_vec().into()),
                direction: pb::Direction::from(direction).into(),
                limit: u64::try_from(limit)?,
            })
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
        let resp = self
            .inner
            .acquire()
            .await
            .get_latest(pb::GetLatestReq {
                keys: keys.into_iter().map(pb::Key::from).collect(),
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
        let preconds_pb: Vec<_> = preconds.into_iter().map(pb::Precondition::from).collect();

        let mut keys_pb = Vec::with_capacity(muts.len());
        let mut muts_pb = Vec::with_capacity(muts.len());
        for (key, m) in muts.into_iter() {
            keys_pb.push(pb::Key::from(key));
            muts_pb.push(pb::Mutation::from(m));
        }

        let resp = self
            .inner
            .acquire()
            .await
            .write(pb::WriteReq {
                preconds: preconds_pb,
                keys: keys_pb,
                muts: muts_pb,
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
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tonic::transport::server::TcpIncoming;

    use crate::grpc::GatewayServer;
    use crate::pb;
    use crate::test::obsidian_test_suite;
    use crate::test::ObsidianForTest;
    use crate::ColoGroupId;
    use crate::Key;
    use crate::KeyspaceId;
    use crate::Mutation;
    use crate::Obsidian;
    use crate::Record;

    #[tokio::test]
    async fn test_write() -> anyhow::Result<()> {
        let obs = ObsidianForTest::new(1).await?;
        let keyspace_id = KeyspaceId(ColoGroupId(1), 1);
        obs.gateway
            .create_colo_group(keyspace_id.0, vec![] /*splits*/)
            .await?;
        obs.gateway.create_keyspace(keyspace_id).await?;

        let client = spawn_server(obs.gateway).await?;

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

        use super::spawn_server;
        use crate::test::ObsidianForTest;
        use crate::Obsidian;

        async || {
            let obs = ObsidianForTest::new(2 /*n_shards*/).await?;
            let client = spawn_server(obs.gateway).await?;
            Ok(Arc::new(client) as Arc<dyn Obsidian>)
        }
    });

    async fn spawn_server<O: Obsidian + 'static>(obs: O) -> anyhow::Result<ObsidianClientServer> {
        let (shutdown, on_shutdown) = oneshot::channel::<()>();
        let listener = TcpListener::bind("[::1]:0").await?;
        let addr = listener.local_addr()?;
        let server = GatewayServer::new(obs);
        let serve = tonic::transport::Server::builder()
            .add_service(pb::obsidian_server::ObsidianServer::new(server))
            .serve_with_incoming_shutdown(
                TcpIncoming::from_listener(
                    listener, true, /*nodelay*/
                    None, /*keepalive*/
                )
                .map_err(|e| anyhow!("{}", e))?,
                on_shutdown.map(|_| ()),
            );

        tokio::spawn(async { serve.await.unwrap() });

        let url = "http://".to_string() + &addr.to_string();

        let client =
            super::GatewayClient::new(&pb::obsidian_client::ObsidianClient::connect(url).await?);

        Ok(ObsidianClientServer {
            inner: client,
            shutdown: Some(shutdown),
        })
    }

    struct ObsidianClientServer {
        inner: super::GatewayClient,
        shutdown: Option<oneshot::Sender<()>>,
    }

    #[async_trait]
    impl Obsidian for ObsidianClientServer {
        async fn get_multi(
            &self,
            ts: crate::Timestamp,
            keys: BTreeSet<Key>,
        ) -> anyhow::Result<BTreeMap<Key, Record>> {
            self.inner.get_multi(ts, keys).await
        }

        async fn scan_page(
            &self,
            ts: crate::Timestamp,
            keyspace_id: KeyspaceId,
            range: crate::Range<&[u8]>,
            direction: crate::Direction,
            limit: usize,
        ) -> anyhow::Result<(Vec<Record>, Option<crate::Range<Vec<u8>>>)> {
            self.inner
                .scan_page(ts, keyspace_id, range, direction, limit)
                .await
        }

        async fn latest_snapshot(&self, keys: BTreeSet<Key>) -> anyhow::Result<crate::Timestamp> {
            self.inner.latest_snapshot(keys).await
        }

        async fn write(
            &self,
            preconds: Vec<crate::Precondition>,
            muts: BTreeMap<Key, crate::Mutation>,
        ) -> Result<crate::Timestamp, crate::WriteError> {
            self.inner.write(preconds, muts).await
        }

        async fn create_colo_group(
            &self,
            colo_group_id: ColoGroupId,
            initial_splits: Vec<crate::Bound<Vec<u8>>>,
        ) -> anyhow::Result<()> {
            self.inner
                .create_colo_group(colo_group_id, initial_splits)
                .await
        }

        async fn create_keyspace(&self, keyspace_id: KeyspaceId) -> anyhow::Result<()> {
            self.inner.create_keyspace(keyspace_id).await
        }
    }

    impl Drop for ObsidianClientServer {
        fn drop(&mut self) {
            // TODO: Not clear if there's a way to find out that the serve actually stopped and
            // unbound the port. The `serve` future appears not to end.
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
        }
    }
}
