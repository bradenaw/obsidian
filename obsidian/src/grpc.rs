use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::iter;
use std::ops::Deref;
use std::ops::DerefMut;

use anyhow::anyhow;
use async_trait::async_trait;

use crate::obsidian::Obsidian;
use crate::pb;
use crate::range::Bound;
use crate::range::Range;
use crate::types::ColoGroupId;
use crate::types::Direction;
use crate::types::KeyspaceId;
use crate::types::Mutation;
use crate::types::Precondition;
use crate::types::Timestamp;
use crate::types::WriteError;

pub struct ObsidianClient {
    inner: Pool<pb::obsidian_client::ObsidianClient<tonic::transport::Channel>>,
}

impl ObsidianClient {
    fn new(inner: &pb::obsidian_client::ObsidianClient<tonic::transport::Channel>) -> Self {
        Self {
            inner: Pool::new(32, inner),
        }
    }
}

#[async_trait]
impl Obsidian for ObsidianClient {
    async fn get(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        key: Vec<u8>,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let resp = self
            .inner
            .acquire()
            .await
            .get(pb::GetReq {
                snapshot_ts: ts.as_nanos(),
                keys: Vec::from([pb::Key::from((keyspace_id, key))]),
            })
            .await?
            .into_inner();

        let mut results: Vec<Option<Vec<u8>>> = resp
            .results
            .into_iter()
            .map(|result_pb| match result_pb.result_type {
                Some(pb::get_result::ResultType::Value(v)) => Ok(Some(v)),
                Some(pb::get_result::ResultType::NotFound(())) => Ok(None),
                None => Err(anyhow!("invalid response: GetResult.result_type missing")),
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        Ok(results
            .pop()
            .ok_or_else(|| anyhow!("invalid response: missing GetResp.results"))?)
    }

    async fn scan_page(
        &self,
        ts: Timestamp,
        keyspace_id: KeyspaceId,
        range: Range<&[u8]>,
        direction: Direction,
        limit: usize,
    ) -> anyhow::Result<(Vec<(Vec<u8>, Timestamp, Vec<u8>)>, Option<Range<Vec<u8>>>)> {
        let resp = self
            .inner
            .acquire()
            .await
            .scan(pb::ScanReq {
                snapshot_ts: ts.as_nanos(),
                keyspace_id: Some(keyspace_id.into()),
                range: Some(range.to_vec().into()),
                direction: pb::Direction::from(direction).into(),
                limit: u64::try_from(limit)?,
            })
            .await?
            .into_inner();

        let results: Vec<(Vec<u8>, Timestamp, Vec<u8>)> = resp
            .records
            .into_iter()
            .map(|r| {
                Ok((
                    r.key
                        .ok_or_else(|| anyhow!("invalid response: record missing key"))?
                        .bytes,
                    Timestamp::from_nanos(r.ts),
                    r.value,
                ))
            })
            .collect::<anyhow::Result<Vec<(Vec<u8>, Timestamp, Vec<u8>)>>>()?;

        let continue_range = Range::try_from(
            resp.remaining
                .ok_or_else(|| anyhow!("invalid response: missing continue_range"))?,
        )?;
        let maybe_continue_range = if continue_range.is_empty() {
            None
        } else {
            Some(continue_range)
        };

        Ok((results, maybe_continue_range))
    }

    async fn latest_snapshot(
        &self,
        keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
    ) -> anyhow::Result<Timestamp> {
        let resp = self
            .inner
            .acquire()
            .await
            .get_latest(pb::GetLatestReq {
                keys: keys.into_iter().map(pb::Key::from).collect(),
            })
            .await?
            .into_inner();

        Ok(Timestamp::from_nanos(resp.snapshot_ts))
    }

    async fn write(
        &self,
        preconds: Vec<Precondition>,
        muts: BTreeMap<(KeyspaceId, Vec<u8>), Mutation>,
    ) -> Result<Timestamp, WriteError> {
        let preconds_pb: Vec<_> = preconds.into_iter().map(pb::Precondition::from).collect();

        let mut keys_pb = Vec::with_capacity(muts.len());
        let mut muts_pb = Vec::with_capacity(muts.len());
        for ((keyspace_id, key), m) in muts.into_iter() {
            keys_pb.push(pb::Key::from((keyspace_id, key)));
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

        let write_ts = Timestamp::from_nanos(resp.write_ts);

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

pub struct ObsidianServer<O> {
    inner: O,
}

impl<O> ObsidianServer<O> {
    fn new(inner: O) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<O: Obsidian + Send + Sync + 'static> pb::obsidian_server::Obsidian for ObsidianServer<O> {
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
        for (keyspace_id, key) in keys {
            let maybe_value = self
                .inner
                .get(ts, keyspace_id, key.clone())
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?;

            values[key_idxs[&(keyspace_id, key)]] = maybe_value;
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
        for (keyspace_id, key) in keys {
            let maybe_value = self
                .inner
                .get(ts, keyspace_id, key.clone())
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?;

            values[key_idxs[&(keyspace_id, key)]] = maybe_value;
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
            records: records
                .into_iter()
                .map(|(key, ts, value)| pb::Record {
                    key: Some(pb::Key {
                        keyspace_id: Some(keyspace_id.into()),
                        bytes: key,
                    }),
                    ts: ts.as_nanos(),
                    value,
                })
                .collect(),
            remaining: Some(maybe_continue_range.unwrap_or(Range::empty()).into()),
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
            .collect::<Result<Vec<(KeyspaceId, Vec<u8>)>, _>>()
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

fn options_to_get_results(values: Vec<Option<Vec<u8>>>) -> Vec<pb::GetResult> {
    values
        .into_iter()
        .map(|maybe_value| match maybe_value {
            Some(value) => pb::GetResult {
                result_type: Some(pb::get_result::ResultType::Value(value)),
            },
            None => pb::GetResult {
                result_type: Some(pb::get_result::ResultType::NotFound(())),
            },
        })
        .collect()
}

fn key_set_from_pb(
    keys_pb: Vec<pb::Key>,
) -> anyhow::Result<(
    BTreeSet<(KeyspaceId, Vec<u8>)>,
    BTreeMap<(KeyspaceId, Vec<u8>), usize>,
)> {
    let mut keys = BTreeSet::new();
    let mut key_idxs = BTreeMap::new();
    for (i, key_pb) in keys_pb.into_iter().enumerate() {
        let keyspace_id = KeyspaceId::try_from(
            key_pb
                .keyspace_id
                .ok_or_else(|| anyhow!("missing keyspace_id on key {}", i))?,
        )
        .map_err(|e| anyhow!("invalid keyspace_id on key {}: {}", i, e))?;
        let key = (keyspace_id, key_pb.bytes);

        if keys.contains(&key) {
            return Err(anyhow!("duplicate key {:?}", key));
        }

        keys.insert(key.clone());
        key_idxs.insert(key, i);
    }
    Ok((keys, key_idxs))
}

fn required<T, U>(name: &'static str, v: Option<T>) -> Result<U, tonic::Status>
where
    U: TryFrom<T, Error = anyhow::Error>,
{
    v.ok_or_else(|| tonic::Status::invalid_argument(format!("missing {}", name)))?
        .try_into()
        .map_err(|e: anyhow::Error| {
            tonic::Status::invalid_argument(format!("couldn't parse {}: {}", name, e.to_string()))
        })
}

fn invalid_argument(e: anyhow::Error) -> tonic::Status {
    tonic::Status::invalid_argument(e.to_string())
}

fn internal(e: anyhow::Error) -> tonic::Status {
    tonic::Status::internal(e.to_string())
}

struct Pool<T> {
    free: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<T>>,
    ret: tokio::sync::mpsc::Sender<T>,
}

impl<T: Clone> Pool<T> {
    fn new(n: usize, t: &T) -> Self {
        let (ret, free) = tokio::sync::mpsc::channel(n);
        for _ in 0..n {
            ret.try_send(t.clone())
                .expect("channel should have capacity by construction");
        }

        Pool {
            free: tokio::sync::Mutex::new(free),
            ret,
        }
    }

    async fn acquire(&self) -> PooledItem<T> {
        // unwrap is appropriate here because recv() only returns None if the sender has been
        // dropped, but the sender lives in self and we have &self here.
        let item = self.free.lock().await.recv().await.unwrap();

        PooledItem {
            item: Some(item),
            ret: self.ret.clone(),
        }
    }
}

struct PooledItem<T> {
    item: Option<T>,
    ret: tokio::sync::mpsc::Sender<T>,
}

impl<T> DerefMut for PooledItem<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // unwrap is appropriate here because item is only None once self is dropped.
        self.item.as_mut().unwrap()
    }
}

impl<T> Deref for PooledItem<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // unwrap is appropriate here because item is only None once self is dropped.
        self.item.as_ref().unwrap()
    }
}

impl<T> Drop for PooledItem<T> {
    fn drop(&mut self) {
        // unwrap is appropriate here because this is the only way that item becomes None and
        // it only happens once.
        //
        // try_send will never fail if the pool is still alive because the sender is guaranteed to
        // have capacity based on the construction of the Pool. We can get an error here only if
        // the pool is already gone, and so we don't care.
        _ = self.ret.try_send(self.item.take().unwrap());
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

    use crate::obsidian::Obsidian;
    use crate::pb;
    use crate::test::new_for_test;
    use crate::test::obsidian_test_suite;
    use crate::types::ColoGroupId;
    use crate::types::KeyspaceId;
    use crate::types::Mutation;

    #[tokio::test]
    async fn test_write() -> anyhow::Result<()> {
        let obs = new_for_test(1).await?;
        obs.create_colo_group(ColoGroupId(1), vec![] /*splits*/)
            .await?;

        let client = spawn_server(obs).await?;

        let key = (KeyspaceId(ColoGroupId(1), 1), b"abc".to_vec());

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

    obsidian_test_suite!(
        async |n_tablets: usize| -> anyhow::Result<crate::grpc::tests::ObsidianClientServer> {
            use super::spawn_server;
            use crate::test::new_for_test;

            let obs = new_for_test(n_tablets).await?;
            let client = spawn_server(obs).await?;
            Ok(client)
        }
    );

    async fn spawn_server<O: Obsidian + Send + Sync + 'static>(
        obs: O,
    ) -> anyhow::Result<ObsidianClientServer> {
        let (shutdown, on_shutdown) = oneshot::channel::<()>();
        let listener = TcpListener::bind("[::1]:0").await?;
        let addr = listener.local_addr()?;
        let server = super::ObsidianServer::new(obs);
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
            super::ObsidianClient::new(&pb::obsidian_client::ObsidianClient::connect(url).await?);

        Ok(ObsidianClientServer {
            inner: client,
            shutdown: Some(shutdown),
        })
    }

    struct ObsidianClientServer {
        inner: super::ObsidianClient,
        shutdown: Option<oneshot::Sender<()>>,
    }

    #[async_trait]
    impl Obsidian for ObsidianClientServer {
        async fn get(
            &self,
            ts: crate::types::Timestamp,
            keyspace_id: KeyspaceId,
            key: Vec<u8>,
        ) -> anyhow::Result<Option<Vec<u8>>> {
            self.inner.get(ts, keyspace_id, key).await
        }

        async fn scan_page(
            &self,
            ts: crate::types::Timestamp,
            keyspace_id: KeyspaceId,
            range: crate::range::Range<&[u8]>,
            direction: crate::types::Direction,
            limit: usize,
        ) -> anyhow::Result<(
            Vec<(Vec<u8>, crate::types::Timestamp, Vec<u8>)>,
            Option<crate::range::Range<Vec<u8>>>,
        )> {
            self.inner
                .scan_page(ts, keyspace_id, range, direction, limit)
                .await
        }

        async fn latest_snapshot(
            &self,
            keys: BTreeSet<(KeyspaceId, Vec<u8>)>,
        ) -> anyhow::Result<crate::types::Timestamp> {
            self.inner.latest_snapshot(keys).await
        }

        async fn write(
            &self,
            preconds: Vec<crate::types::Precondition>,
            muts: BTreeMap<(KeyspaceId, Vec<u8>), crate::types::Mutation>,
        ) -> Result<crate::types::Timestamp, crate::types::WriteError> {
            self.inner.write(preconds, muts).await
        }

        async fn create_colo_group(
            &self,
            colo_group_id: ColoGroupId,
            initial_splits: Vec<crate::range::Bound<Vec<u8>>>,
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
