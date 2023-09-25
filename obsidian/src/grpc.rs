use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::iter;
use std::ops::Deref;
use std::ops::DerefMut;

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
        _ts: Timestamp,
        _keyspace_id: KeyspaceId,
        _key: Vec<u8>,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        todo!()
    }

    async fn scan_page(
        &self,
        _ts: Timestamp,
        _keyspace_id: KeyspaceId,
        _range: Range<&[u8]>,
        _direction: Direction,
        _limit: usize,
    ) -> anyhow::Result<(Vec<(Vec<u8>, Timestamp, Vec<u8>)>, Option<Range<Vec<u8>>>)> {
        todo!()
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
        _colo_group_id: ColoGroupId,
        _initial_splits: Vec<Bound<Vec<u8>>>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn create_keyspace(&self, _keyspace_id: KeyspaceId) -> anyhow::Result<()> {
        todo!()
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
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            let maybe_value = self
                .inner
                .get(ts, key.0, key.1)
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?;

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
        req: tonic::Request<pb::WriteReq>,
    ) -> Result<tonic::Response<pb::WriteResp>, tonic::Status> {
        let req_inner = req.into_inner();

        let preconds = req_inner
            .preconds
            .into_iter()
            .map(|x| x.try_into())
            .collect::<Result<Vec<Precondition>, _>>()
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        let keys = req_inner
            .keys
            .into_iter()
            .map(|x| x.try_into())
            .collect::<Result<Vec<(KeyspaceId, Vec<u8>)>, _>>()
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        let muts = req_inner
            .muts
            .into_iter()
            .map(|x| x.try_into())
            .collect::<Result<Vec<Mutation>, _>>()
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;

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
    use futures::FutureExt;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tonic::transport::server::TcpIncoming;

    use crate::obsidian::Obsidian;
    use crate::pb;
    use crate::test::new_for_test;
    use crate::types::ColoGroupId;
    use crate::types::KeyspaceId;
    use crate::types::Mutation;

    #[tokio::test]
    async fn test_write() -> anyhow::Result<()> {
        let obs = new_for_test(1).await?;
        obs.create_colo_group(ColoGroupId(1), vec![] /*splits*/)
            .await?;

        let (shutdown, on_shutdown) = oneshot::channel::<()>();
        let listener = TcpListener::bind("[::1]:0").await?;
        let addr = listener.local_addr()?;
        let serve = tonic::transport::Server::builder()
            .add_service(pb::obsidian_server::ObsidianServer::new(
                super::ObsidianServer::new(obs),
            ))
            .serve_with_incoming_shutdown(
                TcpIncoming::from_listener(
                    listener, true, /*nodelay*/
                    None, /*keepalive*/
                )
                .map_err(|e| anyhow!("{}", e))?,
                on_shutdown.map(|_| ()),
            );

        tokio::spawn(async move {
            serve.await.expect("got error serving");
        });

        let client = super::ObsidianClient::new(
            &pb::obsidian_client::ObsidianClient::connect(
                "http://".to_string() + &addr.to_string(),
            )
            .await?,
        );

        let key = (KeyspaceId(ColoGroupId(1), 1), b"abc".to_vec());

        let write_ts = client
            .write(
                vec![],
                BTreeMap::from([(key.clone(), Mutation::Put(b"def".to_vec()))]),
            )
            .await?;

        let snapshot_ts = client.latest_snapshot(BTreeSet::from([key])).await?;

        assert_eq!(write_ts, snapshot_ts);

        _ = shutdown.send(());

        Ok(())
    }
}
