use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::ops::Deref;
use std::ops::DerefMut;

use anyhow::anyhow;

use crate::pb;
use crate::types::KeyspaceId;

pub(super) fn options_to_get_results(values: Vec<Option<Vec<u8>>>) -> Vec<pb::GetResult> {
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

pub(super) fn key_set_from_pb(
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

pub(super) fn required<T, U>(name: &'static str, v: Option<T>) -> Result<U, tonic::Status>
where
    U: TryFrom<T, Error = anyhow::Error>,
{
    v.ok_or_else(|| tonic::Status::invalid_argument(format!("missing {}", name)))?
        .try_into()
        .map_err(|e: anyhow::Error| {
            tonic::Status::invalid_argument(format!("couldn't parse {}: {}", name, e.to_string()))
        })
}

pub(super) fn invalid_argument(e: anyhow::Error) -> tonic::Status {
    tonic::Status::invalid_argument(e.to_string())
}

pub(super) fn internal(e: anyhow::Error) -> tonic::Status {
    tonic::Status::internal(e.to_string())
}

pub(super) struct Pool<T> {
    free: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<T>>,
    ret: tokio::sync::mpsc::Sender<T>,
}

impl<T: Clone> Pool<T> {
    pub(super) fn new(n: usize, t: &T) -> Self {
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

    pub(super) async fn acquire(&self) -> PooledItem<T> {
        // unwrap is appropriate here because recv() only returns None if the sender has been
        // dropped, but the sender lives in self and we have &self here.
        let item = self.free.lock().await.recv().await.unwrap();

        PooledItem {
            item: Some(item),
            ret: self.ret.clone(),
        }
    }
}

pub(super) struct PooledItem<T> {
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
