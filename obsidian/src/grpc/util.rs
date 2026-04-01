use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::iter;
use std::ops::Deref;
use std::ops::DerefMut;

use anyhow::anyhow;

use crate::pb;
use crate::Direction;
use crate::Key;
use crate::KeyspaceId;
use crate::Mutation;
use crate::Precondition;
use crate::Range;
use crate::Record;
use crate::Timestamp;

pub(super) fn get_req_results(
    keys_pb: Vec<pb::Key>,
) -> anyhow::Result<(BTreeSet<Key>, GetResultsBuilder)> {
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

    Ok((keys, GetResultsBuilder { key_idxs }))
}

pub(super) struct GetResultsBuilder {
    key_idxs: BTreeMap<Key, usize>,
}

impl GetResultsBuilder {
    pub fn build(self, records: BTreeMap<Key, Record>) -> anyhow::Result<Vec<pb::GetResult>> {
        let mut results: Vec<pb::GetResult> = Vec::new();
        for _ in 0..self.key_idxs.len() {
            results.push(pb::GetResult {
                result_type: Some(pb::get_result::ResultType::NotFound(())),
            });
        }

        for (key, record) in records {
            let idx = self
                .key_idxs
                .get(&key)
                .ok_or_else(|| anyhow!("got response for not-requested key {:?}", key))?;
            results[*idx] = pb::GetResult {
                result_type: Some(pb::get_result::ResultType::Record(record.into())),
            };
        }

        Ok(results)
    }
}

pub(super) fn parse_scan_req(
    req_pb: pb::ScanReq,
) -> anyhow::Result<(Timestamp, KeyspaceId, Range<Vec<u8>>, Direction, usize)> {
    let snapshot_ts = Timestamp::from_micros(req_pb.snapshot_ts);
    let keyspace_id: KeyspaceId = required("keyspace_id", req_pb.keyspace_id)?;
    let range: Range<Vec<u8>> = required("range", req_pb.range)?;
    let direction: Direction = pb::Direction::from_i32(req_pb.direction)
        .ok_or_else(|| anyhow!("unknown direction"))?
        .try_into()
        .map_err(invalid_argument)?;
    let limit = usize::try_from(req_pb.limit).map_err(|_| anyhow!("invalid limit"))?;

    Ok((snapshot_ts, keyspace_id, range, direction, limit))
}

pub(super) fn parse_write_req(
    req_pb: pb::WriteReq,
) -> anyhow::Result<(Vec<Precondition>, BTreeMap<Key, Mutation>)> {
    let preconds = req_pb
        .preconds
        .into_iter()
        .map(|x| x.try_into())
        .collect::<Result<Vec<Precondition>, _>>()?;
    let keys = req_pb
        .keys
        .into_iter()
        .map(|x| x.try_into())
        .collect::<Result<Vec<Key>, _>>()?;
    let muts = req_pb
        .muts
        .into_iter()
        .map(|x| x.try_into())
        .collect::<Result<Vec<Mutation>, _>>()?;

    let mut muts_map = BTreeMap::new();
    if keys.len() != muts.len() {
        return Err(anyhow!(
            "keys and muts must have the same number of elements",
        ));
    }
    for (key, m) in iter::zip(keys, muts) {
        if muts_map.contains_key(&key) {
            return Err(anyhow!("duplicate key {:?}", key));
        }
        muts_map.insert(key, m);
    }
    Ok((preconds, muts_map))
}

pub(crate) fn scan_req_to_proto(
    ts: Timestamp,
    keyspace_id: KeyspaceId,
    range: Range<&[u8]>,
    direction: Direction,
    limit: usize,
) -> anyhow::Result<pb::ScanReq> {
    Ok(pb::ScanReq {
        snapshot_ts: ts.as_micros(),
        keyspace_id: Some(keyspace_id.into()),
        range: Some(range.to_vec().into()),
        direction: pb::Direction::from(direction).into(),
        limit: u64::try_from(limit)?,
    })
}

pub(super) fn preconds_muts_to_proto(
    preconds: Vec<Precondition>,
    muts: BTreeMap<Key, Mutation>,
) -> (Vec<pb::Precondition>, Vec<pb::Key>, Vec<pb::Mutation>) {
    let preconds_pb: Vec<_> = preconds.into_iter().map(pb::Precondition::from).collect();
    let mut keys_pb = Vec::with_capacity(muts.len());
    let mut muts_pb = Vec::with_capacity(muts.len());
    for (key, m) in muts.into_iter() {
        keys_pb.push(pb::Key::from(key));
        muts_pb.push(pb::Mutation::from(m));
    }
    (preconds_pb, keys_pb, muts_pb)
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
