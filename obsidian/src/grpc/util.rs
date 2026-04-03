use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::iter;
use std::ops::Deref;
use std::ops::DerefMut;

use anyhow::anyhow;
use prost::Message;
use tonic::metadata::MetadataValue;

use crate::pb;
use crate::Direction;
use crate::InternalError;
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

static INTERNAL_ERROR_METADATA_KEY: &str = "obsidian-internal-error-bin";

pub(super) fn internal_err_to_status(err: InternalError) -> tonic::Status {
    let msg = err.to_string();

    let mut status = match err {
        InternalError::Conflict(_) => tonic::Status::failed_precondition(msg),
        InternalError::AlreadyCommitted => tonic::Status::already_exists(msg),
        InternalError::AlreadyAborted => tonic::Status::already_exists(msg),
        InternalError::PreconditionFailed => {
            // This is a mismatch from immediate expectation but matches the criteria from
            // https://grpc.io/docs/guides/status-codes/:
            //
            // - Use ABORTED if the client should retry at a higher level (e.g., when a
            //   client-specified test-and-set fails, indicating the client should restart a
            //   read-modify-write sequence).
            //
            // - Use FAILED_PRECONDITION if the client should not retry until the system state has
            //   been explicitly fixed. E.g., if an “rmdir” fails because the directory is
            //   non-empty, FAILED_PRECONDITION should be returned since the client should not
            //   retry unless the files are deleted from the directory.
            tonic::Status::aborted(msg)
        }
        InternalError::TxOutcomeMissing => tonic::Status::not_found(msg),
        InternalError::TabletNotReadable(_) => tonic::Status::failed_precondition(msg),
        InternalError::TabletNotWriteable(_) => tonic::Status::failed_precondition(msg),
        InternalError::TabletNotHydrating(_) => tonic::Status::failed_precondition(msg),
        // This is not supposed to be returned this way.
        InternalError::PartialGet { .. } => tonic::Status::internal(msg),
        InternalError::NotLeader(_) => tonic::Status::failed_precondition(msg),
        InternalError::Other(_) => tonic::Status::internal(msg),
    };

    let err_pb = match pb::internal::InternalError::try_from(err) {
        Ok(err_pb) => err_pb,
        Err(_) => return status,
    };

    let err_bytes = err_pb.encode_to_vec();

    status.metadata_mut().insert_bin(
        INTERNAL_ERROR_METADATA_KEY,
        MetadataValue::from_bytes(&err_bytes[..]),
    );

    status
}

pub(super) fn internal_err_from_status(status: tonic::Status) -> InternalError {
    let value = match status.metadata().get_bin(INTERNAL_ERROR_METADATA_KEY) {
        Some(value) => value,
        None => return InternalError::Other(anyhow::Error::msg(status.message().to_string())),
    };

    let bytes = match value.to_bytes() {
        Ok(bytes) => bytes,
        Err(_) => return InternalError::Other(anyhow::Error::msg(status.message().to_string())),
    };

    let err_pb = match pb::internal::InternalError::decode(bytes) {
        Ok(err_pb) => err_pb,
        Err(_) => return InternalError::Other(anyhow::Error::msg(status.message().to_string())),
    };

    match InternalError::try_from(err_pb) {
        Ok(err) => err,
        Err(_) => return InternalError::Other(anyhow::Error::msg(status.message().to_string())),
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use crate::grpc::util::internal_err_from_status;
    use crate::grpc::util::internal_err_to_status;
    use crate::InternalError;
    use crate::ShardId;
    use crate::TabletId;
    use crate::Txid;

    #[test]
    fn test_internal_error_roundtrip() {
        let txid = Txid::new(ShardId(5));
        let tablet_id = TabletId(ShardId(17), 1234);
        let shard_id = ShardId(8151);

        match internal_err_from_status(internal_err_to_status(InternalError::Conflict(txid))) {
            InternalError::Conflict(other_txid) => assert_eq!(txid, other_txid),
            e => panic!("{}", e),
        }

        assert_matches!(
            internal_err_from_status(internal_err_to_status(InternalError::AlreadyCommitted)),
            InternalError::AlreadyCommitted,
        );
        assert_matches!(
            internal_err_from_status(internal_err_to_status(InternalError::AlreadyAborted)),
            InternalError::AlreadyAborted,
        );
        assert_matches!(
            internal_err_from_status(internal_err_to_status(InternalError::PreconditionFailed)),
            InternalError::PreconditionFailed,
        );
        assert_matches!(
            internal_err_from_status(internal_err_to_status(InternalError::TxOutcomeMissing)),
            InternalError::TxOutcomeMissing,
        );

        match internal_err_from_status(internal_err_to_status(InternalError::TabletNotReadable(
            tablet_id,
        ))) {
            InternalError::TabletNotReadable(other_tablet_id) => {
                assert_eq!(tablet_id, other_tablet_id)
            }
            e => panic!("{}", e),
        }
        match internal_err_from_status(internal_err_to_status(InternalError::TabletNotWriteable(
            tablet_id,
        ))) {
            InternalError::TabletNotWriteable(other_tablet_id) => {
                assert_eq!(tablet_id, other_tablet_id)
            }
            e => panic!("{}", e),
        }
        match internal_err_from_status(internal_err_to_status(InternalError::TabletNotHydrating(
            tablet_id,
        ))) {
            InternalError::TabletNotHydrating(other_tablet_id) => {
                assert_eq!(tablet_id, other_tablet_id)
            }
            e => panic!("{}", e),
        }
        match internal_err_from_status(internal_err_to_status(InternalError::NotLeader(shard_id))) {
            InternalError::NotLeader(other_shard_id) => assert_eq!(shard_id, other_shard_id),
            e => panic!("{}", e),
        }
    }
}
