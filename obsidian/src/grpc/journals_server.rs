use std::pin::Pin;
use std::sync::Arc;

use async_stream::try_stream;
use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;

use crate::pb;
use crate::pb::external::JournalName;
use crate::runtime::Journals;
use crate::util::encode;
use crate::util::Decode;
use crate::util::Encode;
use crate::JournalSeq;
use crate::ShardId;

pub(crate) struct JournalsServer<E> {
    inner: Arc<dyn Journals<E>>,
}

#[async_trait]
impl<E> pb::external::journals_server::Journals for JournalsServer<E>
where
    E: Encode + Decode + Send + 'static,
{
    async fn append(
        &self,
        req: tonic::Request<pb::external::JournalAppendReq>,
    ) -> Result<tonic::Response<pb::external::JournalSeqResp>, tonic::Status> {
        let req_inner = req.into_inner();
        let shard_id = shard_id_from_journal_name(req_inner.journal_name)?;
        let entry = E::decode(&req_inner.entry)
            .map_err(|e| tonic::Status::invalid_argument(format!("couldn't parse entry: {}", e)))?;

        let journal = self.inner.journal(shard_id).await;
        let seq = journal
            .append(entry)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(pb::external::JournalSeqResp {
            seq: seq.0,
        }))
    }

    type TailStream =
        Pin<Box<dyn Stream<Item = Result<pb::external::JournalTailResp, tonic::Status>> + Send>>;
    async fn tail(
        &self,
        req: tonic::Request<pb::external::JournalTailReq>,
    ) -> Result<tonic::Response<Self::TailStream>, tonic::Status> {
        let req_inner = req.into_inner();
        let shard_id = shard_id_from_journal_name(req_inner.journal_name)?;
        let first = JournalSeq(req_inner.first);

        let journal = self.inner.journal(shard_id).await;
        Ok(tonic::Response::new(Box::pin(try_stream! {
            let mut stream = journal.tail(first);
            while let Some((seq, entry)) = stream
                .next()
                .await
                .transpose()
                .map_err(|e| tonic::Status::internal(e.to_string()))?
            {
                yield pb::external::JournalTailResp {
                    entries: vec![pb::external::JournalTailEntry{
                        seq: seq.0,
                        entry: encode(&entry),
                    }],
                };
            }
        })))
    }

    async fn oldest_available(
        &self,
        req: tonic::Request<pb::external::JournalNameReq>,
    ) -> Result<tonic::Response<pb::external::JournalSeqResp>, tonic::Status> {
        let req_inner = req.into_inner();
        let shard_id = shard_id_from_journal_name(req_inner.journal_name)?;

        let journal = self.inner.journal(shard_id).await;
        Ok(tonic::Response::new(pb::external::JournalSeqResp {
            seq: journal
                .oldest_available()
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?
                .0,
        }))
    }

    async fn latest(
        &self,
        req: tonic::Request<pb::external::JournalNameReq>,
    ) -> Result<tonic::Response<pb::external::JournalSeqResp>, tonic::Status> {
        let req_inner = req.into_inner();
        let shard_id = shard_id_from_journal_name(req_inner.journal_name)?;

        let journal = self.inner.journal(shard_id).await;
        Ok(tonic::Response::new(pb::external::JournalSeqResp {
            seq: journal
                .latest()
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?
                .0,
        }))
    }
}

fn shard_id_from_journal_name(journal_name: Option<JournalName>) -> Result<ShardId, tonic::Status> {
    match journal_name
        .ok_or_else(|| tonic::Status::invalid_argument("missing journal_name"))?
        .journal_name
        .ok_or_else(|| tonic::Status::invalid_argument("missing journal_name"))?
    {
        pb::external::journal_name::JournalName::ShardId(shard_id_raw) => Ok(ShardId(shard_id_raw)),
    }
}
