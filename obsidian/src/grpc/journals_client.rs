use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use async_stream::try_stream;
use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;
use obsidian_external::Journal;
use obsidian_external::Journals;
use obsidian_pb as pb;
use obsidian_util::encode;
use obsidian_util::Decode;
use obsidian_util::Encode;

use crate::JournalSeq;
use crate::ShardId;

struct JournalsClient<E>
where
    E: Encode + Decode + Send + 'static,
{
    phantom: PhantomData<E>,
    grpc_client: pb::external::journals_client::JournalsClient<tonic::transport::Channel>,
}

#[async_trait]
impl<E> Journals<E> for JournalsClient<E>
where
    E: Encode + Decode + Send + Sync + 'static,
{
    async fn journal(&self, shard_id: ShardId) -> Arc<dyn Journal<E>> {
        Arc::new(JournalClient {
            phantom: PhantomData::default(),
            shard_id,
            grpc_client: self.grpc_client.clone(),
        })
    }
}

struct JournalClient<E> {
    phantom: PhantomData<E>,
    shard_id: ShardId,
    grpc_client: pb::external::journals_client::JournalsClient<tonic::transport::Channel>,
}

#[async_trait]
impl<E> Journal<E> for JournalClient<E>
where
    E: Encode + Decode + Send + Sync + 'static,
{
    async fn append(&self, entry: E) -> anyhow::Result<JournalSeq> {
        let resp = self
            .grpc_client
            .clone()
            .append(pb::external::JournalAppendReq {
                journal_name: Some(journal_name(self.shard_id)),
                entry: encode(&entry),
            })
            .await?;

        Ok(JournalSeq(resp.into_inner().seq))
    }

    fn tail(
        &self,
        first: JournalSeq,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<(JournalSeq, E)>> + Send + '_>> {
        Box::pin(try_stream! {
            let resp = self.grpc_client.clone().tail(pb::external::JournalTailReq {
                journal_name: Some(journal_name(self.shard_id)),
                first: first.0,
            }).await?;
            let mut stream = resp.into_inner();

            while let Some(stream_item) = stream.next().await.transpose()? {
                for entry_pb in stream_item.entries {
                    let seq = JournalSeq(entry_pb.seq);
                    let entry = E::decode(&entry_pb.entry)?;

                    yield (seq, entry);
                }
            }
        })
    }

    async fn oldest_available(&self) -> anyhow::Result<JournalSeq> {
        let resp = self
            .grpc_client
            .clone()
            .oldest_available(pb::external::JournalNameReq {
                journal_name: Some(journal_name(self.shard_id)),
            })
            .await?;

        Ok(JournalSeq(resp.into_inner().seq))
    }

    async fn latest(&self) -> anyhow::Result<JournalSeq> {
        let resp = self
            .grpc_client
            .clone()
            .latest(pb::external::JournalNameReq {
                journal_name: Some(journal_name(self.shard_id)),
            })
            .await?;

        Ok(JournalSeq(resp.into_inner().seq))
    }

    async fn trim(&self, _before: JournalSeq) -> anyhow::Result<()> {
        todo!()
    }
}

fn journal_name(shard_id: ShardId) -> pb::external::JournalName {
    pb::external::JournalName {
        journal_name: Some(pb::external::journal_name::JournalName::ShardId(shard_id.0)),
    }
}
