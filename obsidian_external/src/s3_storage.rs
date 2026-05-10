use std::io;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use tokio::io::AsyncReadExt;

use crate::FileName;
use crate::FileReader;
use crate::FileWriter;
use crate::Storage;

pub struct S3Storage {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl S3Storage {
    pub fn new(client: aws_sdk_s3::Client, bucket: String) -> Self {
        Self { client, bucket }
    }
}

#[async_trait]
impl Storage for S3Storage {
    async fn put(&self, name: FileName) -> anyhow::Result<Box<dyn FileWriter>> {
        Ok(Box::new(S3FileWriter {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            name,
            body: Some(vec![]),
        }))
    }

    async fn delete(&self, name: FileName) -> anyhow::Result<()> {
        self.client
            .delete_object()
            .bucket(self.bucket.clone())
            .key(file_name_to_key(name))
            .send()
            .await?;
        Ok(())
    }

    async fn get(&self, name: FileName) -> anyhow::Result<Arc<dyn FileReader>> {
        let attrs = self
            .client
            .get_object_attributes()
            .bucket(self.bucket.clone())
            .key(file_name_to_key(name.clone()))
            .send()
            .await?;

        let len = attrs
            .object_size
            .ok_or_else(|| anyhow!("no size in object attrs"))? as u64;

        Ok(Arc::new(S3FileReader {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            name,
            len,
        }))
    }
}

struct S3FileReader {
    client: aws_sdk_s3::Client,
    bucket: String,
    name: FileName,
    len: u64,
}

#[async_trait]
impl FileReader for S3FileReader {
    fn len(&self) -> u64 {
        self.len
    }

    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<()> {
        let resp = self
            .client
            .get_object()
            .bucket(self.bucket.clone())
            .key(file_name_to_key(self.name.clone()))
            .range(format!("bytes={}-{}", offset, offset + (buf.len() as u64)))
            .send()
            .await?;

        let content_length = resp
            .content_length
            .ok_or_else(|| anyhow!("missing content-length"))?;
        if content_length != buf.len() as i64 {
            return Err(anyhow!(
                "response wrong size: expected {}, got {}",
                buf.len(),
                content_length
            ));
        }

        resp.body.into_async_read().read_exact(buf).await?;

        Ok(())
    }
}

struct S3FileWriter {
    client: aws_sdk_s3::Client,
    bucket: String,
    name: FileName,

    body: Option<Vec<u8>>,
}

#[async_trait]
impl FileWriter for S3FileWriter {
    async fn write_all(&mut self, src: &[u8]) -> io::Result<()> {
        let body = self
            .body
            .as_mut()
            .ok_or_else(|| io::Error::other("already shutdown"))?;
        body.extend_from_slice(src);
        Ok(())
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        let body = self
            .body
            .take()
            .ok_or_else(|| io::Error::other("already shutdown"))?;
        self.client
            .put_object()
            .bucket(self.bucket.clone())
            .key(file_name_to_key(self.name.clone()))
            .body(ByteStream::from(body))
            .send()
            .await
            .map_err(io::Error::other)?;
        Ok(())
    }
}

fn file_name_to_key(file_name: FileName) -> String {
    match file_name {
        FileName::Run(run_id) => format!("/run/{}", run_id),
    }
}
