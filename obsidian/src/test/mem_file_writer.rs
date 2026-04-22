use std::io;

use async_trait::async_trait;

use crate::runtime::FileWriter;
use crate::test::MemFileReader;

pub(crate) struct MemFileWriter {
    inner: Vec<u8>,
}

impl MemFileWriter {
    pub fn new() -> Self {
        Self { inner: vec![] }
    }

    pub fn into_reader(self) -> MemFileReader {
        MemFileReader::new(self.inner)
    }
}

#[async_trait]
impl FileWriter for MemFileWriter {
    async fn write_all(&mut self, src: &[u8]) -> io::Result<()> {
        self.inner.extend_from_slice(src);
        Ok(())
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        Ok(())
    }
}
