use async_trait::async_trait;

use crate::runtime::FileReader;

pub(crate) struct MemFileReader {
    inner: Vec<u8>,
}

impl MemFileReader {
    pub fn new(inner: Vec<u8>) -> Self {
        Self{inner}
    }
}

#[async_trait]
impl FileReader for MemFileReader {
    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<()> {
        if offset + (buf.len() as u64) > self.inner.len() as u64 {
            return Err(
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "unexpected eof").into(),
            );
        }
        Ok(buf.copy_from_slice(&self.inner[(offset as usize)..(offset as usize) + buf.len()]))
    }

    fn len(&self) -> u64 {
        self.inner.len() as u64
    }
}
