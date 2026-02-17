use std::pin::Pin;
use std::task::Poll;

use tokio::io::AsyncWrite;

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

impl AsyncWrite for MemFileWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let self_ = Pin::get_mut(self);
        self_.inner.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }
}
