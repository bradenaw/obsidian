use async_trait::async_trait;

#[allow(clippy::len_without_is_empty)]
#[async_trait]
pub trait FileReader: Sync + Send + 'static {
    /// Fills `buf` with the bytes of the file starting at `offset`. Returns an error if the end of
    /// the file is reached before filling `buf`.
    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<()>;
    /// Returns the length of the file in bytes.
    fn len(&self) -> u64;
}
