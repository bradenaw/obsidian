pub trait FromProtoEnum: Sized {
    /// Converts from the native proto representation. Returns Ok(None) for zero, taken to mean
    /// unset.
    fn try_from_i32(v: i32) -> anyhow::Result<Option<Self>>;
}
