fn main() -> std::io::Result<()> {
    tonic_build::configure().compile(&["../proto/obsidian.proto"], &["../proto/"])?;
    tonic_build::configure().compile(
        &[
            "../proto/internal/meta.proto",
            "../proto/internal/tablet.proto",
        ],
        &["../proto", "../proto/internal/"],
    )?;
    Ok(())
}
