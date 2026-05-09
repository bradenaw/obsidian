fn main() -> std::io::Result<()> {
    tonic_build::configure().compile(&["../proto/obsidian.proto"], &["../proto/"])?;
    tonic_build::configure().compile(
        &[
            "../proto/internal/error.proto",
            "../proto/internal/internal.proto",
            "../proto/internal/meta.proto",
            "../proto/internal/node.proto",
            "../proto/internal/proposal.proto",
            "../proto/internal/tablet.proto",
        ],
        &["../proto", "../proto/internal/"],
    )?;
    tonic_build::configure().compile(
        &["../proto/external/journals.proto"],
        &["../proto", "../proto/external/"],
    )?;
    Ok(())
}
