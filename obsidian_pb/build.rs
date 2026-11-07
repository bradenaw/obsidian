fn main() -> std::io::Result<()> {
    tonic_prost_build::configure().compile_protos(&["../proto/obsidian.proto"], &["../proto/"])?;
    tonic_prost_build::configure().compile_protos(
        &[
            "../proto/internal/error.proto",
            "../proto/internal/internal.proto",
            "../proto/internal/meta.proto",
            "../proto/internal/node.proto",
            "../proto/internal/proposal.proto",
            "../proto/internal/storage_gc.proto",
        ],
        &["../proto", "../proto/internal/"],
    )?;
    tonic_prost_build::configure().compile_protos(
        &["../proto/external/journals.proto"],
        &["../proto", "../proto/external/"],
    )?;
    Ok(())
}
