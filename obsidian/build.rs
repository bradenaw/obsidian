fn main() -> std::io::Result<()> {
    prost_build::compile_protos(&["../proto/obsidian.proto"], &["../proto/"])?;
    prost_build::compile_protos(
        &[
            "../proto/internal/meta.proto",
            "../proto/internal/tablet.proto",
        ],
        &["../proto", "../proto/internal/"],
    )?;
    Ok(())
}
