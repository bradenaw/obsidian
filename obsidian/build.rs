fn main() -> std::io::Result<()> {
    prost_build::compile_protos(
        &["../proto/meta.proto", "../proto/obsidian.proto"],
        &["../proto/"],
    )?;
    Ok(())
}
