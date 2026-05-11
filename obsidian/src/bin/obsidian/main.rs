use obsidian::cmd_main;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    cmd_main().await
}
