use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::sync::Arc;

use obsidian_common::JournalEntry;
use obsidian_external::mem::MemJournals;
use obsidian_external::ConsulNodeDiscovery;
use obsidian_external::S3Storage;
use obsidian_pb as pb;
use rs_consul::Consul;
use tokio::net::TcpListener;
use tonic::transport::server::TcpIncoming;

use crate::discovery::Discovery;
use crate::election::Proposal;
use crate::gateway::Gateway;
use crate::grpc::GatewayServer;
use crate::grpc::GrpcNodes;
use crate::grpc::JournalsClient;
use crate::grpc::JournalsServer;
use crate::grpc::NodeServer;
use crate::meta::MetaSynced;
use crate::node::Node;
use crate::runtime::Nodes;
use crate::runtime::Shards;
use crate::NodeId;

#[derive(clap::Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Run an Obsidian node.
    Node(NodeArgs),
    /// Run an in-memory journals server for use in tests.
    TestJournals(TestJournalsArgs),
}

pub async fn cmd_main() -> anyhow::Result<()> {
    pretty_env_logger::init_timed();

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .unwrap();

    let cli: Cli = clap::Parser::parse();

    match cli.command {
        Command::Node(args) => cmd_node(args).await?,
        Command::TestJournals(args) => cmd_test_journals(args).await?,
    }

    Ok(())
}

#[derive(clap::Args, Debug)]
struct NodeArgs {
    #[arg(long)]
    journals_addr: String,

    #[arg(long)]
    s3_bucket: String,

    #[arg(long)]
    consul_addr: String,

    #[arg(long)]
    consul_service: String,

    #[arg(long, default_value_t = 0)]
    port: u16,
}

async fn cmd_node(args: NodeArgs) -> anyhow::Result<()> {
    let addr = IpAddr::V6(Ipv6Addr::LOCALHOST);
    let listener = TcpListener::bind(format!("{}:{}", addr, args.port)).await?;
    let node_id = NodeId::new(addr, listener.local_addr()?.port());

    log::info!("starting {:?}", node_id);

    let node_discovery = ConsulNodeDiscovery::join(
        node_id,
        Consul::new({
            let mut config = rs_consul::Config::default();
            config.address = args.consul_addr;
            config
        }),
        args.consul_service,
    );
    let nodes = Arc::new(GrpcNodes::new(Arc::new(node_discovery)));

    let discovery = Arc::new(Discovery::new(Arc::clone(&nodes) as Arc<dyn Nodes>));

    let storage = S3Storage::new(
        aws_sdk_s3::Client::new(
            &aws_config::from_env()
                .endpoint_url("http://[::1]:9000")
                .load()
                .await,
        ),
        args.s3_bucket,
    );

    let meta_synced = Arc::new(MetaSynced::new(discovery.meta()));

    let journals = JournalsClient::new(
        pb::external::journals_client::JournalsClient::connect(args.journals_addr).await?,
    );

    let node = Node::new(
        node_id,
        nodes,
        Arc::new(storage),
        discovery.meta(),
        Arc::clone(&discovery) as Arc<dyn Shards>,
        Arc::clone(&meta_synced),
        Arc::new(journals),
    );

    let gateway = Gateway::new(
        discovery.meta(),
        MetaSynced::new(discovery.meta()),
        discovery,
    );

    log::info!("starting to serve grpc");
    tonic::transport::Server::builder()
        .add_service(pb::internal::node_server::NodeServer::new(NodeServer::new(
            Arc::new(node),
        )))
        .add_service(pb::obsidian_server::ObsidianServer::new(
            GatewayServer::new(Arc::new(gateway)),
        ))
        .serve_with_incoming(
            TcpIncoming::from_listener(listener, true /*nodelay*/, None /*keepalive*/).unwrap(),
        )
        .await?;
    Ok(())
}

#[derive(clap::Args, Debug)]
struct TestJournalsArgs {
    #[arg(long, default_value_t = 0)]
    port: u16,
}

async fn cmd_test_journals(args: TestJournalsArgs) -> anyhow::Result<()> {
    let addr = IpAddr::V6(Ipv6Addr::LOCALHOST);
    let listener = TcpListener::bind(format!("{}:{}", addr, args.port)).await?;
    log::info!(
        "running test journals on port {}",
        listener.local_addr()?.port(),
    );
    tonic::transport::Server::builder()
        .add_service(pb::external::journals_server::JournalsServer::new(
            JournalsServer::new(Arc::new(MemJournals::<Proposal<JournalEntry>>::new())),
        ))
        .serve_with_incoming(
            TcpIncoming::from_listener(listener, true /*nodelay*/, None /*keepalive*/).unwrap(),
        )
        .await?;
    Ok(())
}
