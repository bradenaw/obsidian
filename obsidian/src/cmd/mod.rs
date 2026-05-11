use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::sync::Arc;

use obsidian_external::ConsulNodeDiscovery;
use obsidian_external::S3Storage;
use obsidian_pb as pb;
use rs_consul::Consul;
use tokio::net::TcpListener;
use tonic::transport::server::TcpIncoming;

use crate::discovery::Discovery;
use crate::grpc::GrpcNodes;
use crate::grpc::JournalsClient;
use crate::grpc::NodeServer;
use crate::meta::MetaSynced;
use crate::node::Node;
use crate::runtime::Nodes;
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

pub async fn cmd_main() -> anyhow::Result<()> {
    pretty_env_logger::init();

    let cli: Cli = clap::Parser::parse();

    match cli.command {
        Command::Node(args) => cmd_node(args).await?,
    }

    Ok(())
}

async fn cmd_node(args: NodeArgs) -> anyhow::Result<()> {
    let addr = IpAddr::V6(Ipv6Addr::LOCALHOST);
    let listener = TcpListener::bind(format!("{}:{}", addr, args.port)).await?;
    let node_id = NodeId::new(addr, listener.local_addr()?.port());

    let node_discovery = ConsulNodeDiscovery::new(
        node_id,
        Consul::new({
            let mut config = rs_consul::Config::default();
            config.address = args.consul_addr;
            config
        }),
        args.consul_service,
    );
    let nodes = Arc::new(GrpcNodes::new(Arc::new(node_discovery)));

    let discovery = Discovery::new(Arc::clone(&nodes) as Arc<dyn Nodes>);

    let storage = S3Storage::new(
        aws_sdk_s3::Client::new(&aws_config::load_from_env().await),
        args.s3_bucket,
    );

    let meta_synced = MetaSynced::new(discovery.meta());

    let journals = JournalsClient::new(
        pb::external::journals_client::JournalsClient::connect(args.journals_addr).await?,
    );

    let node = Node::new(
        node_id,
        nodes,
        Arc::new(storage),
        discovery.meta(),
        Arc::new(discovery),
        Arc::new(meta_synced),
        Arc::new(journals),
    );

    let serve = tonic::transport::Server::builder()
        .add_service(pb::internal::node_server::NodeServer::new(NodeServer::new(
            Arc::new(node),
        )))
        .serve_with_incoming(
            TcpIncoming::from_listener(listener, true /*nodelay*/, None /*keepalive*/).unwrap(),
        );
    serve.await?;
    Ok(())
}
