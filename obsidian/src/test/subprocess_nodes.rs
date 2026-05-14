use std::collections::HashMap;
use std::env;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::path::PathBuf;
use std::sync::Arc;

use obsidian_common::NodeId;
use obsidian_external::ConsulNodeDiscovery;
use rs_consul::Consul;
use tokio::process::Child;
use tokio::process::Command;

use crate::discovery::Discovery;
use crate::grpc::GrpcNodes;

const S3_PORT: u16 = 9000;
const S3_CONSOLE_PORT: u16 = 9001;
const CONSUL_PORT: u16 = 8500;
const JOURNALS_PORT: u16 = 8000;
const NODE_PORTS_START: u16 = 8001;

const MINIO_IMAGE: &str =
    "docker.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e";
const CONSUL_IMAGE: &str = "docker.io/hashicorp/consul@sha256:a230dcea0bb107bd7958a912d1429fb7f9d399637de7ffb814b34412b9e8c543";
const CONSUL_SERVICE: &str = "obsidian";

struct SubprocessNodes {
    discovery: Arc<Discovery>,
    cargo_bin: PathBuf,
    storage: Child,
    consul: Child,
    journals: Child,
    next_port: u16,
    nodes: HashMap<NodeId, Child>,
}

impl SubprocessNodes {
    fn new() -> anyhow::Result<Self> {
        let consul_service = "obsidian";

        let storage = Command::new("podman")
            .arg("run")
            .arg("-p")
            .arg(format!("{}:{}", S3_PORT, S3_PORT))
            .arg("-p")
            .arg(format!("{}:{}", S3_CONSOLE_PORT, S3_CONSOLE_PORT))
            .arg(MINIO_IMAGE.to_string())
            .arg("server")
            .arg("/data")
            .arg("--address")
            .arg(format!(":{}", S3_PORT))
            .arg("--console-address")
            .arg(format!(":{}", S3_CONSOLE_PORT))
            .kill_on_drop(true)
            .spawn()?;

        let consul = Command::new("podman")
            .arg("run")
            .arg("-p")
            .arg(format!("{}:{}", CONSUL_PORT, CONSUL_PORT))
            .arg(CONSUL_IMAGE.to_string())
            .arg("--grpc-port")
            .arg(CONSUL_PORT.to_string())
            .kill_on_drop(true)
            .spawn()?;

        let cargo_bin = PathBuf::from(env::var("CARGO_BIN_PATH")?.to_string());

        let journals = Command::new(cargo_bin.join("obsidian"))
            .env("RUST_LOG", "info")
            .env("RUST_BACKTRACE", "1")
            .arg("test-journals")
            .arg("--port")
            .arg(JOURNALS_PORT.to_string())
            .kill_on_drop(true)
            .spawn()?;

        let node_discovery = ConsulNodeDiscovery::observe(
            Consul::new({
                let mut config = rs_consul::Config::default();
                config.address = format!("http://[::1]:{}", CONSUL_PORT);
                config
            }),
            consul_service.to_string(),
        );

        let nodes = GrpcNodes::new(Arc::new(node_discovery));
        let discovery = Arc::new(Discovery::new(Arc::new(nodes)));

        Ok(Self {
            cargo_bin,

            discovery,
            storage,
            consul,
            journals,
            next_port: NODE_PORTS_START,
            nodes: HashMap::new(),
        })
    }

    pub fn discovery(&self) -> Arc<Discovery> {
        Arc::clone(&self.discovery)
    }

    pub async fn create_node(&mut self) -> anyhow::Result<NodeId> {
        let port = self.next_port;
        self.next_port += 1;
        let addr = IpAddr::V6(Ipv6Addr::LOCALHOST);
        let node_id = NodeId::new(addr, port);

        let node = Command::new(self.cargo_bin.join("obsidian"))
            .env("RUST_BACKTRACE", "1")
            .env("RUST_LOG", "info")
            .env("AWS_REGION", "us-west-2")
            .env("AWS_ACCESS_KEY_ID", "minioadmin")
            .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
            .arg("node")
            .arg("--node-id")
            .arg(node_id.to_string())
            .arg("--port")
            .arg(format!("{}", port))
            .arg("--journals-addr")
            .arg(format!("http://[::1]:{}", JOURNALS_PORT))
            .arg("--s3-addr")
            .arg(format!("http://[::1]:{}", S3_PORT))
            .arg("--s3-bucket")
            .arg("obsidian")
            .arg("--consul-addr")
            .arg(format!("http://[::1]:{}", CONSUL_PORT))
            .arg("--consul-service")
            .arg(CONSUL_SERVICE.to_string())
            .kill_on_drop(true)
            .spawn()?;

        self.nodes.insert(node_id, node);

        Ok(node_id)
    }

    pub fn remove_node(&mut self, node_id: NodeId) {
        self.nodes.remove(&node_id);
    }
}
