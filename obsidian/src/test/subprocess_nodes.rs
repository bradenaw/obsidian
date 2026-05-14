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

struct SubprocessNodes {
    s3_port: u16,
    consul_port: u16,
    journals_port: u16,
    consul_service: String,

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
        const S3_PORT: u16 = 9000;
        const S3_CONSOLE_PORT: u16 = 9001;
        const CONSUL_PORT: u16 = 8500;
        const JOURNALS_PORT: u16 = 8000;

        let consul_service = "obsidian";

        let storage = Command::new("podman")
            .arg("run")
            .arg("-p")
            .arg(format!("{}:{}", S3_PORT, S3_PORT))
            .arg("-p")
            .arg(format!("{}:{}", S3_CONSOLE_PORT, S3_CONSOLE_PORT))
            .arg("docker.io/minio/minio")
            .arg("server")
            .arg("/data")
            .arg("--address")
            .arg(format!(":{}", S3_PORT))
            .arg("--console-address")
            .arg(format!(":{}", S3_CONSOLE_PORT))
            .kill_on_drop(true)
            .spawn()?;

        // TODO: actually tell consul about CONSUL_PORT
        let consul = Command::new("podman")
            .arg("run")
            .arg("-p")
            .arg(format!("{}:{}", CONSUL_PORT, CONSUL_PORT))
            .arg("docker.io/hashicorp/consul")
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
            s3_port: S3_PORT,
            consul_port: CONSUL_PORT,
            journals_port: JOURNALS_PORT,
            consul_service: consul_service.to_string(),
            cargo_bin,

            discovery,
            storage,
            consul,
            journals,
            next_port: 8001,
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
            .arg(format!("http://[::1]:{}", self.journals_port))
            .arg("--s3-addr")
            .arg(format!("http://[::1]:{}", self.s3_port))
            .arg("--s3-bucket")
            .arg("obsidian")
            .arg("--consul-addr")
            .arg(format!("http://[::1]:{}", self.consul_port))
            .arg("--consul-service")
            .arg(self.consul_service.clone())
            .kill_on_drop(true)
            .spawn()?;

        self.nodes.insert(node_id, node);

        Ok(node_id)
    }

    pub fn remove_node(&mut self, node_id: NodeId) {
        self.nodes.remove(&node_id);
    }
}
