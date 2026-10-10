use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use im::OrdSet;
use obsidian_common::NodeId;
use obsidian_external::ConsulNodeDiscovery;
use obsidian_external::NodeDiscovery;
use rs_consul::Consul;

use crate::discovery::Discovery;
use crate::grpc::GrpcNodes;
use crate::runtime::Node;
use crate::runtime::Nodes;
use crate::test::containers::Container;
use crate::test::containers::Containers;
use crate::test::test_nodes::TestNodes;

const S3_PORT: u16 = 9090;
const CONSUL_PORT: u16 = 8500;
const JOURNALS_PORT: u16 = 8000;
const NODE_PORTS_START: u16 = 8001;

const S3MOCK_IMAGE: &str =
    "docker.io/adobe/s3mock@sha256:ab01a6946750f451ca215a47e91030695b260e4003b8a5a6201d25029b8fca92";
const CONSUL_IMAGE: &str = "docker.io/hashicorp/consul@sha256:a230dcea0bb107bd7958a912d1429fb7f9d399637de7ffb814b34412b9e8c543";
const CONSUL_SERVICE: &str = "obsidian";
const S3_BUCKET_NAME: &str = "obsidian";

pub(crate) struct SubprocessNodes {
    discovery: Arc<Discovery>,
    inner_nodes: Arc<dyn Nodes>,

    cargo_bin: PathBuf,
    containers: Containers,
    next_port: u16,
    nodes: HashMap<NodeId, Container>,
}

impl SubprocessNodes {
    pub async fn new(label: impl Into<String>) -> anyhow::Result<Self> {
        let mut containers = Containers::new(label.into()).await?;

        let _s3_container = containers
            .new_container("s3mock")
            .image(S3MOCK_IMAGE)
            .port(S3_PORT)
            .env(
                "COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS",
                S3_BUCKET_NAME,
            )
            .spawn()?;

        let _consul_container = containers
            .new_container("consul")
            .image(CONSUL_IMAGE)
            .port(CONSUL_PORT)
            .arg("agent")
            .arg("--dev") // allow single-node cluster
            .arg("--client")
            .arg("[::]") // defaults to localhost
            .arg("--http-port")
            .arg(CONSUL_PORT.to_string())
            .spawn()?;

        // The obsidian binary gets built and dumped into the same directory as the binary that is
        // this test.
        //
        // TODO: Does this need to be in tests/ instead of src to guarantee it's present?
        let cargo_bin = env::current_exe().map(|mut path| {
            path.pop();
            if path.ends_with("deps") {
                path.pop();
            }
            path
        })?;

        let _journals_container = containers
            .new_container("journals")
            .port(JOURNALS_PORT)
            .mount(cargo_bin.to_string_lossy(), "/cargo-bin", true /*ro*/)
            .env("RUST_LOG", "info")
            .env("RUST_BACKTRACE", "1")
            .arg("/cargo-bin/obsidian")
            .arg("test-journals")
            .arg("--port")
            .arg(JOURNALS_PORT.to_string())
            .spawn()?;

        let node_discovery = ConsulNodeDiscovery::observe(
            Consul::new({
                let mut config = rs_consul::Config::default();
                config.address = format!("http://localhost:{}", CONSUL_PORT);
                config
            }),
            CONSUL_SERVICE.to_string(),
        );

        let inner_nodes = Arc::new(GrpcNodes::new(Arc::new(node_discovery)));
        let discovery = Arc::new(Discovery::new(Arc::clone(&inner_nodes) as Arc<dyn Nodes>));

        Ok(Self {
            cargo_bin,

            discovery,
            inner_nodes,
            containers,
            next_port: NODE_PORTS_START,
            nodes: HashMap::new(),
        })
    }
}

#[async_trait]
impl TestNodes for SubprocessNodes {
    fn discovery(&self) -> Arc<Discovery> {
        Arc::clone(&self.discovery)
    }

    async fn create_node(&mut self) -> anyhow::Result<NodeId> {
        let port = self.next_port;
        self.next_port += 1;
        let addr = IpAddr::V6(Ipv6Addr::LOCALHOST);
        let node_id = NodeId::new(addr, port);

        let node = self
            .containers
            .new_container(format!("node{}", port))
            .port(port)
            .mount(
                self.cargo_bin.to_string_lossy(),
                "/cargo-bin",
                true, /*ro*/
            )
            // Necessary to trick rustls, needed for S3 client.
            .mount("/etc/ssl/certs", "/etc/ssl/certs", true /*ro*/)
            .env("RUST_BACKTRACE", "1")
            .env("RUST_LOG", "info")
            .env("AWS_REGION", "us-east-1")
            .arg("/cargo-bin/obsidian")
            .arg("node")
            .arg("--node-id")
            .arg(node_id.to_string())
            .arg("--journals-addr")
            .arg(format!("http://localhost:{}", JOURNALS_PORT))
            .arg("--s3-addr")
            .arg(format!("http://localhost:{}", S3_PORT))
            .arg("--s3-bucket")
            .arg(S3_BUCKET_NAME.to_string())
            .arg("--consul-addr")
            .arg(format!("http://localhost:{}", CONSUL_PORT))
            .arg("--consul-service")
            .arg(CONSUL_SERVICE.to_string())
            .spawn()?;

        self.nodes.insert(node_id, node);

        Ok(node_id)
    }

    fn remove_node(&mut self, node_id: NodeId) {
        self.nodes.remove(&node_id);
    }
}

impl Nodes for SubprocessNodes {
    fn node(&self, node_id: NodeId) -> anyhow::Result<Arc<dyn Node>> {
        self.inner_nodes.node(node_id)
    }
}

impl NodeDiscovery for SubprocessNodes {
    fn node_ids(&self) -> (OrdSet<NodeId>, Box<dyn Future<Output = ()> + Send + Unpin>) {
        self.inner_nodes.node_ids()
    }
}
