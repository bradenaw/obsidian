use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use im::OrdSet;
use obsidian_common::NodeId;
use obsidian_external::ConsulNodeDiscovery;
use obsidian_external::NodeDiscovery;
use rs_consul::Consul;
use tokio::process::Child;
use tokio::process::Command;
use uuid::Uuid;

use crate::discovery::Discovery;
use crate::grpc::GrpcNodes;
use crate::runtime::Node;
use crate::runtime::Nodes;
use crate::test::test_nodes::TestNodes;

const S3_PORT: u16 = 9000;
const S3_CONSOLE_PORT: u16 = 9001;
const CONSUL_PORT: u16 = 8500;
const JOURNALS_PORT: u16 = 8000;
const NODE_PORTS_START: u16 = 8001;

const MINIO_IMAGE: &str =
    "docker.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e";
const CONSUL_IMAGE: &str = "docker.io/hashicorp/consul@sha256:a230dcea0bb107bd7958a912d1429fb7f9d399637de7ffb814b34412b9e8c543";
const CONSUL_SERVICE: &str = "obsidian";
const S3_BUCKET_NAME: &str = "obsidian";

pub(crate) struct SubprocessNodes {
    discovery: Arc<Discovery>,
    inner_nodes: Arc<dyn Nodes>,

    cargo_bin: PathBuf,
    kill_on_exit: Child,
    storage: Child,
    consul: Child,
    journals: Child,
    next_port: u16,
    nodes: HashMap<NodeId, Child>,
}

impl SubprocessNodes {
    pub async fn new() -> anyhow::Result<Self> {
        let label = format!("obsidian-test-{}", Uuid::new_v4());

        // Slightly hacky: `podman run` runs the containers in the background. Even if the `podman
        // run` process dies, the containers still stick around. That's not the behavior we want,
        // when the test ends (or gets sigtermed etc.) we want it to clean up after itself.
        //
        // The hacky solution is to spawn this process that waits for its own stdin to close (which
        // happens when we die), and then kills the containers with the label we made.
        //
        // There's still a race here where if we die while spawning a container, this reaper
        // process might clean up too early. Oh well.
        let kill_on_exit = Command::new("bash")
            .arg("-c")
            .arg(format!("while read line; do true; done; podman ps --quiet --filter 'label={}' | xargs podman kill", label.clone()))
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        let storage = Command::new("podman")
            .arg("run")
            .arg("--rm")
            .arg("--name")
            .arg("obsidian_test_minio")
            .arg("--label")
            .arg(label.clone())
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

        // JANK: need to wait for container to launch we try to make the bucket
        tokio::time::sleep(Duration::from_millis(500)).await;

        Command::new("podman")
            .arg("exec")
            .arg("obsidian_test_minio")
            .arg("mkdir")
            .arg("-p")
            .arg(format!("/data/{}", S3_BUCKET_NAME))
            .spawn()?
            .wait()
            .await?;

        let consul = Command::new("podman")
            .arg("run")
            .arg("--rm")
            .arg("--label")
            .arg(label)
            .arg("-p")
            .arg(format!("{}:{}", CONSUL_PORT, CONSUL_PORT))
            .arg(CONSUL_IMAGE.to_string())
            .arg("agent")
            .arg("--dev") // allow single-node cluster
            .arg("--client")
            .arg("[::]") // defaults to localhost
            .arg("--http-port")
            .arg(CONSUL_PORT.to_string())
            .kill_on_drop(true)
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
            CONSUL_SERVICE.to_string(),
        );

        let inner_nodes = Arc::new(GrpcNodes::new(Arc::new(node_discovery)));
        let discovery = Arc::new(Discovery::new(Arc::clone(&inner_nodes) as Arc<dyn Nodes>));

        Ok(Self {
            cargo_bin,

            discovery,
            inner_nodes,
            kill_on_exit,
            storage,
            consul,
            journals,
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

        let node = Command::new(self.cargo_bin.join("obsidian"))
            .env("RUST_BACKTRACE", "1")
            .env("RUST_LOG", "info")
            .env("AWS_REGION", "us-west-2")
            .env("AWS_ACCESS_KEY_ID", "minioadmin")
            .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
            .arg("node")
            .arg("--node-id")
            .arg(node_id.to_string())
            .arg("--journals-addr")
            .arg(format!("http://[::1]:{}", JOURNALS_PORT))
            .arg("--s3-addr")
            .arg(format!("http://[::1]:{}", S3_PORT))
            .arg("--s3-bucket")
            .arg(S3_BUCKET_NAME.to_string())
            .arg("--consul-addr")
            .arg(format!("http://[::1]:{}", CONSUL_PORT))
            .arg("--consul-service")
            .arg(CONSUL_SERVICE.to_string())
            .kill_on_drop(true)
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
