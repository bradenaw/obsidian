use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context as _;
use async_trait::async_trait;
use im::OrdSet;
use obsidian_common::NodeId;
use obsidian_external::ConsulNodeDiscovery;
use obsidian_external::NodeDiscovery;
use obsidian_util::Retry;
use rs_consul::Consul;
use tokio::net::TcpStream;

use crate::discovery::Discovery;
use crate::grpc::GrpcNodes;
use crate::runtime::Node;
use crate::runtime::Nodes;
use crate::test::containers::Container;
use crate::test::containers::Containers;
use crate::test::test_nodes::TestNodes;

const JOURNALS_CONTAINER_NAME: &str = "journals";
const JOURNALS_PORT: u16 = 8000;

const S3MOCK_IMAGE: &str =
    "docker.io/adobe/s3mock@sha256:ab01a6946750f451ca215a47e91030695b260e4003b8a5a6201d25029b8fca92";
const S3MOCK_CONTAINER_NAME: &str = "s3mock";
const S3_PORT: u16 = 9090;
const S3_BUCKET_NAME: &str = "obsidian";

const CONSUL_IMAGE: &str = "docker.io/hashicorp/consul@sha256:a230dcea0bb107bd7958a912d1429fb7f9d399637de7ffb814b34412b9e8c543";
const CONSUL_CONTAINER_NAME: &str = "consul";
const CONSUL_PORT: u16 = 8500;
const CONSUL_SERVICE: &str = "obsidian";

const NODE_PORTS_START: u16 = 8001;

pub(crate) struct SubprocessNodes {
    discovery: Arc<Discovery>,
    inner_nodes: Arc<dyn Nodes>,

    cargo_bin: PathBuf,
    containers: Containers,
    next_port: u16,
    nodes: HashMap<NodeId, Container>,
}

impl SubprocessNodes {
    pub async fn new() -> anyhow::Result<Self> {
        let mut containers = Containers::new().await?;

        let _s3_container = containers
            .new_container(S3MOCK_CONTAINER_NAME)
            .image(S3MOCK_IMAGE)
            .port(S3_PORT)
            .env(
                "COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS",
                S3_BUCKET_NAME,
            )
            .spawn()?;

        let _consul_container = containers
            .new_container(CONSUL_CONTAINER_NAME)
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
            .new_container(JOURNALS_CONTAINER_NAME)
            .port(JOURNALS_PORT)
            .mount(cargo_bin.to_string_lossy(), "/cargo-bin", true /*ro*/)
            .env("RUST_LOG", "info")
            .env("RUST_BACKTRACE", "1")
            .arg("/cargo-bin/obsidian")
            .arg("test-journals")
            .arg("--port")
            .arg(JOURNALS_PORT.to_string())
            .spawn()?;

        wait_http(&format!("http://localhost:{}/actuator/health", S3_PORT)).await;
        wait_port(CONSUL_PORT).await;
        wait_port(JOURNALS_PORT).await;

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
        let node_id = NodeId::new("localhost".to_string(), port);

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
            .arg(format!(
                "http://{}:{}",
                JOURNALS_CONTAINER_NAME, JOURNALS_PORT,
            ))
            .arg("--s3-addr")
            .arg(format!("http://{}:{}", S3MOCK_CONTAINER_NAME, S3_PORT))
            .arg("--s3-bucket")
            .arg(S3_BUCKET_NAME.to_string())
            .arg("--s3-disable-credentials")
            .arg("--consul-addr")
            .arg(format!("http://{}:{}", CONSUL_CONTAINER_NAME, CONSUL_PORT))
            .arg("--consul-service")
            .arg(CONSUL_SERVICE.to_string())
            .spawn()?;

        self.nodes.insert(node_id.clone(), node);

        Ok(node_id)
    }

    fn remove_node(&mut self, node_id: &NodeId) {
        self.nodes.remove(node_id);
    }
}

impl Nodes for SubprocessNodes {
    fn node(&self, node_id: &NodeId) -> anyhow::Result<Arc<dyn Node>> {
        self.inner_nodes.node(node_id)
    }
}

impl NodeDiscovery for SubprocessNodes {
    fn node_ids(&self) -> (OrdSet<NodeId>, Box<dyn Future<Output = ()> + Send + Unpin>) {
        self.inner_nodes.node_ids()
    }
}

async fn wait_port(port: u16) {
    Retry::new()
        .indefinitely(&async || -> anyhow::Result<()> {
            TcpStream::connect(format!("localhost:{}", port))
                .await
                .context(format!("waiting for port {} to be bound", port))?;
            Ok(())
        })
        .await;
}

async fn wait_http(addr: &str) {
    Retry::new()
        .indefinitely(&async || -> anyhow::Result<()> {
            reqwest::get(addr).await?;
            Ok(())
        })
        .await;
}

#[cfg(test)]
mod tests {
    use obsidian_common::RunId;
    use obsidian_external::FileName;
    use obsidian_external::S3Storage;
    use obsidian_external::Storage;

    use super::*;

    #[tokio::test]
    async fn s3mock() -> anyhow::Result<()> {
        let _ = pretty_env_logger::try_init();

        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .unwrap();

        let _nodes = SubprocessNodes::new().await?;
        let s3_client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::config::Builder::from(
                &aws_config::from_env()
                    .endpoint_url(format!("http://localhost:{}", S3_PORT))
                    .region("us-east-1")
                    .credentials_provider(aws_credential_types::Credentials::new(
                        "dummy-access-key",
                        "dummy-secret-key",
                        None,
                        None,
                        "static",
                    ))
                    .load()
                    .await,
            )
            .force_path_style(true)
            .build(),
        );
        let storage = S3Storage::new(s3_client, S3_BUCKET_NAME.to_string());

        let expected = vec![0xde, 0xad, 0xbe, 0xef];
        let file_name = FileName::Run(RunId::new());
        let mut writer = storage.put(file_name.clone()).await?;
        writer.write_all(&expected[..]).await?;
        writer.shutdown().await?;

        let reader = storage.get(file_name).await?;
        assert_eq!(reader.len(), expected.len() as u64);
        let mut buf = vec![0, 0, 0, 0];
        reader.read_exact_at(&mut buf[..], 0).await?;
        assert_eq!(buf, expected);

        Ok(())
    }
}
