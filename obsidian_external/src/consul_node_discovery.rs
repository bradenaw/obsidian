use std::future::Future;
use std::net::IpAddr;
use std::str::FromStr;

use anyhow::anyhow;
use im::OrdSet;
use obsidian_common::NodeId;
use obsidian_util::OwnedWithBackground;
use obsidian_util::Retry;
use obsidian_util::Watchable;
use rs_consul::Consul;
use rs_consul::GetServiceNodesRequest;
use rs_consul::QueryOptions;
use rs_consul::RegisterEntityPayload;
use rs_consul::RegisterEntityService;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::NodeDiscovery;

pub struct ConsulNodeDiscovery(OwnedWithBackground<ConsulNodeDiscoveryInner>);

struct ConsulNodeDiscoveryInner {
    consul: Consul,
    service: String,
    node_id: NodeId,
    need_register: Notify,
    node_ids: Watchable<OrdSet<NodeId>>,
}

impl ConsulNodeDiscovery {
    pub fn new(node_id: NodeId, consul: Consul, service: String) -> Self {
        let inner = OwnedWithBackground::new(ConsulNodeDiscoveryInner {
            consul,
            service,
            node_id,
            need_register: Notify::new(),
            node_ids: Watchable::new(OrdSet::new()),
        });

        inner.spawn(async |inner| {
            inner.watch().await;
        });

        inner.spawn(async |inner| {
            inner.keep_registered().await;
        });

        Self(inner)
    }
}

impl ConsulNodeDiscoveryInner {
    async fn watch(&self) {
        // Index used for long-poll updates. It's returned in each response from get_service_nodes,
        // and on each request, if the data hasn't changed the request blocks until it does.
        let mut index = None;

        loop {
            let new_index = Retry::new()
                .indefinitely(&async || {
                    self.poll(index)
                        .await
                        .map_err(|e| anyhow!("during consul discovery poll: {}", e))
                })
                .await;
            index = Some(new_index);
        }
    }

    async fn poll(&self, index: Option<u64>) -> anyhow::Result<u64> {
        let mut query_opts = QueryOptions::default();
        query_opts.index = index;
        let resp = self
            .consul
            .get_service_nodes(
                GetServiceNodesRequest::builder()
                    .service(&self.service)
                    .build(),
                Some(query_opts),
            )
            .await?;
        let mut node_ids: OrdSet<NodeId> = resp
            .response
            .into_iter()
            .map(|service_node| {
                Ok(NodeId {
                    addr: IpAddr::from_str(&service_node.node.address).map_err(|e| {
                        anyhow!(
                            "couldn't parse node address {:?}: {}",
                            service_node.node.address,
                            e
                        )
                    })?,
                    port: service_node.service.port,
                    uuid: Uuid::try_from(service_node.node.node)?,
                })
            })
            .collect::<anyhow::Result<_>>()?;
        if !node_ids.contains(&self.node_id) {
            self.need_register.notify_one();
            node_ids.insert(self.node_id);
        }
        let prev_node_ids = self.node_ids.get().0;
        if node_ids != prev_node_ids {
            self.node_ids.set(node_ids);
        }
        Ok(resp.index)
    }

    async fn keep_registered(&self) {
        loop {
            let trigger = self.need_register.notified();
            Retry::new()
                .indefinitely(&async || {
                    self.register()
                        .await
                        .map_err(|e| anyhow!("failed to register with consul: {}", e))
                })
                .await;
            trigger.await;
            log::warn!(
                "consul registration for {:?} dropped, reregistering",
                self.node_id
            );
        }
    }

    async fn register(&self) -> anyhow::Result<()> {
        self.consul
            .register_entity(
                &RegisterEntityPayload::builder()
                    .Node(self.node_id.uuid.to_string())
                    .Address(self.node_id.addr.to_string())
                    .Service(
                        RegisterEntityService::builder()
                            .Service(self.service.clone())
                            .Port(self.node_id.port)
                            .build(),
                    )
                    .build(),
            )
            .await?;

        Ok(())
    }
}

impl NodeDiscovery for ConsulNodeDiscovery {
    fn node_ids(&self) -> (OrdSet<NodeId>, Box<dyn Future<Output = ()> + Send + Unpin>) {
        let (node_ids, changed) = self.0.node_ids.get();
        (node_ids, Box::new(Box::pin(changed)))
    }
}
