use std::ops::Deref;
use std::sync::Arc;

use crate::runtime::Node;
use crate::runtime::Tablet;
use crate::test::tablet_test_suite;
use crate::test::GrpcBridge;

tablet_test_suite!({
    use std::ops::Deref;
    use std::sync::Arc;

    use crate::runtime::Node;
    use crate::runtime::Shards as _;
    use crate::runtime::Tablet;
    use crate::test::node_grpc_bridge;
    use crate::test::GrpcBridge;
    use crate::test::ObsidianForTestBuilder;
    use crate::Bound;
    use crate::ColoGroupId;
    use crate::KeyspaceId;
    use crate::Obsidian;

    struct TabletAndBridge(Arc<dyn Tablet>, GrpcBridge<Arc<dyn Node>>);
    impl Deref for TabletAndBridge {
        type Target = Arc<dyn Tablet>;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    async || {
        let obs = ObsidianForTestBuilder::new().n_shards(1).build().await?;

        obs.gateway
            .create_colo_group(
                ColoGroupId(1),
                vec![Bound::Before(vec![0x00]), Bound::AfterPrefix(vec![0x00])],
            )
            .await?;
        obs.gateway
            .create_keyspace(KeyspaceId(ColoGroupId(1), 1))
            .await?;

        let tablet_id = obs.meta_synced.tablet_id_for_key(ColoGroupId(1), &[0x00])?;

        let node = obs.nodes.discovery().current_leader(tablet_id.0)?;

        let node_client = node_grpc_bridge(node).await?;

        // We need this TabletAndBridge wrapper here because without it, node_client would drop,
        // which would close the server.
        Ok::<_, anyhow::Error>(TabletAndBridge(node_client.tablet(tablet_id)?, node_client))
    }
});
