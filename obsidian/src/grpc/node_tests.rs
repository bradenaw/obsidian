#[cfg(test)]
mod tests {
    use crate::test::tablet_test_suite;

    tablet_test_suite!({
        use crate::runtime::Shards as _;
        use crate::test::ObsidianForTest;
        use crate::Bound;
        use crate::ColoGroupId;
        use crate::KeyspaceId;
        use crate::Obsidian;

        async || {
            let obs = ObsidianForTest::new(1 /*n_shards*/).await?;

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

            let tablet = obs.nodes.discovery().tablet(tablet_id)?;

            // We have node_grpc_bridge but we only have a tablet here. We need to get a node ID
            // out of discovery.
            todo!();
        }
    });
}
