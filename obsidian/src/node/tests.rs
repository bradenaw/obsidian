use futures::StreamExt;
use obsidian_common::ShardId;

use crate::runtime::ReplicaState;
use crate::test::ObsidianForTestBuilder;

#[tokio::test]
async fn test_meta_promotion() -> anyhow::Result<()> {
    let _ = pretty_env_logger::try_init_timed();

    let mut obs = ObsidianForTestBuilder::new().build().await?;

    let node_ids = obs.nodes.node_ids().0;

    assert_eq!(node_ids.len(), 1);

    let initial_meta_leader = node_ids.iter().next().unwrap();

    let new_node_id = obs.nodes.create_node().await?;
    obs.nodes.remove_node(*initial_meta_leader);

    let new_node = obs.nodes.node(new_node_id)?;
    let mut stream = new_node.shards_subscribe();
    loop {
        let shards = stream.next().await.unwrap().unwrap();
        if let Some(ReplicaState::Leader(_)) = shards.get(&ShardId::META) {
            break;
        }
    }

    Ok(())
}
