use std::collections::BTreeMap;
use std::collections::BTreeSet;

use futures::pin_mut;
use futures::StreamExt;
use futures::TryStreamExt;

use crate::obsidian::Obsidian;
use crate::obsidian::ObsidianExt;
use crate::range::Range;
use crate::types::Direction;
use crate::types::KeyspaceId;
use crate::types::Mutation;
use crate::types::Precondition;

struct WorkloadAppend<O> {
    obsidian: O,
}

impl<O: Obsidian + Sync> WorkloadAppend<O> {
    async fn write(&self) -> anyhow::Result<()> {
        let list_id = self.choose_list();
        let (list_keyspace_id, list_key) = list_id.to_key();

        let list_item = self.new_list_item(list_id);
        let (list_item_keyspace_id, list_item_key) = list_item.to_key();

        let read_ts = self
            .obsidian
            .latest_snapshot(BTreeSet::from([(list_keyspace_id, list_key.clone())]))
            .await?;

        self.obsidian
            .write(
                vec![Precondition::NotChangedSince(
                    list_keyspace_id,
                    list_key,
                    read_ts,
                )],
                BTreeMap::from([(
                    (list_item_keyspace_id, list_item_key),
                    Mutation::Put(vec![]),
                )]),
            )
            .await?;

        Ok(())
    }

    async fn read(&self) -> anyhow::Result<()> {
        let list_id = self.choose_list();
        let (list_keyspace_id, list_key) = list_id.to_key();

        let read_ts = self
            .obsidian
            .latest_snapshot(BTreeSet::from([(list_keyspace_id, list_key.clone())]))
            .await?;

        let s = self.obsidian.scan(
            read_ts,
            list_keyspace_id,
            Range::prefix(list_key),
            Direction::Asc,
        );

        pin_mut!(s);

        while let Some(record) = s.try_next().await? {
            println!("{:?}", record);
        }

        Ok(())
    }

    fn choose_list(&self) -> ListId {
        todo!()
    }

    fn new_list_item(&self, _list_id: ListId) -> ListItem {
        todo!();
    }
}

struct ListId(KeyspaceId, u64);
impl ListId {
    fn to_key(&self) -> (KeyspaceId, Vec<u8>) {
        todo!();
    }
}

struct ListItem(ListId, u64);
impl ListItem {
    fn to_key(&self) -> (KeyspaceId, Vec<u8>) {
        todo!();
    }
}
