use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;

use anyhow::anyhow;
use obsidian_pb as pb;
use obsidian_util::longest_shared_prefix_len;

use crate::ColoGroupId;
use crate::Key;
use crate::KeyspaceId;

pub(crate) fn key_set_to_proto(keys: BTreeSet<Key>) -> pb::internal::CompressedKeySet {
    let mut keyspace_id_counts = HashMap::new();
    let mut key_to_keyspace_ids = BTreeMap::new();
    for (keyspace_id, key) in keys {
        *(keyspace_id_counts.entry(keyspace_id).or_insert(0)) += 1;
        key_to_keyspace_ids
            .entry(key)
            .or_insert_with(Vec::new)
            .push(keyspace_id);
    }
    let mut keyspace_ids_by_pop = keyspace_id_counts.keys().copied().collect::<Vec<_>>();
    keyspace_ids_by_pop.sort_by_key(|keyspace_id| keyspace_id_counts.get(keyspace_id));
    let keyspace_id_to_idx = keyspace_ids_by_pop
        .iter()
        .enumerate()
        .map(|(i, keyspace_id)| (*keyspace_id, i))
        .collect::<HashMap<_, _>>();

    let mut key_fragments = vec![];
    let mut key_shared_prefixes = vec![];
    let mut maybe_prev_key = None;
    for key in key_to_keyspace_ids.keys() {
        let n_shared = match maybe_prev_key {
            Some(prev_key) => longest_shared_prefix_len(key, prev_key),
            None => 0,
        };

        key_fragments.push(key[n_shared..].to_vec());
        key_shared_prefixes.push(n_shared as u32);

        maybe_prev_key = Some(key);
    }

    let mut key_keyspaces_counts = vec![];
    let mut key_keyspaces_refs = vec![];
    if keyspace_id_to_idx.len() > 1 {
        for keyspace_ids in key_to_keyspace_ids.values() {
            let mut count = 0;
            for keyspace_id in keyspace_ids {
                let idx = *(keyspace_id_to_idx.get(keyspace_id).unwrap());
                count += 1;
                key_keyspaces_refs.push(idx as u32);
            }
            key_keyspaces_counts.push(count);
        }
    }

    pb::internal::CompressedKeySet {
        keyspace_ids: keyspace_ids_by_pop
            .iter()
            .map(|keyspace_id| pb::KeyspaceId {
                colo_group_id: keyspace_id.0 .0,
                id: keyspace_id.1,
            })
            .collect(),
        key_fragments,
        key_shared_prefixes,
        key_keyspaces_counts,
        key_keyspaces_refs,
    }
}

pub(crate) fn key_set_from_proto(
    set_pb: pb::internal::CompressedKeySet,
) -> anyhow::Result<BTreeSet<Key>> {
    let keyspace_ids = set_pb
        .keyspace_ids
        .iter()
        .map(|keyspace_id_pb| {
            KeyspaceId(ColoGroupId(keyspace_id_pb.colo_group_id), keyspace_id_pb.id)
        })
        .collect::<Vec<_>>();

    if set_pb.key_fragments.len() != set_pb.key_shared_prefixes.len() {
        return Err(anyhow!(""));
    }

    let mut prev_key = vec![];
    let mut j = 0;
    let mut out = BTreeSet::new();
    for (i, key_fragment) in set_pb.key_fragments.iter().enumerate() {
        let n_shared = set_pb.key_shared_prefixes[i] as usize;
        let n_more = key_fragment.len();

        if n_shared > prev_key.len() {
            return Err(anyhow!(""));
        }

        let mut key = vec![0u8; n_shared + n_more];
        (key[..n_shared]).copy_from_slice(&prev_key[..n_shared]);
        (key[n_shared..]).copy_from_slice(&key_fragment);

        if keyspace_ids.len() == 1 {
            out.insert((keyspace_ids[0], key.clone()));
        } else {
            for _ in 0..set_pb.key_keyspaces_counts[i] {
                if j >= set_pb.key_keyspaces_refs.len() {
                    return Err(anyhow!(""));
                }

                let idx = set_pb.key_keyspaces_refs[j] as usize;
                if idx >= keyspace_ids.len() {
                    return Err(anyhow!(""));
                }

                let keyspace_id = keyspace_ids[idx];
                out.insert((keyspace_id, key.clone()));
                j += 1;
            }
        }

        prev_key = key;
    }

    Ok(out)
}
