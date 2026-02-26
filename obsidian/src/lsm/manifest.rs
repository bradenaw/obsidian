use std::collections::HashMap;
use std::fmt::Debug;

use crate::lsm::RunId;
use crate::KeyspaceId;
use crate::Range;

#[derive(Clone)]
pub(crate) struct Manifest {
    pub(crate) keyspaces: HashMap<KeyspaceId, KeyspaceManifest>,
}

impl Manifest {
    pub fn new() -> Self {
        Self {
            keyspaces: HashMap::new(),
        }
    }
}

impl Manifest {
    pub fn runs(&self) -> impl Iterator<Item = (KeyspaceId, usize, &RunManifest)> {
        self.keyspaces
            .iter()
            .map(|(keyspace_id, keyspace)| {
                keyspace
                    .levels
                    .iter()
                    .enumerate()
                    .map(move |(i, level)| level.runs.iter().map(move |run| (*keyspace_id, i, run)))
                    .flatten()
            })
            .flatten()
    }
}

impl Debug for Manifest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut keyspace_ids: Vec<_> = self.keyspaces.keys().collect();
        keyspace_ids.sort_unstable();

        write!(f, "manifest\n")?;
        for keyspace_id in keyspace_ids {
            let keyspace = &self.keyspaces[keyspace_id];
            write!(f, "  {:?}\n", keyspace_id)?;
            for (i, level) in keyspace.levels.iter().enumerate() {
                write!(f, "    l{}\n", i)?;
                for run_manifest in &level.runs {
                    write!(
                        f,
                        "      {:?} {:?}\n",
                        run_manifest.run_id, run_manifest.range
                    )?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct KeyspaceManifest {
    pub(crate) levels: Vec<LevelManifest>,
}

#[derive(Clone, Debug)]
pub(crate) struct LevelManifest {
    pub(crate) runs: Vec<RunManifest>,
}

#[derive(Clone, Debug)]
pub(crate) struct RunManifest {
    pub(crate) run_id: RunId,
    pub(crate) range: Range<Vec<u8>>,
}
