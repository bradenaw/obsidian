use std::collections::HashMap;
use std::fmt::Debug;
use std::iter;

use anyhow::anyhow;

use crate::lsm::RunId;
use crate::pb;
use crate::types::uuid_from_proto;
use crate::types::uuid_to_proto;
use crate::util::merge_sorted2;
use crate::util::OrdEqByFirst;
use crate::KeyspaceId;
use crate::Range;

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct Manifest {
    pub(crate) keyspaces: HashMap<KeyspaceId, KeyspaceManifest>,
}

impl Manifest {
    pub fn empty() -> Self {
        Self {
            keyspaces: HashMap::new(),
        }
    }

    /// Merges two manifests together. Returns an error if any resulting level >0 would have
    /// intersecting runs in it.
    pub fn merge(self, other: Manifest) -> anyhow::Result<Manifest> {
        Ok(Manifest {
            keyspaces: merge_maps(self.keyspaces, other.keyspaces)
                .map(|(keyspace_id, maybe_a, maybe_b)| {
                    let a = maybe_a.unwrap_or_else(KeyspaceManifest::empty);
                    let b = maybe_b.unwrap_or_else(KeyspaceManifest::empty);
                    a.merge(b).map(|merged| (keyspace_id, merged))
                })
                .collect::<anyhow::Result<HashMap<_, _>>>()?,
        })
    }

    /// Removes all runs from the manifest that are not fully contained within the given range.
    pub fn clip(&mut self, range: Range<&[u8]>) {
        for (_, keyspace) in &mut self.keyspaces {
            keyspace.clip(range);
        }
    }

    /// Returns an iterator of (keyspace_id, level_idx, run_manifest) for all runs in the manifest.
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

impl TryFrom<pb::internal::Manifest> for Manifest {
    type Error = anyhow::Error;

    fn try_from(value: pb::internal::Manifest) -> Result<Self, Self::Error> {
        let keyspace_ids = value
            .keyspace_ids
            .into_iter()
            .map(KeyspaceId::try_from)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let keyspace_manifests = value
            .keyspaces
            .into_iter()
            .map(KeyspaceManifest::try_from)
            .collect::<anyhow::Result<Vec<_>>>()?;

        if keyspace_ids.len() != keyspace_manifests.len() {
            return Err(anyhow!(
                "must have same number of keyspace_ids as keyspaces: {} != {}",
                keyspace_ids.len(),
                keyspace_manifests.len()
            ));
        }

        let mut keyspaces_map = HashMap::new();
        for (keyspace_id, keyspace_manifest) in
            iter::zip(keyspace_ids.into_iter(), keyspace_manifests.into_iter())
        {
            if keyspaces_map.contains_key(&keyspace_id) {
                return Err(anyhow!("duplicate {:?}", keyspace_id));
            }

            keyspaces_map.insert(keyspace_id, keyspace_manifest);
        }

        Ok(Manifest {
            keyspaces: keyspaces_map,
        })
    }
}

impl From<Manifest> for pb::internal::Manifest {
    fn from(value: Manifest) -> Self {
        let mut keyspace_ids_pb = Vec::with_capacity(value.keyspaces.len());
        let mut keyspaces_pb = Vec::with_capacity(value.keyspaces.len());

        for (keyspace_id, keyspace) in value.keyspaces {
            keyspace_ids_pb.push(keyspace_id.into());
            keyspaces_pb.push(keyspace.into());
        }

        pb::internal::Manifest {
            keyspace_ids: keyspace_ids_pb,
            keyspaces: keyspaces_pb,
        }
    }
}

/// The manifest for a single keyspace. L0 is always empty because L0 is kept in-memory rather than
/// in Runs, so there are no RunIds for it.
#[derive(Clone, Eq, Debug, PartialEq)]
pub(crate) struct KeyspaceManifest {
    levels: Vec<LevelManifest>,
}

impl KeyspaceManifest {
    pub fn new(levels: Vec<LevelManifest>) -> anyhow::Result<KeyspaceManifest> {
        if levels.is_empty() {
            return Ok(KeyspaceManifest::empty());
        }

        if !levels[0].runs().is_empty() {
            return Err(anyhow!("manifest with non-empty L0"));
        }

        Ok(KeyspaceManifest { levels })
    }

    pub fn empty() -> KeyspaceManifest {
        KeyspaceManifest { levels: vec![] }
    }

    pub fn levels(&self) -> &[LevelManifest] {
        &self.levels
    }

    pub fn merge(self, other: KeyspaceManifest) -> anyhow::Result<KeyspaceManifest> {
        Ok(KeyspaceManifest {
            levels: zip_max(self.levels.into_iter(), other.levels.into_iter())
                .enumerate()
                .map(|(i, (maybe_a, maybe_b))| {
                    let a = maybe_a.unwrap_or_else(|| LevelManifest::empty());
                    let mut b = maybe_b.unwrap_or_else(|| LevelManifest::empty());
                    if i == 0 {
                        // L0 is not sorted and overlaps are allowed.
                        let mut runs = a.runs;
                        runs.append(&mut b.runs);
                        Ok(LevelManifest { runs: runs })
                    } else {
                        a.merge(b)
                    }
                })
                .collect::<anyhow::Result<Vec<_>>>()?,
        })
    }

    pub fn clip(&mut self, range: Range<&[u8]>) {
        for level in &mut self.levels {
            level.clip(range);
        }
    }
}

impl TryFrom<pb::internal::KeyspaceManifest> for KeyspaceManifest {
    type Error = anyhow::Error;

    fn try_from(value: pb::internal::KeyspaceManifest) -> Result<Self, Self::Error> {
        Ok(KeyspaceManifest {
            levels: value
                .levels
                .into_iter()
                .map(LevelManifest::try_from)
                .collect::<anyhow::Result<Vec<_>>>()?,
        })
    }
}

impl From<KeyspaceManifest> for pb::internal::KeyspaceManifest {
    fn from(value: KeyspaceManifest) -> Self {
        pb::internal::KeyspaceManifest {
            levels: value.levels.into_iter().map(|level| level.into()).collect(),
        }
    }
}

#[derive(Clone, Eq, Debug, PartialEq)]
pub(crate) struct LevelManifest {
    runs: Vec<RunManifest>,
}

impl LevelManifest {
    pub fn new(mut runs: Vec<RunManifest>) -> anyhow::Result<LevelManifest> {
        runs.sort_unstable_by(|a, b| a.range.lower.cmp(&b.range.lower));
        for i in 1..runs.len() {
            if runs[i].range.intersects(&runs[i - 1].range) {
                return Err(anyhow!(
                    "invalid manifest: {:?} {:?} overlaps {:?} {:?}",
                    runs[i].run_id,
                    runs[i].range,
                    runs[i - 1].run_id,
                    runs[i - 1].range
                ));
            }
        }

        Ok(LevelManifest { runs })
    }

    pub fn empty() -> LevelManifest {
        LevelManifest { runs: vec![] }
    }

    /// The runs that comprise this level. Always in sorted order by range, and ranges are
    /// guaranteed non-overlapping.
    pub fn runs(&self) -> &[RunManifest] {
        &self.runs
    }

    pub fn merge(self, other: LevelManifest) -> anyhow::Result<LevelManifest> {
        let mut runs: Vec<RunManifest> = Vec::with_capacity(self.runs.len() + other.runs.len());
        let merged_runs = merge_sorted2(
            self.runs
                .into_iter()
                .map(|run| OrdEqByFirst(run.range.lower, (run.range.upper, run.run_id))),
            other
                .runs
                .into_iter()
                .map(|run| OrdEqByFirst(run.range.lower, (run.range.upper, run.run_id))),
        )
        .map(|OrdEqByFirst(lower, (upper, run_id))| RunManifest {
            run_id: run_id,
            range: Range { lower, upper },
        });
        for run in merged_runs {
            if let Some(last) = runs.last() {
                if last.range.intersects(&run.range) {
                    return Err(anyhow!(
                        "can't merge manifests: {:?} {:?} intersects with {:?} {:?}",
                        run.run_id,
                        run.range,
                        last.run_id,
                        last.range,
                    ));
                }
            }
            runs.push(run);
        }

        Ok(LevelManifest { runs })
    }

    pub fn clip(&mut self, range: Range<&[u8]>) {
        self.runs.retain(|run| range.contains_range(&run.range));
    }
}

impl TryFrom<pb::internal::LevelManifest> for LevelManifest {
    type Error = anyhow::Error;

    fn try_from(value: pb::internal::LevelManifest) -> Result<Self, Self::Error> {
        Ok(LevelManifest {
            runs: value
                .runs
                .into_iter()
                .map(RunManifest::try_from)
                .collect::<anyhow::Result<Vec<_>>>()?,
        })
    }
}

impl From<LevelManifest> for pb::internal::LevelManifest {
    fn from(value: LevelManifest) -> Self {
        pb::internal::LevelManifest {
            runs: value.runs.into_iter().map(|run| run.into()).collect(),
        }
    }
}

#[derive(Clone, Eq, Debug, PartialEq)]
pub(crate) struct RunManifest {
    pub(crate) run_id: RunId,
    pub(crate) range: Range<Vec<u8>>,
}

impl TryFrom<pb::internal::RunManifest> for RunManifest {
    type Error = anyhow::Error;

    fn try_from(value: pb::internal::RunManifest) -> Result<Self, Self::Error> {
        Ok(RunManifest {
            run_id: RunId::from(value.run_id.ok_or_else(|| anyhow!("missing run_id"))?),
            range: Range::try_from(value.range.ok_or_else(|| anyhow!("missing range"))?)?,
        })
    }
}

impl From<RunManifest> for pb::internal::RunManifest {
    fn from(value: RunManifest) -> Self {
        pb::internal::RunManifest {
            run_id: Some(value.run_id.into()),
            range: Some(value.range.into()),
        }
    }
}

// Like Iterator::zip, but rather than terminating when the first iterator terminates, terminates
// when the last of them terminates.
fn zip_max<T, I0, I1>(a: I0, b: I1) -> impl Iterator<Item = (Option<T>, Option<T>)>
where
    I0: Iterator<Item = T>,
    I1: Iterator<Item = T>,
{
    Iterator::zip(
        Iterator::chain(a.map(|item| Some(item)), iter::repeat_with(|| None)),
        Iterator::chain(b.map(|item| Some(item)), iter::repeat_with(|| None)),
    )
    .take_while(|(maybe_a_item, maybe_b_item)| maybe_a_item.is_some() || maybe_b_item.is_some())
}

// Yields an item for each key that appears in either map along with the values that key had in
// each map.
fn merge_maps<K, V>(
    a: HashMap<K, V>,
    mut b: HashMap<K, V>,
) -> impl Iterator<Item = (K, Option<V>, Option<V>)>
where
    K: Eq + std::hash::Hash,
{
    iter::from_coroutine(
        #[coroutine]
        || {
            for (k, va) in a.into_iter() {
                let maybe_vb = b.remove(&k);
                yield (k, Some(va), maybe_vb);
            }
            for (k, kb) in b.into_iter() {
                yield (k, None, Some(kb));
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::collections::HashMap;

    use crate::lsm::KeyspaceManifest;
    use crate::lsm::LevelManifest;
    use crate::lsm::Manifest;
    use crate::lsm::RunId;
    use crate::lsm::RunManifest;
    use crate::Bound;
    use crate::ColoGroupId;
    use crate::KeyspaceId;
    use crate::Range;

    fn run(lower: &str, upper: &str) -> RunManifest {
        RunManifest {
            run_id: RunId::new(),
            range: Range {
                lower: Bound::Before(lower.to_string().into_bytes()),
                upper: Bound::Before(upper.to_string().into_bytes()),
            },
        }
    }

    #[test]
    fn test_merge_non_overlapping_keyspaces() -> anyhow::Result<()> {
        let keyspace_1_manifest = KeyspaceManifest {
            levels: vec![
                LevelManifest::empty(),
                LevelManifest {
                    runs: vec![run("a", "c")],
                },
            ],
        };
        let keyspace_2_manifest = KeyspaceManifest {
            levels: vec![
                LevelManifest::empty(),
                LevelManifest {
                    runs: vec![run("a", "c")],
                },
            ],
        };
        let a = Manifest {
            keyspaces: HashMap::from([(
                KeyspaceId(ColoGroupId(1), 1),
                keyspace_1_manifest.clone(),
            )]),
        };
        let b = Manifest {
            keyspaces: HashMap::from([(
                KeyspaceId(ColoGroupId(1), 2),
                keyspace_2_manifest.clone(),
            )]),
        };

        assert_eq!(
            a.merge(b)?,
            Manifest {
                keyspaces: HashMap::from([
                    (KeyspaceId(ColoGroupId(1), 1), keyspace_1_manifest),
                    (KeyspaceId(ColoGroupId(1), 2), keyspace_2_manifest),
                ]),
            }
        );

        Ok(())
    }

    #[test]
    fn test_merge_overlapping_keyspaces() -> anyhow::Result<()> {
        let keyspace_1_manifest = KeyspaceManifest {
            levels: vec![
                LevelManifest::empty(),
                LevelManifest {
                    runs: vec![run("a", "c")],
                },
            ],
        };
        let keyspace_2_manifest = KeyspaceManifest {
            levels: vec![
                LevelManifest::empty(),
                LevelManifest {
                    runs: vec![run("a", "c")],
                },
            ],
        };

        let keyspace_3_runs = vec![
            run("a", "c"),
            run("c", "d"),
            run("d", "f"),
            run("g", "m"),
            run("m", "z"),
        ];
        let a = Manifest {
            keyspaces: HashMap::from([
                (KeyspaceId(ColoGroupId(1), 1), keyspace_1_manifest.clone()),
                (
                    KeyspaceId(ColoGroupId(1), 3),
                    KeyspaceManifest {
                        levels: vec![
                            LevelManifest::empty(),
                            LevelManifest {
                                runs: vec![
                                    keyspace_3_runs[0].clone(),
                                    keyspace_3_runs[1].clone(),
                                    keyspace_3_runs[4].clone(),
                                ],
                            },
                        ],
                    },
                ),
            ]),
        };
        let b = Manifest {
            keyspaces: HashMap::from([
                (KeyspaceId(ColoGroupId(1), 2), keyspace_2_manifest.clone()),
                (
                    KeyspaceId(ColoGroupId(1), 3),
                    KeyspaceManifest {
                        levels: vec![
                            LevelManifest::empty(),
                            LevelManifest {
                                runs: vec![keyspace_3_runs[2].clone(), keyspace_3_runs[3].clone()],
                            },
                        ],
                    },
                ),
            ]),
        };

        assert_eq!(
            a.merge(b)?,
            Manifest {
                keyspaces: HashMap::from([
                    (KeyspaceId(ColoGroupId(1), 1), keyspace_1_manifest),
                    (KeyspaceId(ColoGroupId(1), 2), keyspace_2_manifest),
                    (
                        KeyspaceId(ColoGroupId(1), 3),
                        KeyspaceManifest {
                            levels: vec![
                                LevelManifest::empty(),
                                LevelManifest {
                                    runs: keyspace_3_runs,
                                },
                            ],
                        },
                    ),
                ]),
            }
        );

        Ok(())
    }

    #[test]
    fn test_merge_different_depths() -> anyhow::Result<()> {
        let runs = vec![
            run("a", "c"),
            run("c", "f"),
            run("a", "b"),
            run("b", "d"),
            run("e", "g"),
        ];
        let a = KeyspaceManifest {
            levels: vec![
                LevelManifest::empty(),
                LevelManifest {
                    runs: vec![runs[0].clone()],
                },
            ],
        };
        let b = KeyspaceManifest {
            levels: vec![
                LevelManifest::empty(),
                LevelManifest {
                    runs: vec![runs[1].clone()],
                },
                LevelManifest {
                    runs: vec![runs[2].clone(), runs[3].clone(), runs[4].clone()],
                },
            ],
        };

        assert_eq!(
            a.merge(b)?,
            KeyspaceManifest {
                levels: vec![
                    LevelManifest::empty(),
                    LevelManifest {
                        runs: vec![runs[0].clone(), runs[1].clone()],
                    },
                    LevelManifest {
                        runs: vec![runs[2].clone(), runs[3].clone(), runs[4].clone()],
                    },
                ],
            }
        );

        Ok(())
    }

    #[test]
    fn test_merge_overlapping() {
        let manifest0 = LevelManifest {
            runs: vec![run("a", "c"), run("e", "f")],
        };
        let manifest1 = LevelManifest {
            runs: vec![run("b", "d")],
        };

        assert_matches!(LevelManifest::merge(manifest0, manifest1), Err(_));
    }

    #[test]
    fn test_clip() {
        let keyspace_1_manifest = KeyspaceManifest {
            levels: vec![
                LevelManifest::empty(),
                LevelManifest {
                    runs: vec![
                        run("a", "c"), // no
                        run("c", "f"), // yes
                    ],
                },
                LevelManifest {
                    runs: vec![
                        run("a", "b"), // no
                        run("b", "d"), // yes
                        run("e", "g"), // yes
                    ],
                },
            ],
        };
        let keyspace_2_manifest = KeyspaceManifest {
            levels: vec![
                LevelManifest::empty(),
                LevelManifest {
                    runs: vec![
                        run("a", "c"), // no
                        run("c", "e"), // yes
                        run("f", "i"), // no
                    ],
                },
                LevelManifest {
                    runs: vec![
                        run("a", "d"), // no
                        run("e", "f"), // yes
                        run("g", "j"), // no
                    ],
                },
            ],
        };

        let manifest = Manifest {
            keyspaces: HashMap::from([
                (KeyspaceId(ColoGroupId(1), 1), keyspace_1_manifest.clone()),
                (KeyspaceId(ColoGroupId(1), 2), keyspace_2_manifest.clone()),
            ]),
        };

        let mut clipped = manifest.clone();
        clipped.clip(
            Range {
                lower: Bound::Before(b"b".to_vec()),
                upper: Bound::After(b"h".to_vec()),
            }
            .borrow(),
        );

        assert_eq!(
            clipped,
            Manifest {
                keyspaces: HashMap::from([
                    (
                        KeyspaceId(ColoGroupId(1), 1),
                        KeyspaceManifest {
                            levels: vec![
                                LevelManifest::empty(),
                                LevelManifest {
                                    runs: vec![keyspace_1_manifest.levels[1].runs[1].clone()],
                                },
                                LevelManifest {
                                    runs: vec![
                                        keyspace_1_manifest.levels[2].runs[1].clone(),
                                        keyspace_1_manifest.levels[2].runs[2].clone(),
                                    ],
                                },
                            ],
                        },
                    ),
                    (
                        KeyspaceId(ColoGroupId(1), 2),
                        KeyspaceManifest {
                            levels: vec![
                                LevelManifest::empty(),
                                LevelManifest {
                                    runs: vec![keyspace_2_manifest.levels[1].runs[1].clone()],
                                },
                                LevelManifest {
                                    runs: vec![keyspace_2_manifest.levels[2].runs[1].clone()],
                                },
                            ],
                        },
                    ),
                ]),
            }
        );
    }
}
