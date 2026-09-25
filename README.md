# Obsidian

Obsidian is a distributed, transactional key-value store. It provides snapshot-consistent reads and
single-shot atomic preconditioned writes.

## Architecture

The architecture bears some similarity to Bigtable[1], in that the base is an LSM hosted in shared
blob storage (like S3, GCP Cloud Storage, Azure Blob Storage); partitioned into ranges and assigned
to tablets with leader election.

It uses Hybrid Logical Clocks[2] to assign versions to transactions, and the read APIs allow
observing the state of the whole store as of a particular version. Cross-shard transactions are
handled with two-phase commit[3].


```
                                      shard:1  keys [a, m)
   ┌────────┐       ┌─────────┐       ┌──────────┐                       ┌──────────────┐
   │ client ├──────▶│ gateway ├───┬──▶│ leader   ├──────────────────────▶│ blob storage │
   └────────┘       └─────────┘   │   └─────────┬┘  ┌─────────────────┐  │              │
                                  │             ├──▶│ shard:1 journal │  │     lsm      │
                                  │   ┌─────────┴┐  └─────────────────┘  │  ┌─┬─┬─┬─┐   │
                                  ├──▶│ follower ├──────────────────────▶│  │ │ ├─┼─┤   │
                                  │   └──────────┘                       │  │ ├─┤ ├─┤   │
                                  │                                      │  ├─┤ ├─┼─┤   │
                                  │   shard:2  keys [m, z)               │  │ ├─┼─┼─┤   │
                                  │   ┌──────────┐                       │  │ ├─┼─┼─┤   │
                                  ├──▶│ leader   ├──────────────────────▶│  ├─┼─┼─┼─┤   │
                                  │   └─────────┬┘  ┌─────────────────┐  │  │ │ ├─┼─┤   │
                                  │             ├──▶│ shard:2 journal │  │  │ ├─┤ ├─┤   │
                                  │   ┌─────────┴┐  └─────────────────┘  │  ├─┤ ├─┼─┤   │
                                  └──▶│ follower ├──────────────────────▶│  │ ├─┼─┼─┤   │
                                      └──────────┘                       │  │ │ ├─┼─┤   │
                                                                         │  └─┴─┴─┴─┘   │
         ┌────────────┐               ┌────────────────┐                 │              │
         │ supervisor ├──────────────▶│ metadata shard ├────────────────▶│              │
         └────────────┘               └────────────────┘                 └──────────────┘
```

The control plane has a single node called the Supervisor responsible for making scheduling
decisions (like when to split, merge, or move ranges between shards, or assigning nodes to join
shards). Control plane metadata, like the routing table, is stored in a special tablet, dissemenated
asynchronously, and cached on every node. Data path requests never hit the control plane, they at
worst can work from slightly stale metadata which can only affect availability and never
correctness.

There is no central participant in every transaction, the shards assign hybrid logical clock
versions to transactions indepentently. This provides greater reliability but means that Obsidian
may exhibit causal reverse: a later transaction can be assigned a lower timestamp than an earlier
one, meaning that it is possible to view a snapshot that contains the results of the later
transaction without the results of the earlier one.

[1][https://storage.googleapis.com/gweb-research2023-media/pubtools/4443.pdf]
[2][https://cse.buffalo.edu/tech-reports/2014-04.pdf]
[3][https://en.wikipedia.org/wiki/Commit_(data_management)#Two-Phase_Commit_(2PC)]
