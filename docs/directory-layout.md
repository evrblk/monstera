# Directory Layout

A node's `data_dir` is organized in the following way:

```
<data_dir>/
├── config/
│   ├── cluster.json       # applied cluster config
│   └── node.json          # node identity
├── raft/                  # shared raft log + stable store (Badger)
├── snapshots/
│   └── <replicaId>/       # per-replica raft snapshot store
└── cores/                 # application core store(s)
```

`config/node.json` holds the node ID. A brand new node starts with an empty `config` directory which means the node
has not been provisioned yet. The first call of `Bootstrap` assigns a node ID and a cluster config. After that the
node becomes ready. The current cluster config lives in `config/cluster.json`. It is rewritten atomically on every
`UpdateClusterConfig`. This makes the data directory self-describing and the applied config durable across restarts.

`raft/` holds a single BadgerDB instance with all Raft log and stable stores for all replicas on the node. Each
replica has a unique prefix which allows all of them to share a single KV store without collisions.

`snapshots/<replicaId>/` stores a few latest (3 by default, configurable) Raft snapshots They are available via
`ListReplicaSnapshots` API.

`cores/` stores all data for application cores. This is only a convention and not enforced by the framework.
