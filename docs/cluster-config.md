# Cluster Configuration

Monstera cluster consists of multiple **applications**. The framework does not know anything about applications except
of their names.

Each application core is defined on a keyspace `[0x00000000; 0xffffffff]` (4 bytes key). This keyspace is 
divided into **shards**. Division can be arbitrary, but typically follows powers of 2. For example, originally there
were 16 shards, then one of them grew and was split into halves. Now there are 17 shards: 15 of the size 1/16th of the
keyspace (`[0x00000000; 0x0fffffff]`, `[0x10000000; 0x1fffffff]`, and so on) and 2 of the size 1/32nd of the keyspace.
There should be no gaps between shards nor overlaps. Shards are referred by ids which are unique across the whole 
cluster. Theoretically, up to 2^32 shards per application is possible.

Each application has a configurable replication factor, which defines the minimum number of **replicas** for each shard.
Replicas are also referred by unique ids. Replicas are assigned to **nodes**. Each node is referred by unique id and
has Monstera server address including port (that allows to run several nodes on a single machine for development 
purposes). Two replicas of the same shard cannot be assigned to the same node. Replicas are assigned to nodes at creation 
and cannot be moved.

The whole cluster is defined by `ClusterConfig` which consists of all applications, shards, replicas, and nodes.

Here is a snippet of a `cluster_config.json` file:

```json
{
  "applications": [
    {
      "name": "GrackleLocks",
      "implementation": "GrackleLocks",
      "shards": [
        {
          "id": "GrackleLocks_00_07",
          "lower_bound": "00",
          "upper_bound": "07",
          "state": "active",
          "replicas": [
            {
              "id": "GrackleLocks_00_07_7a4d737e",
              "node_id": "node-03"
            },
            {
              "id": "GrackleLocks_00_07_df01cea",
              "node_id": "node-01"
            },
            {
              "id": "GrackleLocks_00_07_17c33070",
              "node_id": "node-02"
            }
          ]
        },
        {
          "id": "GrackleLocks_08_0f",
          "lower_bound": "08",
          "upper_bound": "0f",
          "state": "active",
          "replicas": [
            {
              "id": "GrackleLocks_08_0f_5d3a107c",
              "node_id": "node-03"
            },
            {
              "id": "GrackleLocks_08_0f_255a2f63",
              "node_id": "node-05"
            },
            {
              "id": "GrackleLocks_08_0f_2ace936e",
              "node_id": "node-04"
            }
          ]
        },
        //...
      ],
      "replication_factor": 3
    }
  ],
  "nodes": [
    {
      "id": "node-01",
      "grpc_address": "ip-10-0-10-14.us-west-2.compute.internal:7000"
    },
    {
      "id": "node-02",
      "grpc_address": "ip-10-0-10-63.us-west-2.compute.internal:7000"
    },
    {
      "id": "node-03",
      "grpc_address": "ip-10-0-10-78.us-west-2.compute.internal:7000"
    },
    {
      "id": "node-04",
      "grpc_address": "ip-10-0-10-92.us-west-2.compute.internal:7000"
    },
    {
      "id": "node-05",
      "grpc_address": "ip-10-0-10-31.us-west-2.compute.internal:7000"
    }
  ],
  "version": 6
}
```

## Config Rollout

A cluster config is distributed as a single file to all nodes and clients. Processes or entire hosts can restart/reboot
and still have access to the cluster config after a restart. A new config is **pushed** to all nodes when it is changed.
There is no single authoritative place that stores the current config and allows to pull from. This eliminates a single 
point of failure. Monstera hosts are already stateful, because they store Raft logs and application cores data. Why not
to store a cluster config there as well?

Cluster configuration does not change often, it can take weeks until there is a need to split a shard or move shards to
new nodes. Raft leadership information, which is the only thing that can change by itself and often, is not stored in the
cluster config. The diff is always small, incremental, and made to be safe to gradually rollout. **It is safe to have an 
old version of the config on some nodes in the cluster and a new version on others.**

In order to make it safe, a rollout must follow two simple rules:

* There must be at most 2 versions of the config among all nodes of the cluster.
* There must be no breaking changes between those 2 versions.

Since the rollout is a controlled process of pushing a config to all nodes, it easy to resume it from any point and make
sure all nodes have the latest version. Do not proceed to another rollout if the previous one has not finished.

A diff between two versions is validated by `cluster.ValidateTransition()`:

* New nodes can be added, but existing nodes cannot be removed if they have at least one assigned replica in the old config.
* New applications can be added, but existing applications cannot be removed.
* Shards cannot be removed, have their bounds changed, or change their parent.
* Shard states follow the split lifecycle: an `active` shard can start `splitting` (there must be at least 2
  `activating` children of it in the new config), a `splitting` shard can retire to `inactive`, and an `activating`
  shard can become `active`. Any other state change is forbidden: an `active` shard cannot become `inactive` or
  `activating` directly, and an `inactive` shard never changes state again.
* A shard added to an existing application cannot be created `active`.
* New replicas can be added (even exceeding the replication factor), but replicas cannot be both added and removed 
  in the same transition.
* All existing replicas must remain assigned to the same nodes (no reassignment of existing replicas).
* The new config has a greater version than the old config.

With such small and safe diffs having a stale config on some Monstera clients or being in the middle of a rollout to nodes 
will not cause any data loss or routing problems between clients and nodes.

## Creating a new config

At minimum a working cluster config should have 3 nodes and 1 application.

Use `monstera` CLI tool to initialize a new config. The first command will create `./cluster_config.json` file, other
commands will modify it:

```shell
$ monstera config init \
  --node-id=node-01 --node-address=ip-10-0-10-14.us-west-2.compute.internal:7000 \
  --node-id=node-02 --node-address=ip-10-0-10-63.us-west-2.compute.internal:7000 \
  --node-id=node-03 --node-address=ip-10-0-10-78.us-west-2.compute.internal:7000 \
  --output=./cluster_config.json

$ monstera config add-application \
  --config=./cluster_config.json \
  --name=MyFirstApplication \
  --implementation=MyFirstApplication \
  --shards-count=16
  
$ monstera config add-application \
  --config=./cluster_config.json \
  --name=MySecondApplication \
  --implementation=MySecondApplication \
  --shards-count=32
```

## Shard Splitting

Shards can be split into 2 or more pieces under load with no downtime. Intermediate state during the split and the history
of previous splits are all stored in the cluster config.

Each shard has a **state**:

* `active` — serves its key range and has no children.
* `splitting` — still serves its key range while its 2 or more `activating` children (referring to it by
  `parent_id`) are being prepared to take over; the children must exactly cover its range.
* `activating` — a child of a splitting shard that is being activated to take over its part of the parent's
  range; not serving yet, no children.
* `inactive` — retired after a completed split; not serving, its children serve its range instead.

Active and splitting shards must cover the full keyspace with no gaps and no overlaps. Inactive and activating
shards may overlap them. An `activating` child's replicas must live on exactly the same nodes as its splitting
parent's replicas — split seeding is node-local.
