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

The whole cluster is defined by `ClusterConfig` which consists of all applications, shards, replicas, and nodes. It is
distributed as a single file to all nodes and clients. Processes or entire hosts can restart/reboot and still have 
access to the cluster config. New config is **pushed** to all nodes when it is changed. Cluster configuration does not 
change often, it can take weeks or months until there is a need to split a shard or move shards to new nodes. The diff
is always small, incremental, and safe to gradually rollout. That means that it is safe to have an old version of the 
config on some nodes in the cluster and a new version on others.  

Here is an example of `cluster_config.json` file:

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
          "replicas": [
            {
              "id": "GrackleLocks_00_07_7a4d737e",
              "node_id": "node-3"
            },
            {
              "id": "GrackleLocks_00_07_df01cea",
              "node_id": "node-1"
            },
            {
              "id": "GrackleLocks_00_07_17c33070",
              "node_id": "node-2"
            }
          ]
        },
        {
          "id": "GrackleLocks_08_0f",
          "lower_bound": "08",
          "upper_bound": "0f",
          "replicas": [
            {
              "id": "GrackleLocks_08_0f_5d3a107c",
              "node_id": "node-3"
            },
            {
              "id": "GrackleLocks_08_0f_255a2f63",
              "node_id": "node-5"
            },
            {
              "id": "GrackleLocks_08_0f_2ace936e",
              "node_id": "node-4"
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
      "id": "node-1",
      "grpc_address": "ip-10-0-10-14.us-west-2.compute.internal:7000"
    },
    {
      "id": "node-2",
      "grpc_address": "ip-10-0-10-63.us-west-2.compute.internal:7000"
    },
    {
      "id": "node-3",
      "grpc_address": "ip-10-0-10-78.us-west-2.compute.internal:7000"
    },
    {
      "id": "node-4",
      "grpc_address": "ip-10-0-10-92.us-west-2.compute.internal:7000"
    },
    {
      "id": "node-5",
      "grpc_address": "ip-10-0-10-31.us-west-2.compute.internal:7000"
    }
  ],
  "version": 6
}
```
