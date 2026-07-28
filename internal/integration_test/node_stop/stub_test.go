package test

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/internal/integration_test/testcore"
	"github.com/evrblk/monstera/internal/integration_test/testutils"
)

func TestPlaygroundApiMonsteraStub_ReadAndUpdate(t *testing.T) {
	clusterConfig := NewTestClusterConfig(t)
	stub := testutils.NewPlaygroundStub(clusterConfig)
	cl := NewCluster(t, clusterConfig)

	require.Eventually(t, func() bool {
		for _, n := range cl.Nodes {
			if n.NodeState() != monstera.NodeStateReady {
				return false
			}
		}
		return true
	}, 5*time.Second, 100*time.Millisecond, "nodes not ready")
	log.Println("Nodes are ready")

	log.Println("Sending requests")
	for range 5000 {
		// Test reading nonexistent key
		key := rand.Uint64()

		resp1, err := stub.Read(context.Background(), key)
		require.NoError(t, err)
		require.Empty(t, resp1, "Expected empty result for nonexistent key")

		// Update key
		value := fmt.Sprintf("test value %d", key)
		resp2, err := stub.Update(context.Background(), key, value)
		require.NoError(t, err)
		require.Equal(t, value, resp2)

		// Test reading existing key
		resp3, err := stub.Read(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, value, resp3)
	}

	log.Println("Killing a node")

	cl.Nodes[0].Stop()
	cl.Servers[0].Stop()

	log.Println("Sending requests")
	for range 5000 {
		key := rand.Uint64()

		// Update key
		value := fmt.Sprintf("test value %d", key)
		resp2, err := stub.Update(context.Background(), key, value)
		require.NoError(t, err)
		require.Equal(t, value, resp2)

		// Test reading existing key
		resp3, err := stub.Read(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, value, resp3)
	}

	log.Println("Test completed")
}

// NewCluster starts one gRPC node per config entry (fresh t.TempDir data
// dirs) and bootstraps each in-process with the cluster config.
func NewCluster(t *testing.T, clusterConfig *cluster.Config) *testutils.GrpcCluster {
	t.Helper()
	cl := testutils.NewGrpcCluster(t)
	for _, n := range clusterConfig.Nodes {
		node := cl.StartNode(t, testutils.InMemoryNodeConfig(), n.GrpcAddress, testcore.InMemoryPlaygroundDescriptors())
		// Fresh data dir: the node comes up UNPROVISIONED; bootstrap it
		// in-process (mirrors an admin Bootstrap over the wire).
		require.NoError(t, node.Bootstrap(context.Background(), n.Id, clusterConfig))
	}
	return cl
}

// NewTestClusterConfig builds a 3-node, 4-shard playground topology on
// dynamically allocated addresses.
func NewTestClusterConfig(t *testing.T) *cluster.Config {
	t.Helper()
	addrs := testutils.FreeAddrs(t, 3)

	applications := []*cluster.Application{
		{
			Name:              "Core",
			Implementation:    "Core",
			ReplicationFactor: 3,
			Shards: []*cluster.Shard{
				{
					Id:         "shrd_01",
					LowerBound: 0x00000000,
					UpperBound: 0x3fffffff,
					State:      cluster.ShardState_SHARD_STATE_ACTIVE,
					Replicas: []*cluster.Replica{
						{Id: "rplc_01", NodeId: "node_01"},
						{Id: "rplc_02", NodeId: "node_02"},
						{Id: "rplc_03", NodeId: "node_03"},
					},
				},
				{
					Id:         "shrd_02",
					LowerBound: 0x40000000,
					UpperBound: 0x7fffffff,
					State:      cluster.ShardState_SHARD_STATE_ACTIVE,
					Replicas: []*cluster.Replica{
						{Id: "rplc_04", NodeId: "node_01"},
						{Id: "rplc_05", NodeId: "node_02"},
						{Id: "rplc_06", NodeId: "node_03"},
					},
				},
				{
					Id:         "shrd_03",
					LowerBound: 0x80000000,
					UpperBound: 0xbfffffff,
					State:      cluster.ShardState_SHARD_STATE_ACTIVE,
					Replicas: []*cluster.Replica{
						{Id: "rplc_07", NodeId: "node_01"},
						{Id: "rplc_08", NodeId: "node_02"},
						{Id: "rplc_09", NodeId: "node_03"},
					},
				},
				{
					Id:         "shrd_04",
					LowerBound: 0xc0000000,
					UpperBound: 0xffffffff,
					State:      cluster.ShardState_SHARD_STATE_ACTIVE,
					Replicas: []*cluster.Replica{
						{Id: "rplc_10", NodeId: "node_01"},
						{Id: "rplc_11", NodeId: "node_02"},
						{Id: "rplc_12", NodeId: "node_03"},
					},
				},
			},
		},
	}

	nodes := []*cluster.Node{
		{Id: "node_01", GrpcAddress: addrs[0]},
		{Id: "node_02", GrpcAddress: addrs[1]},
		{Id: "node_03", GrpcAddress: addrs[2]},
	}

	clusterConfig, err := cluster.LoadConfig(applications, nodes, nil, 1)
	require.NoError(t, err)
	return clusterConfig
}
