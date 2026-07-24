package admin

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/internal/integration_test/testcore"
	"github.com/evrblk/monstera/internal/integration_test/testutils"
	"github.com/evrblk/monstera/transport/grpc"
)

// TestAdminBootstrapOverGrpc provisions a fresh, UNPROVISIONED node over the gRPC
// admin plane — addressing it by raw address with no config resolution. This is the
// mechanism behind the `monstera cluster bootstrap-node` command.
func TestAdminBootstrapOverGrpc(t *testing.T) {
	addr := testutils.FreeAddr(t)
	// A 3-node config (the minimum) with a single shard replicated on all three.
	// Only node_1 is actually started here; the others just need to exist in the
	// config — bootstrapping node_1 sets it READY without needing a quorum.
	config := testutils.SingleShardConfig(t, []string{addr, testutils.FreeAddr(t), testutils.FreeAddr(t)}, 3)

	// A fresh node (empty data dir) comes up UNPROVISIONED, serving only admin RPCs.
	cl := testutils.NewGrpcCluster(t)
	node := cl.StartNode(t, testutils.InMemoryNodeConfig(), addr, testcore.NopDescriptors())
	require.Equal(t, monstera.UNPROVISIONED, node.NodeState())

	admin := grpc.NewAdminClient()
	t.Cleanup(func() { _ = admin.Close() })

	// Bootstrap by address: the node adopts its id and the config, and goes READY.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, admin.Bootstrap(ctx, addr, "node_1", config))
	require.Equal(t, monstera.READY, node.NodeState())

	// Re-bootstrapping with the same id is an idempotent no-op; a different id is rejected.
	require.NoError(t, admin.Bootstrap(ctx, addr, "node_1", config))
	require.Error(t, admin.Bootstrap(ctx, addr, "node_2", config))

	// The node now serves its applied config over the admin plane.
	got, err := admin.GetClusterConfig(ctx, addr)
	require.NoError(t, err)
	require.EqualValues(t, config.Version, got.Version)
}

// TestAdminBootstrapAllNodesOverGrpc brings up a whole cluster by bootstrapping
// every node over the admin plane — the flow behind `monstera cluster
// bootstrap-nodes`. Once all nodes are provisioned the shard elects a leader.
func TestAdminBootstrapAllNodesOverGrpc(t *testing.T) {
	addrs := testutils.FreeAddrs(t, 3)
	config := testutils.SingleShardConfig(t, addrs, 3)

	cl := testutils.NewGrpcCluster(t)
	nodes := map[string]*monstera.Node{}
	for _, cn := range config.Nodes {
		node := cl.StartNode(t, testutils.InMemoryNodeConfig(), cn.GrpcAddress, testcore.NopDescriptors())
		require.Equal(t, monstera.UNPROVISIONED, node.NodeState())
		nodes[cn.Id] = node
	}

	admin := grpc.NewAdminClient()
	t.Cleanup(func() { _ = admin.Close() })

	// Bootstrap every node by its configured address.
	for _, cn := range config.Nodes {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		require.NoErrorf(t, admin.Bootstrap(ctx, cn.GrpcAddress, cn.Id, config), "bootstrapping %s", cn.Id)
		cancel()
	}
	for id, node := range nodes {
		require.Equalf(t, monstera.READY, node.NodeState(), "node %s not READY after bootstrap", id)
	}

	// With all three provisioned, the shard's Raft group elects a leader.
	testutils.RequireLeader(t, admin, addrs, nil)
}
