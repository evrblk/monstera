package control

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/control"
	"github.com/evrblk/monstera/internal/integration_test/testcore"
	"github.com/evrblk/monstera/internal/integration_test/testutils"
	"github.com/evrblk/monstera/transport/grpc"
)

// TestAddNodeSequenceOverGrpc runs the add-node control sequence against a real
// in-process gRPC cluster: three provisioned nodes plus a fourth left
// UNPROVISIONED. PlanAddNode + Executor push the new config to the existing nodes
// and bootstrap the new one; every node must converge to base+1 with node_4
// present and READY.
func TestAddNodeSequenceOverGrpc(t *testing.T) {
	addrs := testutils.FreeAddrs(t, 4)
	// Base config knows only the first three nodes; node_4 is added by the sequence.
	base := testutils.SingleShardConfig(t, addrs[:3], 3)

	admin := grpc.NewAdminClient()
	t.Cleanup(func() { _ = admin.Close() })

	// Start all four gRPC nodes (all UNPROVISIONED to begin with).
	cl := testutils.NewGrpcCluster(t)
	for _, addr := range addrs[:3] {
		cl.StartNode(t, testutils.InMemoryNodeConfig(), addr, testcore.NopDescriptors())
	}
	node4 := cl.StartNode(t, testutils.InMemoryNodeConfig(), addrs[3], testcore.NopDescriptors())

	// Bootstrap the three existing nodes into the base cluster.
	testutils.BootstrapNodes(t, admin, addrs[:3], []string{"node_1", "node_2", "node_3"}, base)
	require.Equal(t, monstera.UNPROVISIONED, node4.NodeState(), "node_4 must be unprovisioned before add-node")

	// Plan and run add-node for node_4.
	seq, err := control.PlanAddNode(base, "node_4", addrs[3])
	require.NoError(t, err)

	exec := control.NewExecutor(admin, base, filepath.Join(t.TempDir(), "seq.json"),
		control.Options{PollInterval: 50 * time.Millisecond, RPCTimeout: 5 * time.Second, Logf: t.Logf})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, exec.Run(ctx, seq))

	// Sequence completed; every node is at base+1 with node_4 present, and node_4 is READY.
	require.Equal(t, control.StatusCompleted, seq.Status)
	require.Equal(t, monstera.READY, node4.NodeState())
	for _, addr := range addrs {
		cctx, c := context.WithTimeout(context.Background(), time.Second)
		cfg, err := admin.GetClusterConfig(cctx, addr)
		c()
		require.NoErrorf(t, err, "GetClusterConfig(%s)", addr)
		require.EqualValues(t, base.Version+1, cfg.Version, "node at %s not converged", addr)
		_, err = cfg.GetNode("node_4")
		require.NoErrorf(t, err, "node_4 missing from config on node at %s", addr)
	}

	// Re-running the completed sequence is a clean no-op.
	require.NoError(t, exec.Run(ctx, seq))
}
