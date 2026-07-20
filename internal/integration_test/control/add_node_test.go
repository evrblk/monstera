package control

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/control"
	"github.com/evrblk/monstera/internal/integration_test/testcore"
	"github.com/evrblk/monstera/transport/grpc"
)

// TestAddNodeSequenceOverGrpc runs the add-node control sequence against a real
// in-process gRPC cluster: three provisioned nodes plus a fourth left
// UNPROVISIONED. PlanAddNode + Executor push the new config to the existing nodes
// and bootstrap the new one; every node must converge to base+1 with node_4
// present and READY.
func TestAddNodeSequenceOverGrpc(t *testing.T) {
	addr1, addr2, addr3, addr4 := freeAddr(t), freeAddr(t), freeAddr(t), freeAddr(t)
	base := threeNodeConfig(t, addr1, addr2, addr3)

	nodeConfig := monstera.DefaultMonsteraNodeConfig
	nodeConfig.UseInMemoryRaftStore = true

	admin := grpc.NewAdminClient()
	t.Cleanup(func() { _ = admin.Close() })

	cl := &grpcCluster{}
	t.Cleanup(cl.stop)

	// Start all four gRPC nodes (all UNPROVISIONED to begin with).
	existing := []struct{ id, addr string }{{"node_1", addr1}, {"node_2", addr2}, {"node_3", addr3}}
	for _, n := range existing {
		startGrpcNode(t, cl, nodeConfig, n.addr, testcore.NopDescriptors())
	}
	node4 := startGrpcNode(t, cl, nodeConfig, addr4, testcore.NopDescriptors())

	// Bootstrap the three existing nodes into the base cluster.
	for _, n := range existing {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		require.NoErrorf(t, admin.Bootstrap(ctx, n.addr, n.id, base), "bootstrapping %s", n.id)
		cancel()
	}
	require.Equal(t, monstera.UNPROVISIONED, node4.NodeState(), "node_4 must be unprovisioned before add-node")

	// Plan and run add-node for node_4.
	seq, err := control.PlanAddNode(base, "node_4", addr4)
	require.NoError(t, err)

	exec := control.NewExecutor(admin, base, filepath.Join(t.TempDir(), "seq.json"),
		control.Options{PollInterval: 50 * time.Millisecond, RPCTimeout: 5 * time.Second, Logf: t.Logf})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, exec.Run(ctx, seq))

	// Sequence completed; every node is at base+1 with node_4 present, and node_4 is READY.
	require.Equal(t, control.StatusCompleted, seq.Status)
	require.Equal(t, monstera.READY, node4.NodeState())
	for _, addr := range []string{addr1, addr2, addr3, addr4} {
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

// grpcCluster tracks the in-process nodes and servers so they can be torn down in
// the right order.
type grpcCluster struct {
	nodes   []*monstera.Node
	servers []*grpc.GrpcServer
}

// stop tears the cluster down in two phases: stop every node first (each moves to
// STOPPED, rejecting Raft RPCs and cancelling its outbound streams), then
// GracefulStop every server. Stopping all nodes before any server prevents
// GracefulStop from blocking on Raft streams held open by peers that haven't been
// stopped yet — which deadlocks once a shard replica spans several running nodes.
func (c *grpcCluster) stop() {
	for _, n := range c.nodes {
		n.Stop()
	}
	for _, s := range c.servers {
		s.Stop()
	}
}

func startGrpcNode(t *testing.T, cl *grpcCluster, nodeConfig monstera.NodeConfig, addr string, descriptors monstera.ApplicationCoreDescriptors) *monstera.Node {
	t.Helper()
	node, err := monstera.NewNode(t.TempDir(), descriptors, nodeConfig, grpc.NewDataPlaneClient())
	require.NoError(t, err)
	node.Start()

	server := grpc.NewGrpcServer(node)
	go func() { _ = server.Serve(addr) }()
	cl.nodes = append(cl.nodes, node)
	cl.servers = append(cl.servers, server)
	waitForListen(t, addr)
	return node
}

func threeNodeConfig(t *testing.T, addr1, addr2, addr3 string) *cluster.Config {
	t.Helper()
	c := cluster.CreateEmptyConfig()
	_, err := c.CreateNode("node_1", addr1)
	require.NoError(t, err)
	_, err = c.CreateNode("node_2", addr2)
	require.NoError(t, err)
	_, err = c.CreateNode("node_3", addr3)
	require.NoError(t, err)
	a, err := c.CreateApplication("Core", "Core", 3)
	require.NoError(t, err)
	s, err := c.CreateShard(a.Name, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, "")
	require.NoError(t, err)
	for _, id := range []string{"node_1", "node_2", "node_3"} {
		_, err = c.CreateReplica(a.Name, s.Id, id)
		require.NoError(t, err)
	}
	require.NoError(t, c.Validate())
	return c
}

func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
}

func waitForListen(t *testing.T, addr string) {
	t.Helper()
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 10*time.Second, 100*time.Millisecond, "gRPC server never started listening on %s", addr)
}
