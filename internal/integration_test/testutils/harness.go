// Package testutils holds the shared harness for integration tests: network
// helpers, gRPC and local-transport cluster startup, cluster config builders,
// and replica-state assertions. Domain-specific helpers (split topologies,
// data-dir layout checks, ...) stay in their test packages; the application
// core they all run is one of testcore's playground cores (or NopCore).
package testutils

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/internal/integration_test/testcore"
	"github.com/evrblk/monstera/transport/grpc"
	"github.com/evrblk/monstera/transport/local"
)

// InMemoryNodeConfig returns the default node config with the in-memory Raft
// store — the standard configuration for integration tests.
func InMemoryNodeConfig() monstera.NodeConfig {
	nodeConfig := monstera.DefaultMonsteraNodeConfig
	nodeConfig.UseInMemoryRaftStore = true
	return nodeConfig
}

// FreeAddr returns a currently-free 127.0.0.1 address. There is a small window
// between closing the probe listener and the node binding it, which is
// acceptable for a test.
func FreeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
}

// FreeAddrs returns n currently-free 127.0.0.1 addresses.
func FreeAddrs(t *testing.T, n int) []string {
	t.Helper()
	addrs := make([]string, n)
	for i := range addrs {
		addrs[i] = FreeAddr(t)
	}
	return addrs
}

// WaitForListen blocks until something accepts TCP connections on addr.
func WaitForListen(t *testing.T, addr string) {
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

// GrpcCluster tracks in-process nodes and gRPC servers so they can be torn
// down in the right order.
type GrpcCluster struct {
	Nodes   []*monstera.Node
	Servers []*grpc.GrpcServer
}

// NewGrpcCluster returns an empty cluster whose Stop is registered as a test
// cleanup.
func NewGrpcCluster(t *testing.T) *GrpcCluster {
	t.Helper()
	cl := &GrpcCluster{}
	t.Cleanup(cl.Stop)
	return cl
}

// Stop tears the cluster down in two phases: stop every node first (each
// moves to STOPPED, rejecting Raft RPCs and cancelling its outbound streams),
// then GracefulStop every server. Stopping all nodes before any server
// prevents GracefulStop from blocking on Raft streams held open by peers that
// haven't been stopped yet — which deadlocks once a shard replica spans
// several running nodes.
func (c *GrpcCluster) Stop() {
	for _, n := range c.Nodes {
		n.Stop()
	}
	for _, s := range c.Servers {
		s.Stop()
	}
}

// StartNode creates a monstera node with a fresh t.TempDir data dir, starts
// it (UNPROVISIONED — bootstrap separately), and serves it over gRPC on addr.
func (c *GrpcCluster) StartNode(t *testing.T, nodeConfig monstera.NodeConfig, addr string, descriptors monstera.ApplicationCoreDescriptors) *monstera.Node {
	t.Helper()
	return c.StartNodeAt(t, t.TempDir(), nodeConfig, addr, descriptors)
}

// StartNodeAt is StartNode with a caller-owned data dir, so a test can stop a
// node and start a fresh one over the same durable state (kill/restart
// scenarios). A dir holding a provisioned node's state comes back READY; a
// fresh dir comes up UNPROVISIONED.
func (c *GrpcCluster) StartNodeAt(t *testing.T, baseDir string, nodeConfig monstera.NodeConfig, addr string, descriptors monstera.ApplicationCoreDescriptors) *monstera.Node {
	t.Helper()
	node, err := monstera.NewNode(baseDir, descriptors, nodeConfig, grpc.NewDataPlaneClient())
	require.NoError(t, err)
	node.Start()

	server := grpc.NewGrpcServer(node)
	go func() { _ = server.Serve(addr) }()
	c.Nodes = append(c.Nodes, node)
	c.Servers = append(c.Servers, server)
	WaitForListen(t, addr)
	return node
}

// KillNode abruptly stops node i: the gRPC server is hard-stopped first
// (closing live connections and Raft streams, so peers see a crash rather
// than a drain), then the node is stopped to release its on-disk stores for
// a later restart. The slot stays in Nodes/Servers; Stop tolerates it.
func (c *GrpcCluster) KillNode(i int) {
	c.Servers[i].Kill()
	c.Nodes[i].Stop()
}

// BootstrapNodes provisions node ids[i] at addrs[i] with cfg over the admin
// plane.
func BootstrapNodes(t *testing.T, admin *grpc.AdminClient, addrs []string, ids []string, cfg *cluster.Config) {
	t.Helper()
	require.Equal(t, len(addrs), len(ids))
	for i, id := range ids {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := admin.Bootstrap(ctx, addrs[i], id, cfg)
		cancel()
		require.NoErrorf(t, err, "bootstrap %s", id)
	}
}

// PushConfig validates cfg and installs it on every node.
func PushConfig(t *testing.T, admin *grpc.AdminClient, addrs []string, cfg *cluster.Config) {
	t.Helper()
	require.NoError(t, cfg.Validate())
	for _, addr := range addrs {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := admin.UpdateClusterConfig(ctx, addr, cfg)
		cancel()
		require.NoErrorf(t, err, "pushing config v%d to %s", cfg.Version, addr)
	}
}

// StartLocalNode creates a monstera node on the local (in-memory) transport,
// starts it, bootstraps it if fresh, and registers it under its assigned id
// so peers can reach it (a fresh node has no id until Bootstrap).
func StartLocalNode(t *testing.T, baseDir, nodeId string, config *cluster.Config, trans *local.LocalTransport, inMemory bool) *monstera.Node {
	t.Helper()
	nodeConfig := monstera.DefaultMonsteraNodeConfig
	nodeConfig.UseInMemoryRaftStore = inMemory

	node, err := monstera.NewNode(baseDir, testcore.NopDescriptors(), nodeConfig, trans)
	require.NoError(t, err)
	node.Start()
	if node.NodeState() == monstera.UNPROVISIONED {
		require.NoError(t, node.Bootstrap(context.Background(), nodeId, config))
	}
	trans.Register(node)
	return node
}

// NewPlaygroundStub returns a testcore playground stub talking to the cluster
// described by cfg over gRPC.
func NewPlaygroundStub(cfg *cluster.Config) *testcore.PlaygroundApiMonsteraStub {
	trans := grpc.NewDataPlaneClient()
	client := monstera.NewMonsteraClient(monstera.NewStaticClusterConfigProvider(cfg), trans, monstera.DefaultClientConfig())
	return testcore.NewPlaygroundApiMonsteraStub(client)
}
