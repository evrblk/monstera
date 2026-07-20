package admin

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/internal/integration_test/testcore"
	"github.com/evrblk/monstera/transport"
	"github.com/evrblk/monstera/transport/grpc"
)

// TestAdminBootstrapOverGrpc provisions a fresh, UNPROVISIONED node over the gRPC
// admin plane — addressing it by raw address with no config resolution. This is the
// mechanism behind the `monstera cluster bootstrap-node` command.
func TestAdminBootstrapOverGrpc(t *testing.T) {
	addr := freeAddr(t)
	// A 3-node config (the minimum) with a single shard replicated on all three.
	// Only node_1 is actually started here; the others just need to exist in the
	// config — bootstrapping node_1 sets it READY without needing a quorum.
	config := threeNodeConfig(t, addr, freeAddr(t), freeAddr(t))

	// A fresh node (empty data dir) comes up UNPROVISIONED, serving only admin RPCs.
	nodeConfig := monstera.DefaultMonsteraNodeConfig
	nodeConfig.UseInMemoryRaftStore = true
	node, err := monstera.NewNode(t.TempDir(), testcore.NopDescriptors(), nodeConfig, grpc.NewDataPlaneClient())
	require.NoError(t, err)
	node.Start()
	require.Equal(t, monstera.UNPROVISIONED, node.NodeState())

	server := grpc.NewGrpcServer(node)
	go func() { _ = server.Serve(addr) }()
	t.Cleanup(func() {
		// Stop the node before the server: node.Stop() moves it to STOPPED so its
		// RaftMessage handler rejects, which closes peer streams and lets the
		// server's GracefulStop return instead of blocking on them.
		node.Stop()
		server.Stop()
	})
	waitForListen(t, addr)

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
	config := threeNodeConfig(t, freeAddr(t), freeAddr(t), freeAddr(t))

	nodeConfig := monstera.DefaultMonsteraNodeConfig
	nodeConfig.UseInMemoryRaftStore = true

	nodes := map[string]*monstera.Node{}
	for _, cn := range config.Nodes {
		node, err := monstera.NewNode(t.TempDir(), testcore.NopDescriptors(), nodeConfig, grpc.NewDataPlaneClient())
		require.NoError(t, err)
		node.Start()
		require.Equal(t, monstera.UNPROVISIONED, node.NodeState())

		server := grpc.NewGrpcServer(node)
		go func() { _ = server.Serve(cn.GrpcAddress) }()
		t.Cleanup(func() {
			// Stop the node before the server (see TestAdminBootstrapOverGrpc).
			node.Stop()
			server.Stop()
		})
		waitForListen(t, cn.GrpcAddress)
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
	require.Eventually(t, func() bool {
		for _, cn := range config.Nodes {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			states, err := admin.ListReplicaStates(ctx, cn.GrpcAddress)
			cancel()
			if err != nil {
				continue
			}
			for _, s := range states {
				if s.RaftState == transport.RaftStateLeader {
					return true
				}
			}
		}
		return false
	}, 15*time.Second, 200*time.Millisecond, "no leader elected after bootstrapping all nodes")
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

// freeAddr returns a currently-free 127.0.0.1 address. There is a small window
// between closing the probe listener and the node binding it, which is acceptable
// for a test.
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
