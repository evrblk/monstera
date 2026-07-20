package control

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/control"
	"github.com/evrblk/monstera/internal/integration_test/testcore"
	"github.com/evrblk/monstera/transport"
	"github.com/evrblk/monstera/transport/grpc"
)

// TestMoveShardSequenceOverGrpc runs a move-shard sequence (add replica -> bake ->
// remove old replica) against a real 4-node gRPC cluster. A shard hosted on
// node_1/2/3 is moved off node_1 onto node_4. The executor's caught-up gate blocks
// until node_4's new replica has actually caught up, so a passing Run proves the
// catch-up happened; we then assert the membership moved and a leader remains.
func TestMoveShardSequenceOverGrpc(t *testing.T) {
	var addrs [4]string
	for i := range addrs {
		addrs[i] = freeAddr(t)
	}
	base := moveShardBaseConfig(t, addrs)
	shardId := base.Applications[0].Shards[0].Id

	nodeConfig := monstera.DefaultMonsteraNodeConfig
	nodeConfig.UseInMemoryRaftStore = true

	admin := grpc.NewAdminClient()
	t.Cleanup(func() { _ = admin.Close() })

	cl := &grpcCluster{}
	t.Cleanup(cl.stop)

	ids := []string{"node_1", "node_2", "node_3", "node_4"}
	for i := range ids {
		startGrpcNode(t, cl, nodeConfig, addrs[i], testcore.NopDescriptors())
	}
	for i, id := range ids {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		require.NoErrorf(t, admin.Bootstrap(ctx, addrs[i], id, base), "bootstrap %s", id)
		cancel()
	}

	// Wait for the shard (on node_1/2/3) to elect a leader before moving it.
	requireShardLeader(t, admin, addrs[:3])

	// Move the shard's replica from node_1 to node_4, with a short bake.
	seq, err := control.PlanMoveShard(base, shardId, "node_1", "node_4", 500*time.Millisecond)
	require.NoError(t, err)
	require.Len(t, seq.Steps, 3)
	require.Equal(t, control.StepBake, seq.Steps[1].Kind, "middle step must be the bake step")

	exec := control.NewExecutor(admin, base, filepath.Join(t.TempDir(), "seq.json"),
		control.Options{PollInterval: 50 * time.Millisecond, RPCTimeout: 5 * time.Second, Logf: t.Logf})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	require.NoError(t, exec.Run(ctx, seq))
	require.Equal(t, control.StatusCompleted, seq.Status)

	// Every node converged to base+2.
	for _, addr := range addrs {
		cctx, c := context.WithTimeout(context.Background(), time.Second)
		cfg, err := admin.GetClusterConfig(cctx, addr)
		c()
		require.NoErrorf(t, err, "GetClusterConfig(%s)", addr)
		require.EqualValues(t, base.Version+2, cfg.Version, "node at %s not converged", addr)
	}

	// The shard's replicas moved off node_1 onto node_4 (count preserved).
	cfg, err := admin.GetClusterConfig(context.Background(), addrs[1])
	require.NoError(t, err)
	shard, err := cfg.GetShard(shardId)
	require.NoError(t, err)
	require.Len(t, shard.Replicas, 3)
	nodeSet := map[string]bool{}
	for _, r := range shard.Replicas {
		nodeSet[r.NodeId] = true
	}
	require.False(t, nodeSet["node_1"], "shard should no longer have a replica on node_1")
	require.True(t, nodeSet["node_4"], "shard should now have a replica on node_4")

	// The shard still has a leader among the new membership.
	requireShardLeader(t, admin, []string{addrs[1], addrs[2], addrs[3]})
}

func moveShardBaseConfig(t *testing.T, addrs [4]string) *cluster.Config {
	t.Helper()
	c := cluster.CreateEmptyConfig()
	ids := []string{"node_1", "node_2", "node_3", "node_4"}
	for i, id := range ids {
		_, err := c.CreateNode(id, addrs[i])
		require.NoError(t, err)
	}
	a, err := c.CreateApplication("Core", "Core", 3)
	require.NoError(t, err)
	s, err := c.CreateShard(a.Name, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, "")
	require.NoError(t, err)
	// Replicas on node_1/2/3; node_4 hosts none of this shard yet.
	for _, id := range []string{"node_1", "node_2", "node_3"} {
		_, err := c.CreateReplica(a.Name, s.Id, id)
		require.NoError(t, err)
	}
	require.NoError(t, c.Validate())
	return c
}

func requireShardLeader(t *testing.T, admin *grpc.AdminClient, addrs []string) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, addr := range addrs {
			cctx, c := context.WithTimeout(context.Background(), time.Second)
			states, err := admin.ListReplicaStates(cctx, addr)
			c()
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
	}, 20*time.Second, 200*time.Millisecond, "shard never elected a leader")
}
