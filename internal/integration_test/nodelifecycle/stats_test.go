package nodelifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/transport"
	"github.com/evrblk/monstera/transport/local"
)

// TestListReplicaStatesCarriesStats checks that ListReplicaStates surfaces
// per-replica RaftStats (applied/commit/log indexes) — the catch-up signal used to
// tell whether a replica has caught up to the leader. Exercised over the in-memory
// local transport.
func TestListReplicaStatesCarriesStats(t *testing.T) {
	config := newConfig(t)
	trans := local.NewLocalTransport()
	t.Cleanup(func() { _ = trans.Close() })

	nodes := make([]*monstera.Node, 0, len(config.Nodes))
	for _, n := range config.Nodes {
		node := startNode(t, t.TempDir(), n.Id, config, trans, true)
		nodes = append(nodes, node)
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.Stop()
		}
	})

	shards, err := config.ListShards("Core")
	require.NoError(t, err)
	shardId := shards[0].Id

	// Drive a few committed updates through the leader (Client handles routing
	// and retries while leadership settles).
	client := monstera.NewMonsteraClient(monstera.NewStaticClusterConfigProvider(config), trans, monstera.DefaultClientConfig())
	require.NoError(t, client.Start(context.Background()))
	t.Cleanup(client.Stop)

	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	committed := false
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := client.UpdateShard(ctx, "Core", shardId, []byte("payload"))
		cancel()
		if err == nil {
			committed = true
			break
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	require.True(t, committed, "no update committed before deadline: %v", lastErr)

	// The leader must now report non-zero progress in its RaftStats.
	require.Eventually(t, func() bool {
		for _, n := range config.Nodes {
			states, err := trans.ListReplicaStates(context.Background(), n.Id)
			if err != nil {
				continue
			}
			for _, s := range states {
				if s.RaftState == transport.RaftStateLeader {
					return s.Stats.CommitIndex > 0 &&
						s.Stats.AppliedIndex > 0 &&
						s.Stats.LastLogIndex > 0
				}
			}
		}
		return false
	}, 10*time.Second, 200*time.Millisecond, "leader never reported non-zero RaftStats")
}

func newConfig(t *testing.T) *cluster.Config {
	t.Helper()

	c := cluster.CreateEmptyConfig()
	n1, err := c.CreateNode("node_1", "node_1")
	require.NoError(t, err)
	n2, err := c.CreateNode("node_2", "node_2")
	require.NoError(t, err)
	n3, err := c.CreateNode("node_3", "node_3")
	require.NoError(t, err)

	a, err := c.CreateApplication("Core", "Core", 3)
	require.NoError(t, err)

	s, err := c.CreateShard(a.Name, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, "")
	require.NoError(t, err)

	for _, n := range []*cluster.Node{n1, n2, n3} {
		_, err := c.CreateReplica(a.Name, s.Id, n.Id)
		require.NoError(t, err)
	}

	require.NoError(t, c.Validate())
	return c
}
