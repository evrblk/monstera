package nodelifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/transport"
	"github.com/evrblk/monstera/transport/local"
)

// TestReconcileAddAndRemoveReplica checks that applying a config that adds a
// replica makes the node create it and the shard leader add it as a voter and let
// it catch up; applying a config that removes it tears it down and shrinks the
// Raft group. Driven directly through Node.UpdateClusterConfig over the local
// transport.
func TestReconcileAddAndRemoveReplica(t *testing.T) {
	// 4 nodes; one shard with 3 replicas on node_1/2/3. node_4 hosts nothing yet.
	config := fourNodeConfig(t)
	shardId := config.Applications[0].Shards[0].Id

	trans := local.NewLocalTransport()
	t.Cleanup(func() { _ = trans.Close() })

	nodes := map[string]*monstera.Node{}
	for _, n := range config.Nodes {
		node := startNode(t, t.TempDir(), n.Id, config, trans, true)
		nodes[n.Id] = node
		t.Cleanup(node.Stop)
	}
	nodeIds := []string{"node_1", "node_2", "node_3", "node_4"}

	// Wait for a leader, then commit a few entries so catch-up is meaningful.
	requireLeader(t, trans, nodeIds)
	client := monstera.NewMonsteraClient(monstera.NewStaticClusterConfigProvider(config), trans, monstera.DefaultClientConfig())
	require.NoError(t, client.Start(context.Background()))
	t.Cleanup(client.Stop)
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = client.UpdateShard(ctx, "Core", shardId, []byte("payload"))
		cancel()
	}

	// --- Add a replica on node_4 (add-only transition) ---
	v2 := proto.Clone(config).(*cluster.Config)
	_, err := v2.AddReplica("Core", shardId, "replica_4", "node_4")
	require.NoError(t, err)
	v2.IncrementVersion()
	for _, id := range nodeIds {
		require.NoError(t, nodes[id].UpdateClusterConfig(context.Background(), v2))
	}

	// node_4 hosts replica_4, the leader now has 3 peers (4 members), and
	// replica_4 catches up to the leader's commit index.
	require.Eventually(t, func() bool {
		states := allReplicaStates(t, trans, nodeIds)
		leader, ok := leaderState(states)
		if !ok || leader.Stats.NumPeers != 3 {
			return false
		}
		r4, ok := replicaState(states, "replica_4")
		return ok && r4.Stats.AppliedIndex >= leader.Stats.CommitIndex && r4.Stats.CommitIndex > 0
	}, 20*time.Second, 200*time.Millisecond, "replica_4 did not join and catch up")

	// --- Remove replica_4 (remove-only transition) ---
	v3 := proto.Clone(v2).(*cluster.Config)
	removeReplica(t, v3, shardId, "replica_4")
	v3.IncrementVersion()
	for _, id := range nodeIds {
		require.NoError(t, nodes[id].UpdateClusterConfig(context.Background(), v3))
	}

	// node_4 hosts nothing, and the group is back to 3 members (leader has 2 peers).
	require.Eventually(t, func() bool {
		states := allReplicaStates(t, trans, nodeIds)
		if _, ok := replicaState(states, "replica_4"); ok {
			return false
		}
		leader, ok := leaderState(states)
		return ok && leader.Stats.NumPeers == 2
	}, 20*time.Second, 200*time.Millisecond, "replica_4 was not removed / group did not shrink")
}

func fourNodeConfig(t *testing.T) *cluster.Config {
	t.Helper()
	c := cluster.CreateEmptyConfig()
	for i := 1; i <= 4; i++ {
		_, err := c.CreateNode("node_"+string(rune('0'+i)), "node_"+string(rune('0'+i)))
		require.NoError(t, err)
	}
	a, err := c.CreateApplication("Core", "Core", 3)
	require.NoError(t, err)
	s, err := c.CreateShard(a.Name, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, "")
	require.NoError(t, err)
	for i := 1; i <= 3; i++ {
		_, err := c.CreateReplica(a.Name, s.Id, "node_"+string(rune('0'+i)))
		require.NoError(t, err)
	}
	require.NoError(t, c.Validate())
	return c
}

func removeReplica(t *testing.T, c *cluster.Config, shardId, replicaId string) {
	t.Helper()
	for _, a := range c.Applications {
		for _, s := range a.Shards {
			if s.Id != shardId {
				continue
			}
			kept := s.Replicas[:0]
			for _, r := range s.Replicas {
				if r.Id != replicaId {
					kept = append(kept, r)
				}
			}
			s.Replicas = kept
			return
		}
	}
	t.Fatalf("shard %s not found", shardId)
}

func allReplicaStates(t *testing.T, trans *local.LocalTransport, nodeIds []string) []*transport.ReplicaState {
	t.Helper()
	var all []*transport.ReplicaState
	for _, id := range nodeIds {
		states, err := trans.ListReplicaStates(context.Background(), id)
		if err != nil {
			continue
		}
		all = append(all, states...)
	}
	return all
}

func leaderState(states []*transport.ReplicaState) (*transport.ReplicaState, bool) {
	for _, s := range states {
		if s.RaftState == transport.RaftStateLeader {
			return s, true
		}
	}
	return nil, false
}

func replicaState(states []*transport.ReplicaState, replicaId string) (*transport.ReplicaState, bool) {
	for _, s := range states {
		if s.ReplicaId == replicaId {
			return s, true
		}
	}
	return nil, false
}

func requireLeader(t *testing.T, trans *local.LocalTransport, nodeIds []string) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, ok := leaderState(allReplicaStates(t, trans, nodeIds))
		return ok
	}, 15*time.Second, 200*time.Millisecond, "no leader elected")
}
