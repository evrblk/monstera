package nodelifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/internal/integration_test/testutils"
	"github.com/evrblk/monstera/transport/local"
)

// TestReconcileAddAndRemoveReplica checks that applying a config that adds a
// replica makes the node create it and the shard leader add it as a voter and let
// it catch up; applying a config that removes it tears it down and shrinks the
// Raft group. Driven directly through Node.UpdateClusterConfig over the local
// transport.
func TestReconcileAddAndRemoveReplica(t *testing.T) {
	// 4 nodes; one shard with 3 replicas on node_1/2/3. node_4 hosts nothing yet.
	config := testutils.SingleShardLocalConfig(t, 4, 3)
	shardId := config.Applications[0].Shards[0].Id

	trans := local.NewLocalTransport()
	t.Cleanup(func() { _ = trans.Close() })

	nodes := map[string]*monstera.Node{}
	for _, n := range config.Nodes {
		node := testutils.StartLocalNode(t, t.TempDir(), n.Id, config, trans, true)
		nodes[n.Id] = node
		t.Cleanup(node.Stop)
	}
	nodeIds := []string{"node_1", "node_2", "node_3", "node_4"}

	// Wait for a leader, then commit a few entries so catch-up is meaningful.
	testutils.RequireLeader(t, trans, nodeIds, nil)
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
		states := testutils.AllReplicaStates(trans, nodeIds)
		leader, ok := testutils.FindLeader(states)
		if !ok || leader.Stats.NumPeers != 3 {
			return false
		}
		r4, ok := testutils.FindReplicaState(states, "replica_4")
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
		states := testutils.AllReplicaStates(trans, nodeIds)
		if _, ok := testutils.FindReplicaState(states, "replica_4"); ok {
			return false
		}
		leader, ok := testutils.FindLeader(states)
		return ok && leader.Stats.NumPeers == 2
	}, 20*time.Second, 200*time.Millisecond, "replica_4 was not removed / group did not shrink")
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
