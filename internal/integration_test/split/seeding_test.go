// Package split holds integration tests for shard splitting: dormant children
// of a splitting shard are seeded node-locally while the parent keeps serving
// writes, observable via ListReplicaStates, and — once seeded — promotable
// into serving replicas that hold exactly the parent's data partitioned by
// key range.
package split

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/internal/integration_test/testcore"
	"github.com/evrblk/monstera/internal/integration_test/testutils"
	"github.com/evrblk/monstera/transport"
	"github.com/evrblk/monstera/transport/grpc"
)

const (
	parentShard = "Core_p"
	child1Shard = "Core_c1" // [0x00000000, 0x7fffffff]
	child2Shard = "Core_c2" // [0x80000000, 0xffffffff]
)

// TestSplitSeedingOverGrpc drives a full split seeding pass on a real
// 3-node gRPC cluster with an in-memory core (CoreTypeInMemory):
//
//  1. a single full-range shard serves writes;
//  2. the splitting config (parent SPLITTING + two co-located ACTIVATING
//     children) is pushed while writes continue;
//  3. every node seeds its children locally; progress is observable via
//     ListReplicaStates (Seeding + SeededIndex) and catches up to the
//     parent's commit index;
//  4. a config re-push mid-split exercises splitter stop/resume from durable
//     progress;
//  5. after writes stop, the activating->active flip promotes the seeded
//     children in place (pre-baked Raft state, no bootstrap), and every key
//     ever written is readable from the children — proving base snapshots,
//     stamped-key routing, NOOP fillers and the bounds-filtered portable
//     Restore end to end.
//
// (The atomic cutoff — freeze + re-route under live writes — is covered by
// TestSplitShardSequenceLiveCutover; writes are stopped before the flip here.)
func TestSplitSeedingOverGrpc(t *testing.T) {
	var addrs [3]string
	copy(addrs[:], testutils.FreeAddrs(t, 3))

	admin := grpc.NewAdminClient()
	t.Cleanup(func() { _ = admin.Close() })

	cl := testutils.NewGrpcCluster(t)

	v1 := splitTestConfig(addrs, 1)
	require.NoError(t, v1.Validate())

	ids := []string{"node_1", "node_2", "node_3"}
	for i := range ids {
		cl.StartNode(t, testutils.InMemoryNodeConfig(), addrs[i], testcore.PlaygroundDescriptors())
	}
	testutils.BootstrapNodes(t, admin, addrs[:], ids, v1)
	testutils.RequireLeader(t, admin, addrs[:], parentReplicaIds())

	// Initial data set through the normal write path.
	stub := testutils.NewPlaygroundStub(v1)
	written := newWrittenSet()
	for i := uint64(1); i <= 100; i++ {
		writeKey(t, stub, written, i)
	}

	// Background writer: keeps the parent under write load through the split
	// config push and the whole seeding phase.
	writerStop := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := uint64(10_000); ; i++ {
			select {
			case <-writerStop:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			v := fmt.Sprintf("value-%d", i)
			if _, err := stub.Update(ctx, i, v); err == nil {
				written.add(i, v)
			}
			cancel()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Declare the split: parent -> SPLITTING, children ACTIVATING, co-located.
	v2 := splitTestConfig(addrs, 2)
	testutils.PushConfig(t, admin, addrs[:], v2)

	// Every node reports both dormant children with seeding progress.
	requireSeedingVisible(t, admin, addrs[:])

	// Re-push (version bump only): splitters stop and resume from durable
	// progress instead of starting over.
	v2b := splitTestConfig(addrs, 3)
	testutils.PushConfig(t, admin, addrs[:], v2b)

	// Let the writer run a while against the splitting parent, then stop it.
	time.Sleep(1 * time.Second)
	close(writerStop)
	<-writerDone

	// All children on all nodes catch up to the parent's post-writes commit index.
	target := parentCommitIndex(t, admin, addrs[:])
	require.Greater(t, target, uint64(0))
	requireSeededTo(t, admin, addrs[:], target)

	// Flip: parent retires, seeded children activate (promotion in place).
	v3 := splitTestConfig(addrs, 4)
	testutils.PushConfig(t, admin, addrs[:], v3)

	// Every key ever written is served by the children, routed by key range.
	stub3 := testutils.NewPlaygroundStub(v3)
	keys, values := written.snapshot()
	require.NotEmpty(t, keys)
	// The first read waits out the children's leader elections.
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		v, err := stub3.Read(ctx, keys[0])
		return err == nil && v == values[0]
	}, 30*time.Second, 200*time.Millisecond, "children never started serving")

	for i, k := range keys {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		v, err := stub3.Read(ctx, k)
		cancel()
		require.NoErrorf(t, err, "reading key %d after the split", k)
		require.Equalf(t, values[i], v, "key %d has wrong value after the split", k)
	}
	t.Logf("verified %d keys after the split", len(keys))
}

// splitTestConfig builds the test topology at a given phase, encoded by
// version: 1 = single active parent; 2, 3 = parent splitting with two
// co-located activating children; 4 = parent inactive, children active.
func splitTestConfig(addrs [3]string, version int64) *cluster.Config {
	parentState := cluster.ShardState_SHARD_STATE_ACTIVE
	childState := cluster.ShardState_SHARD_STATE_ACTIVATING
	withChildren := version >= 2
	if withChildren {
		parentState = cluster.ShardState_SHARD_STATE_SPLITTING
	}
	if version >= 4 {
		parentState = cluster.ShardState_SHARD_STATE_INACTIVE
		childState = cluster.ShardState_SHARD_STATE_ACTIVE
	}

	shards := []*cluster.Shard{
		{
			Id:         parentShard,
			LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
			UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
			State:      parentState,
			Replicas:   replicasFor(parentShard),
		},
	}
	if withChildren {
		shards = append(shards,
			&cluster.Shard{
				Id:         child1Shard,
				LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
				UpperBound: []byte{0x7f, 0xff, 0xff, 0xff},
				State:      childState,
				ParentId:   parentShard,
				Replicas:   replicasFor(child1Shard),
			},
			&cluster.Shard{
				Id:         child2Shard,
				LowerBound: []byte{0x80, 0x00, 0x00, 0x00},
				UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
				State:      childState,
				ParentId:   parentShard,
				Replicas:   replicasFor(child2Shard),
			},
		)
	}

	return &cluster.Config{
		Version: version,
		Applications: []*cluster.Application{
			{
				Name:              "Core",
				Implementation:    "Core",
				ReplicationFactor: 3,
				Shards:            shards,
			},
		},
		Nodes: []*cluster.Node{
			{Id: "node_1", GrpcAddress: addrs[0]},
			{Id: "node_2", GrpcAddress: addrs[1]},
			{Id: "node_3", GrpcAddress: addrs[2]},
		},
	}
}

func replicasFor(shardId string) []*cluster.Replica {
	replicas := make([]*cluster.Replica, 3)
	for i := 0; i < 3; i++ {
		replicas[i] = &cluster.Replica{
			Id:     fmt.Sprintf("%s_r%d", shardId, i+1),
			NodeId: fmt.Sprintf("node_%d", i+1),
		}
	}
	return replicas
}

func parentReplicaIds() map[string]bool {
	ids := make(map[string]bool)
	for _, r := range replicasFor(parentShard) {
		ids[r.Id] = true
	}
	return ids
}

func childReplicaIdsByNode() map[string][]string {
	byNode := make(map[string][]string)
	for _, shardId := range []string{child1Shard, child2Shard} {
		for _, r := range replicasFor(shardId) {
			byNode[r.NodeId] = append(byNode[r.NodeId], r.Id)
		}
	}
	return byNode
}

// requireSeedingVisible waits until every node reports its two dormant
// children with Seeding set.
func requireSeedingVisible(t *testing.T, admin *grpc.AdminClient, addrs []string) {
	t.Helper()
	byNode := childReplicaIdsByNode()
	require.Eventually(t, func() bool {
		for i, addr := range addrs {
			nodeId := fmt.Sprintf("node_%d", i+1)
			states, err := testutils.ListReplicaStates(admin, addr)
			if err != nil {
				return false
			}
			for _, id := range byNode[nodeId] {
				s, ok := states[id]
				if !ok || !s.Seeding || s.RaftState != transport.RaftStateSeeding {
					return false
				}
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond, "children never reported as seeding")
}

// parentCommitIndex returns the parent leader's commit index.
func parentCommitIndex(t *testing.T, admin *grpc.AdminClient, addrs []string) uint64 {
	t.Helper()
	parents := parentReplicaIds()
	var commit uint64
	require.Eventually(t, func() bool {
		for _, addr := range addrs {
			states, err := testutils.ListReplicaStates(admin, addr)
			if err != nil {
				continue
			}
			for id, s := range states {
				if parents[id] && s.RaftState == transport.RaftStateLeader {
					commit = s.Stats.CommitIndex
					return true
				}
			}
		}
		return false
	}, 15*time.Second, 100*time.Millisecond, "no parent leader found")
	return commit
}

// requireSeededTo waits until every child replica on every node has seeded at
// least up to target.
func requireSeededTo(t *testing.T, admin *grpc.AdminClient, addrs []string, target uint64) {
	t.Helper()
	byNode := childReplicaIdsByNode()
	require.Eventually(t, func() bool {
		for i, addr := range addrs {
			nodeId := fmt.Sprintf("node_%d", i+1)
			states, err := testutils.ListReplicaStates(admin, addr)
			if err != nil {
				return false
			}
			for _, id := range byNode[nodeId] {
				s, ok := states[id]
				if !ok || !s.Seeding || s.SeededIndex < target {
					return false
				}
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond, "children never caught up to parent commit index %d", target)
}

// writtenSet tracks every acknowledged write for post-split verification.
type writtenSet struct {
	mu     sync.Mutex
	keys   []uint64
	values []string
}

func newWrittenSet() *writtenSet { return &writtenSet{} }

func (w *writtenSet) add(key uint64, value string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.keys = append(w.keys, key)
	w.values = append(w.values, value)
}

func (w *writtenSet) snapshot() ([]uint64, []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]uint64(nil), w.keys...), append([]string(nil), w.values...)
}

func writeKey(t *testing.T, stub *testcore.PlaygroundApiMonsteraStub, written *writtenSet, key uint64) {
	t.Helper()
	v := fmt.Sprintf("value-%d", key)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := stub.Update(ctx, key, v)
	require.NoErrorf(t, err, "writing key %d", key)
	written.add(key, v)
}
