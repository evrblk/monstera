// Package split holds integration tests for shard splitting: dormant children
// of a splitting shard are seeded node-locally while the parent keeps serving
// writes, observable via ListReplicaStates, and — once seeded — promotable
// into serving replicas that hold exactly the parent's data partitioned by
// key range.
package split

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera/internal/integration_test/testcore"
	"github.com/evrblk/monstera/internal/integration_test/testutils"
	"github.com/evrblk/monstera/transport/grpc"
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
		cl.StartNode(t, testutils.InMemoryNodeConfig(), addrs[i], testcore.InMemoryPlaygroundDescriptors())
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
	// config push and the whole seeding phase. Best-effort: only acknowledged
	// writes are recorded and verified.
	stopWriter := startWriter(stub, written, 10_000, nil, nil)

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
	stopWriter()

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
