package split

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/control"
	"github.com/evrblk/monstera/internal/integration_test/testutils"
	"github.com/evrblk/monstera/transport/grpc"
)

// TestSplitShardSequenceLiveCutover is the acceptance test for the full
// PlanSplitShard sequence (declare -> seed -> CUTOFF -> flip -> bake) running
// over a real 3-node gRPC cluster while a writer keeps writing THROUGH the
// cutoff, once per core storage model. It proves the split's two headline
// guarantees for every CoreType:
//
//   - Zero write downtime: not a single write fails during the whole split —
//     writes that hit the frozen parent are re-routed to the children
//     (delayed by at most the child election, never rejected).
//   - Guaranteed consistency: every acknowledged write — before, during and
//     after the cutoff — is served correctly by the children afterwards.
func TestSplitShardSequenceLiveCutover(t *testing.T) {
	for _, cc := range coreCases() {
		t.Run(cc.name, func(t *testing.T) {
			runLiveCutover(t, cc)
		})
	}
}

func runLiveCutover(t *testing.T, cc coreCase) {
	var addrs [3]string
	copy(addrs[:], testutils.FreeAddrs(t, 3))

	admin := grpc.NewAdminClient()
	t.Cleanup(func() { _ = admin.Close() })

	// Cleanup runs LIFO: data dirs and core stores are created before the
	// cluster so teardown stops the nodes first, then closes the stores, then
	// removes the dirs (see restart_test.go for the failure mode).
	ids := []string{"node_1", "node_2", "node_3"}
	dirs := make([]string, len(ids))
	for i := range ids {
		dirs[i] = t.TempDir()
	}
	stores := newCoreStores(t, cc, 3)
	cl := testutils.NewGrpcCluster(t)

	base := splitTestConfig(addrs, 1) // single full-range active shard
	require.NoError(t, base.Validate())

	for i := range ids {
		cl.StartNodeAt(t, dirs[i], testutils.InMemoryNodeConfig(), addrs[i], cc.newDescriptors(stores.get(i)))
	}
	testutils.BootstrapNodes(t, admin, addrs[:], ids, base)
	testutils.RequireLeader(t, admin, addrs[:], parentReplicaIds())

	// Continuous writer, running through the entire sequence. Every write must
	// succeed: the split's zero-downtime contract.
	stub := testutils.NewPlaygroundStub(base)
	written := newWrittenSet()
	var writeFailures atomic.Int64
	var lastFailure atomic.Value
	stopWriter := startWriter(stub, written, 1, &writeFailures, &lastFailure)

	// Let some writes land pre-split.
	require.Eventually(t, func() bool { k, _ := written.snapshot(); return len(k) > 50 },
		15*time.Second, 50*time.Millisecond, "writer never got going")

	// Plan and execute the whole split sequence while the writer runs.
	seq, err := control.PlanSplitShard(base, parentShard, []byte{0x80, 0x00, 0x00, 0x00}, 500*time.Millisecond)
	require.NoError(t, err)
	require.Len(t, seq.Steps, 4)
	require.Equal(t, control.StepSendCommand, seq.Steps[1].Kind, "second step must be the cutoff")

	exec := control.NewExecutor(admin, base, filepath.Join(t.TempDir(), "seq.json"),
		control.Options{PollInterval: 50 * time.Millisecond, RPCTimeout: 5 * time.Second, Logf: t.Logf})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	require.NoError(t, exec.Run(ctx, seq))
	require.Equal(t, control.StatusCompleted, seq.Status)

	// Keep writing a little longer against the split topology, then stop.
	time.Sleep(500 * time.Millisecond)
	stopWriter()

	// Zero write downtime: nothing failed, before, during or after the cutoff.
	require.EqualValues(t, 0, writeFailures.Load(), "writes failed during the split: last: %v", lastFailure.Load())

	// The cluster converged to the split topology.
	finalCfg, err := admin.GetClusterConfig(context.Background(), addrs[0])
	require.NoError(t, err)
	require.EqualValues(t, base.Version+2, finalCfg.Version)
	parent, err := finalCfg.GetShard(parentShard)
	require.NoError(t, err)
	require.Equal(t, cluster.ShardState_SHARD_STATE_INACTIVE, parent.State)
	var childIds []string
	for _, a := range finalCfg.GetApplications() {
		for _, sh := range a.Shards {
			if sh.ParentId == parentShard {
				require.Equal(t, cluster.ShardState_SHARD_STATE_ACTIVE, sh.State)
				childIds = append(childIds, sh.Id)
			}
		}
	}
	require.Len(t, childIds, 2)

	// The frozen parent replicas report it.
	frozenSeen := false
	for _, addr := range addrs {
		states, err := testutils.ListReplicaStates(admin, addr)
		require.NoError(t, err)
		for id, s := range states {
			if parentReplicaIds()[id] {
				require.True(t, s.Frozen, "parent replica %s not reported frozen", id)
				frozenSeen = true
			}
		}
	}
	require.True(t, frozenSeen)

	// Guaranteed consistency: every acknowledged write is served by the
	// children, routed by key range.
	keys, _ := written.snapshot()
	require.Greater(t, len(keys), 100)
	verifyAllKeys(t, finalCfg, written)

	// And the split cluster keeps serving new writes.
	post := testutils.NewPlaygroundStub(finalCfg)
	pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err = post.Update(pctx, 999_999, "post-split")
	pcancel()
	require.NoError(t, err)
	pctx, pcancel = context.WithTimeout(context.Background(), 5*time.Second)
	v, err := post.Read(pctx, 999_999)
	pcancel()
	require.NoError(t, err)
	require.Equal(t, "post-split", v)
}
