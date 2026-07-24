package split

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/control"
	"github.com/evrblk/monstera/internal/integration_test/testcore"
	"github.com/evrblk/monstera/internal/integration_test/testutils"
	"github.com/evrblk/monstera/transport/grpc"
)

// TestSplitShardSequenceLiveCutover is the Phase-3/5 acceptance test: the full
// PlanSplitShard sequence (declare -> seed -> CUTOFF -> flip -> bake) runs
// over a real 3-node gRPC cluster while a writer keeps writing THROUGH the
// cutoff. It proves the two headline guarantees of notes/shard-split-design.md:
//
//   - Zero write downtime: not a single write fails during the whole split —
//     writes that hit the frozen parent are re-routed to the children
//     (delayed by at most the child election, never rejected).
//   - Guaranteed consistency: every acknowledged write — before, during and
//     after the cutoff — is served correctly by the children afterwards.
func TestSplitShardSequenceLiveCutover(t *testing.T) {
	var addrs [3]string
	copy(addrs[:], testutils.FreeAddrs(t, 3))

	admin := grpc.NewAdminClient()
	t.Cleanup(func() { _ = admin.Close() })

	cl := testutils.NewGrpcCluster(t)

	base := splitTestConfig(addrs, 1) // single full-range active shard
	require.NoError(t, base.Validate())

	ids := []string{"node_1", "node_2", "node_3"}
	for i := range ids {
		cl.StartNode(t, testutils.InMemoryNodeConfig(), addrs[i], testcore.PlaygroundDescriptors())
	}
	testutils.BootstrapNodes(t, admin, addrs[:], ids, base)
	testutils.RequireLeader(t, admin, addrs[:], parentReplicaIds())

	// Continuous writer, running through the entire sequence. Every write must
	// succeed: the split's zero-downtime contract.
	stub := testutils.NewPlaygroundStub(base)
	written := newWrittenSet()
	var writeFailures atomic.Int64
	var lastFailure atomic.Value
	writerStop := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := uint64(1); ; i++ {
			select {
			case <-writerStop:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			v := fmt.Sprintf("value-%d", i)
			_, err := stub.Update(ctx, i, v)
			cancel()
			if err != nil {
				writeFailures.Add(1)
				lastFailure.Store(fmt.Sprintf("key %d: %v", i, err))
				continue
			}
			written.add(i, v)
			time.Sleep(2 * time.Millisecond)
		}
	}()

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
	close(writerStop)
	<-writerDone

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
	verify := testutils.NewPlaygroundStub(finalCfg)
	keys, values := written.snapshot()
	require.Greater(t, len(keys), 100)
	for i, k := range keys {
		rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
		v, err := verify.Read(rctx, k)
		rcancel()
		require.NoErrorf(t, err, "reading key %d after the split", k)
		require.Equalf(t, values[i], v, "key %d has wrong value after the split", k)
	}
	t.Logf("verified %d keys written through a live cutover", len(keys))

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
