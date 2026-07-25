package split

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/internal/integration_test/testutils"
	"github.com/evrblk/monstera/transport/grpc"
)

// TestSplitSeedingNodeRestart kills and restarts a node in the middle of
// split seeding, once per core storage model, and proves the split converges
// and completes anyway:
//
//  1. a 3-node cluster (on-disk Raft stores) serves writes; the split is
//     declared and every node starts seeding its dormant children;
//  2. one node is killed (gRPC server hard-stopped, node stopped, its core
//     store closed) while a background writer keeps writing — the surviving
//     quorum keeps accepting writes;
//  3. the node restarts over the same data dir: it reloads the applied
//     splitting config, recreates its dormant children, and resumes seeding
//     from the durable progress (seeded log / catchUpIndex / nothing,
//     depending on the core type);
//  4. after writes stop, every child on every node — including the restarted
//     one — converges to the parent's commit index;
//  5. the CUTOFF is proposed through the parent leader, the splitters
//     finalize and promote the children, the config flips, and every
//     acknowledged write is served correctly by the children.
func TestSplitSeedingNodeRestart(t *testing.T) {
	for _, cc := range coreCases() {
		t.Run(cc.name, func(t *testing.T) {
			runSeedingNodeRestart(t, cc)
		})
	}
}

func runSeedingNodeRestart(t *testing.T, cc coreCase) {
	var addrs [3]string
	copy(addrs[:], testutils.FreeAddrs(t, 3))

	admin := grpc.NewAdminClient()
	t.Cleanup(func() { _ = admin.Close() })

	// Cleanup runs LIFO, so create the data dirs and core stores BEFORE the
	// cluster: teardown must stop every node first, then close the stores,
	// then remove the dirs — removing a live node's on-disk Badger dir wedges
	// its flush goroutine and Close never returns.
	ids := []string{"node_1", "node_2", "node_3"}
	dirs := make([]string, len(ids))
	for i := range ids {
		dirs[i] = t.TempDir()
	}
	stores := newCoreStores(t, cc, 3)
	cl := testutils.NewGrpcCluster(t)

	// On-disk Raft stores: the restarted node must recover its Raft state and
	// its durable seed progress from disk.
	nodeConfig := monstera.DefaultMonsteraNodeConfig

	v1 := splitTestConfig(addrs, 1)
	require.NoError(t, v1.Validate())

	for i := range ids {
		cl.StartNodeAt(t, dirs[i], nodeConfig, addrs[i], cc.newDescriptors(stores.get(i)))
	}
	testutils.BootstrapNodes(t, admin, addrs[:], ids, v1)
	testutils.RequireLeader(t, admin, addrs[:], parentReplicaIds())

	// Initial data set through the normal write path.
	stub := testutils.NewPlaygroundStub(v1)
	written := newWrittenSet()
	for i := uint64(1); i <= 100; i++ {
		writeKey(t, stub, written, i)
	}

	// Background writer through the split declaration, the kill and the
	// restart. Best-effort: with a node down (possibly the leader), writes may
	// transiently fail; only acknowledged writes are recorded and verified.
	stopWriter := startWriter(stub, written, 10_000, nil, nil)

	// Declare the split and wait until every node seeds its children.
	v2 := splitTestConfig(addrs, 2)
	testutils.PushConfig(t, admin, addrs[:], v2)
	requireSeedingVisible(t, admin, addrs[:])

	// Kill node_3 mid-seeding: hard-stop its server (peers see a crash), stop
	// the node, and close its core store so the restart reopens it cold.
	const victim = 2
	cl.KillNode(victim)
	stores.close(victim)
	t.Log("killed node_3 mid-seeding")

	// The surviving quorum keeps serving writes the children must not lose.
	time.Sleep(500 * time.Millisecond)

	// Restart over the same data dir: the node reloads the applied splitting
	// config and resumes seeding from durable progress.
	node := cl.StartNodeAt(t, dirs[victim], nodeConfig, addrs[victim], cc.newDescriptors(stores.reopen(victim)))
	require.Eventually(t, func() bool { return node.NodeState() == monstera.READY },
		15*time.Second, 100*time.Millisecond, "restarted node never became READY")
	t.Log("restarted node_3")

	// Let seeding run a while longer under load, then stop writes.
	time.Sleep(500 * time.Millisecond)
	stopWriter()

	// Convergence: every child on every node — including the restarted one —
	// catches up to the parent's post-writes commit index.
	target := parentCommitIndex(t, admin, addrs[:])
	require.Greater(t, target, uint64(0))
	requireSeededTo(t, admin, addrs[:], target)

	// Cutoff through the parent leader: splitters drain to the cutoff index,
	// finalize the children's Raft state, and promote them in place.
	sendCutoff(t, admin, addrs[:], parentShard)
	requireParentsFrozen(t, admin, addrs[:])
	testutils.RequireLeader(t, admin, addrs[:], childReplicaIds())

	// Flip the config (parent retires, children activate) and verify every
	// acknowledged write is served by the children, routed by key range.
	v3 := splitTestConfig(addrs, 4)
	testutils.PushConfig(t, admin, addrs[:], v3)
	verifyAllKeys(t, v3, written)
}
