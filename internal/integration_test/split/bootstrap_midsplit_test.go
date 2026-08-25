package split

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera/internal/integration_test/testcore"
	"github.com/evrblk/monstera/internal/integration_test/testutils"
	"github.com/evrblk/monstera/transport/grpc"
)

// TestBootstrapIntoMidSplit is the regression test for M6 (the Bootstrap half):
// a node bootstrapped directly into a config that already contains a SPLITTING
// shard must start its split seeding pipelines rather than leave them dormant.
//
// Fresh nodes are bootstrapped straight into the splitting config (skipping the
// usual active-then-declare flow). Existence of the dormant children alone isn't
// enough — the splitters must actually run — so after writing to the still-serving
// parent we require the children to seed up to the parent's commit index. Before
// the fix, Bootstrap never called startSplitters, so SeededIndex never advanced
// and this timed out.
func TestBootstrapIntoMidSplit(t *testing.T) {
	var addrs [3]string
	copy(addrs[:], testutils.FreeAddrs(t, 3))

	admin := grpc.NewAdminClient()
	t.Cleanup(func() { _ = admin.Close() })

	cl := testutils.NewGrpcCluster(t)

	// The mid-split topology: parent SPLITTING + two co-located ACTIVATING children.
	cfg := splitTestConfig(addrs, 2)
	require.NoError(t, cfg.Validate())

	ids := []string{"node_1", "node_2", "node_3"}
	for i := range ids {
		cl.StartNode(t, testutils.InMemoryNodeConfig(), addrs[i], testcore.InMemoryPlaygroundDescriptors())
	}
	testutils.BootstrapNodes(t, admin, addrs[:], ids, cfg)
	testutils.RequireLeader(t, admin, addrs[:], parentReplicaIds())

	// Children are present as dormant seeding replicas on every node.
	requireSeedingVisible(t, admin, addrs[:])

	// Drive writes through the still-serving parent while the children seed, then
	// quiesce and require the children to catch up to the parent's commit index —
	// the proof that Bootstrap actually started the splitters (SeededIndex would
	// stay put otherwise).
	stub := testutils.NewPlaygroundStub(cfg)
	written := newWrittenSet()
	for i := uint64(1); i <= 100; i++ {
		writeKey(t, stub, written, i)
	}
	stopWriter := startWriter(stub, written, 10_000, nil, nil)
	time.Sleep(1 * time.Second)
	stopWriter()

	target := parentCommitIndex(t, admin, addrs[:])
	require.Greater(t, target, uint64(0))
	requireSeededTo(t, admin, addrs[:], target)
}
