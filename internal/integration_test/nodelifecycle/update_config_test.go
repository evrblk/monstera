package nodelifecycle

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/internal/integration_test/testutils"
	"github.com/evrblk/monstera/transport/local"
)

// TestUpdateClusterConfigRejectsInapplicableConfig checks that UpdateClusterConfig
// rejects configs this node cannot run — referencing an unregistered core
// implementation, or no longer containing the node — BEFORE persisting anything.
// Both configs are internally valid and pass ValidateTransition (node_4 hosts no
// replicas), so only the node-local checks stand between them and the disk; if
// one were persisted, reconcile would fail after the write and NewNode would
// refuse to start on restart.
func TestUpdateClusterConfigRejectsInapplicableConfig(t *testing.T) {
	// 4 nodes; replicas live on node_1/2/3, so node_4 hosts nothing and
	// removing it is a legal transition.
	config := testutils.SingleShardLocalConfig(t, 4, 3)
	baseDir := t.TempDir()

	trans := local.NewLocalTransport()
	t.Cleanup(func() { _ = trans.Close() })
	node := testutils.StartLocalNode(t, baseDir, "node_4", config, trans, false)

	// A config with an application whose implementation is not registered.
	v2 := proto.Clone(config).(*cluster.Config)
	a, err := v2.CreateApplication("Ghost", "GhostImpl", 3)
	require.NoError(t, err)
	s, err := v2.CreateShard(a.Name, 0x00000000, 0xffffffff, "")
	require.NoError(t, err)
	for _, nodeId := range []string{"node_1", "node_2", "node_3"} {
		_, err = v2.CreateReplica(a.Name, s.Id, nodeId)
		require.NoError(t, err)
	}
	v2.IncrementVersion()
	require.NoError(t, v2.Validate())

	err = node.UpdateClusterConfig(context.Background(), v2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no core implementation registered for GhostImpl")

	// A config that no longer contains this node.
	v3 := proto.Clone(config).(*cluster.Config)
	kept := v3.Nodes[:0]
	for _, n := range v3.Nodes {
		if n.Id != "node_4" {
			kept = append(kept, n)
		}
	}
	v3.Nodes = kept
	v3.IncrementVersion()
	require.NoError(t, v3.Validate())

	err = node.UpdateClusterConfig(context.Background(), v3)
	require.Error(t, err)
	require.Contains(t, err.Error(), "node node_4 not found in new cluster config")

	// The node is still serving the original config and neither rejected config
	// reached disk: a restart comes up READY at the original version.
	require.Equal(t, monstera.NodeStateReady, node.NodeState())
	require.Equal(t, config.Version, node.GetClusterConfig().Version)
	node.Stop()

	node2 := testutils.StartLocalNode(t, baseDir, "node_4", config, trans, false)
	t.Cleanup(node2.Stop)
	require.Equal(t, monstera.NodeStateReady, node2.NodeState())
	require.Equal(t, config.Version, node2.GetClusterConfig().Version)
}
