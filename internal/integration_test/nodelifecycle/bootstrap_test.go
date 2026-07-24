package nodelifecycle

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/internal/integration_test/testcore"
	"github.com/evrblk/monstera/internal/integration_test/testutils"
	"github.com/evrblk/monstera/transport"
	"github.com/evrblk/monstera/transport/local"
)

// TestBootstrapUnprovisionedNode checks that a node started without a config comes
// up UNPROVISIONED (rejecting data-plane RPCs), accepts a Bootstrap call that
// provisions it to READY and persists its identity + config, and on restart
// resumes from disk without a config or node id passed in.
func TestBootstrapUnprovisionedNode(t *testing.T) {
	config := testutils.SingleShardLocalConfig(t, 3, 3) // 3 nodes, one shard, three replicas
	shardId := config.Applications[0].Shards[0].Id
	baseDir := t.TempDir()

	nodeConfig := monstera.DefaultMonsteraNodeConfig // on-disk stores so state survives restart

	// Start node_1 with no seed config and an empty data dir -> UNPROVISIONED.
	trans := local.NewLocalTransport()
	node, err := monstera.NewNode(baseDir, testcore.NopDescriptors(), nodeConfig, trans)
	require.NoError(t, err)
	node.Start()
	trans.Register(node)
	require.Equal(t, monstera.UNPROVISIONED, node.NodeState())

	// Data-plane is rejected while unprovisioned.
	_, err = node.Update(context.Background(), &transport.UpdateRequest{
		ApplicationName: "Core",
		ShardId:         shardId,
		Payload:         []byte("x"),
	})
	require.Error(t, err)

	// Bootstrap provisions the node.
	require.NoError(t, node.Bootstrap(context.Background(), "node_1", config))
	require.Equal(t, monstera.READY, node.NodeState())
	requireFile(t, filepath.Join(baseDir, "config", "node.json"))
	requireFile(t, filepath.Join(baseDir, "config", "cluster.json"))

	// Re-bootstrapping with the same id is an idempotent no-op; a different id is rejected.
	require.NoError(t, node.Bootstrap(context.Background(), "node_1", config))
	require.Equal(t, monstera.READY, node.NodeState())
	require.Error(t, node.Bootstrap(context.Background(), "node_2", config))

	node.Stop()
	trans.Close()

	// Restart with NO seed and NO node id: identity + config come from disk.
	trans2 := local.NewLocalTransport()
	t.Cleanup(func() { _ = trans2.Close() })
	node2, err := monstera.NewNode(baseDir, testcore.NopDescriptors(), nodeConfig, trans2)
	require.NoError(t, err)
	node2.Start()
	t.Cleanup(node2.Stop)
	require.Equal(t, monstera.READY, node2.NodeState())
	require.Equal(t, "node_1", node2.NodeId())
}
