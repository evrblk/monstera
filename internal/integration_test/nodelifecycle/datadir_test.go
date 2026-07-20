package nodelifecycle

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/internal/integration_test/testcore"
	"github.com/evrblk/monstera/transport/local"
)

// startNode constructs and starts a single node on baseDir over the local
// transport, registering it so it can receive messages. A node with an empty data
// dir comes up UNPROVISIONED and is bootstrapped in-process with config; a node
// whose data dir already holds a persisted config resumes it (config is ignored).
func startNode(t *testing.T, baseDir, nodeId string, config *cluster.Config, trans *local.LocalTransport, inMemory bool) *monstera.Node {
	t.Helper()
	nodeConfig := monstera.DefaultMonsteraNodeConfig
	nodeConfig.UseInMemoryRaftStore = inMemory

	node, err := monstera.NewNode(baseDir, testcore.NopDescriptors(), nodeConfig, trans)
	require.NoError(t, err)
	node.Start()
	if node.NodeState() == monstera.UNPROVISIONED {
		require.NoError(t, node.Bootstrap(context.Background(), nodeId, config))
	}
	// Register under the node's now-assigned id so peers can reach it (a fresh
	// node has no id until Bootstrap).
	trans.Register(node)
	return node
}

func replicaOnNode(t *testing.T, config *cluster.Config, nodeId string) string {
	t.Helper()
	for _, a := range config.Applications {
		for _, s := range a.Shards {
			for _, r := range s.Replicas {
				if r.NodeId == nodeId {
					return r.Id
				}
			}
		}
	}
	t.Fatalf("no replica for node %s", nodeId)
	return ""
}

// TestDataDirLayout checks that a node's data dir is organized as raft/,
// snapshots/<replicaId>/, config/ — with no top-level per-replica dir.
func TestDataDirLayout(t *testing.T) {
	config := newConfig(t)
	baseDir := t.TempDir()
	trans := local.NewLocalTransport()
	t.Cleanup(func() { _ = trans.Close() })

	// On-disk raft store so the raft/ dir is materialized.
	node := startNode(t, baseDir, "node_1", config, trans, false)
	t.Cleanup(node.Stop)

	replicaId := replicaOnNode(t, config, "node_1")

	requireDir(t, filepath.Join(baseDir, "raft"))
	requireDir(t, filepath.Join(baseDir, "snapshots", replicaId))
	requireFile(t, filepath.Join(baseDir, "config", "cluster.json"))

	// The old top-level per-replica dir must no longer exist.
	_, err := os.Stat(filepath.Join(baseDir, replicaId))
	require.True(t, os.IsNotExist(err), "unexpected legacy per-replica dir at <data_dir>/<replicaId>")

	// snapshots/ holds exactly the per-replica dirs.
	entries, err := os.ReadDir(filepath.Join(baseDir, "snapshots"))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, replicaId, entries[0].Name())
}

// TestConfigPersistsAcrossRestart checks that the applied config is persisted to
// config/cluster.json and resumed from disk on restart (authoritative over any
// config passed at bootstrap time).
func TestConfigPersistsAcrossRestart(t *testing.T) {
	baseDir := t.TempDir()
	seed := newConfig(t) // version 1
	configPath := filepath.Join(baseDir, "config", "cluster.json")

	trans1 := local.NewLocalTransport()
	node1 := startNode(t, baseDir, "node_1", seed, trans1, true)
	require.EqualValues(t, 1, node1.GetClusterConfig().Version)

	// First boot persists the seed.
	onDisk, err := cluster.LoadConfigFromFile(configPath)
	require.NoError(t, err)
	require.EqualValues(t, 1, onDisk.Version)

	// Apply a newer config version; it must land on disk.
	newCfg := proto.Clone(node1.GetClusterConfig()).(*cluster.Config)
	newCfg.IncrementVersion() // version 2
	require.NoError(t, node1.UpdateClusterConfig(context.Background(), newCfg))

	onDisk, err = cluster.LoadConfigFromFile(configPath)
	require.NoError(t, err)
	require.EqualValues(t, 2, onDisk.Version)

	node1.Stop()
	trans1.Close()

	// Restart with a STALE seed (version 1). The persisted v2 must win.
	trans2 := local.NewLocalTransport()
	t.Cleanup(func() { _ = trans2.Close() })
	node2 := startNode(t, baseDir, "node_1", seed, trans2, true)
	t.Cleanup(node2.Stop)
	require.EqualValues(t, 2, node2.GetClusterConfig().Version)
}

func requireDir(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err, "expected directory %s", path)
	require.True(t, info.IsDir(), "%s is not a directory", path)
}

func requireFile(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err, "expected file %s", path)
	require.False(t, info.IsDir(), "%s is a directory, expected file", path)
}
