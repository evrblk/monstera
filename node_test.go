package monstera

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera/cluster"
)

// TestNodeGetClusterConfigReturnsClone verifies GetClusterConfig hands back an
// independent deep copy, so a caller cannot mutate the node's live config
// through the returned pointer (and nil stays nil when unprovisioned).
func TestNodeGetClusterConfigReturnsClone(t *testing.T) {
	cfg := &cluster.Config{
		Version: 1,
		Nodes:   []*cluster.Node{{Id: "n", GrpcAddress: "addr"}},
	}
	n := &Node{clusterConfig: cfg}

	got := n.GetClusterConfig()
	require.NotSame(t, cfg, got, "must not return the live config pointer")
	require.EqualValues(t, 1, got.Version)

	got.Version = 99
	require.EqualValues(t, 1, n.clusterConfig.Version, "mutation of the returned config must not reach the node")

	require.Nil(t, (&Node{}).GetClusterConfig(), "unprovisioned node returns nil")
}

// TestNewNodeDefaultsNonPositiveConfig checks that a hand-built NodeConfig is
// usable without filling in every knob. Left at zero, MembershipReconcileInterval
// panics time.NewTicker in the reconcile goroutine, MaxHops rejects every request
// on its first hop, and the timeouts produce already-expired contexts.
func TestNewNodeDefaultsNonPositiveConfig(t *testing.T) {
	node, err := NewNode(t.TempDir(), ApplicationCoreDescriptors{}, NodeConfig{UseInMemoryRaftStore: true}, nil)
	require.NoError(t, err)
	defer node.raftStore.Close()

	expected := DefaultMonsteraNodeConfig
	expected.UseInMemoryRaftStore = true
	require.Equal(t, expected, node.nodeConfig)
}

// TestNodeConfigWithDefaultsKeepsExplicitValues makes sure defaulting only fills
// gaps and never overrides what the caller asked for. UseInMemoryRaftStore is a
// bool with no default: false is a meaningful choice, not an unset field.
func TestNodeConfigWithDefaultsKeepsExplicitValues(t *testing.T) {
	cfg := NodeConfig{
		MaxHops:                     1,
		MaxReadTimeout:              2,
		MaxUpdateTimeout:            3,
		UseInMemoryRaftStore:        true,
		MembershipReconcileInterval: 4,
		MetricsSampleInterval:       5,
		SnapshotSessionTimeout:      6,
		CoreLogPolicy:               CoreLogPolicy{MinLevel: slog.LevelWarn, LeaderOnly: true, IncludeReplay: true},
	}
	require.Equal(t, cfg, cfg.withDefaults())

	require.False(t, NodeConfig{}.withDefaults().UseInMemoryRaftStore)
}
