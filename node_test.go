package monstera

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
	}
	require.Equal(t, cfg, cfg.withDefaults())

	require.False(t, NodeConfig{}.withDefaults().UseInMemoryRaftStore)
}
