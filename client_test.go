package monstera

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/transport"
)

// TestNewMonsteraClientDefaultsNonPositiveConfig checks that a hand-built
// ClientConfig is usable without filling in every knob. Left at zero,
// RefreshIntervalJitter panics rand.Int64N in the background refresh goroutine
// and MaxRetriesOnSingleReplica makes every request skip its retry loop and
// return ErrAllReplicasFailed.
func TestNewMonsteraClientDefaultsNonPositiveConfig(t *testing.T) {
	c := NewMonsteraClient(NewStaticClusterConfigProvider(nil), nil, ClientConfig{})
	require.Equal(t, DefaultClientConfig(), c.config)

	// Negative values are defaulted too, not just the zero value.
	c = NewMonsteraClient(NewStaticClusterConfigProvider(nil), nil, ClientConfig{
		MaxRetriesOnSingleReplica: -1,
		RefreshIntervalJitter:     -1,
	})
	require.Equal(t, DefaultClientConfig(), c.config)
}

// TestClientConfigWithDefaultsKeepsExplicitValues makes sure defaulting only
// fills gaps and never overrides what the caller asked for.
func TestClientConfigWithDefaultsKeepsExplicitValues(t *testing.T) {
	cfg := ClientConfig{
		MaxRetriesOnSingleReplica: 1,
		ListReplicaStatesTimeout:  2,
		RefreshIntervalBase:       3,
		RefreshIntervalJitter:     4,
		ReadRetryDelay:            5,
		UpdateRetryDelay:          6,
	}
	require.Equal(t, cfg, cfg.withDefaults())
}

// TestClientRefreshLoopSurvivesZeroJitter drives the loop that used to panic.
// With no cluster config the body is skipped and the loop goes straight to the
// jitter computation, then exits on the cancelled context.
func TestClientRefreshLoopSurvivesZeroJitter(t *testing.T) {
	c := NewMonsteraClient(NewStaticClusterConfigProvider(nil), nil, ClientConfig{})
	c.refresherDone = make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.NotPanics(t, func() { c.refreshLoop(ctx) })
}

// TestClient_pruneReplicaStates verifies that leadership state for replicas no
// longer present in the current config (retired by splits/moves) is dropped,
// while state for live replicas is kept.
func TestClient_pruneReplicaStates(t *testing.T) {
	cfg := CreateEmptyClientTestConfig(t)

	c := &Client{
		replicaStates: make(map[string]*transport.ReplicaState),
	}
	// onConfig builds the router from cfg (trans is nil here, and its type
	// assertion is nil-safe).
	c.onConfig(cfg)

	// Two live replicas (present in cfg) and two stale ones (retired ids).
	for _, id := range []string{"rpl_live_1", "rpl_live_2", "rpl_stale_1", "rpl_stale_2"} {
		c.replicaStates[id] = &transport.ReplicaState{ReplicaId: id}
	}

	c.pruneReplicaStates()

	require.Contains(t, c.replicaStates, "rpl_live_1")
	require.Contains(t, c.replicaStates, "rpl_live_2")
	require.NotContains(t, c.replicaStates, "rpl_stale_1")
	require.NotContains(t, c.replicaStates, "rpl_stale_2")
	require.Len(t, c.replicaStates, 2)
}

// CreateEmptyClientTestConfig builds a minimal valid 3-node, single-shard config
// whose replica ids are rpl_live_1..3.
func CreateEmptyClientTestConfig(t *testing.T) *cluster.Config {
	t.Helper()

	c := cluster.CreateEmptyConfig()
	_, err := c.CreateNode("node_1", "localhost:9001")
	require.NoError(t, err)
	_, err = c.CreateNode("node_2", "localhost:9002")
	require.NoError(t, err)
	_, err = c.CreateNode("node_3", "localhost:9003")
	require.NoError(t, err)

	a, err := c.CreateApplication("app", "impl", 3)
	require.NoError(t, err)
	s, err := c.CreateShard(a.Name, 0x00000000, 0xffffffff, "")
	require.NoError(t, err)

	_, err = c.AddReplica(a.Name, s.Id, "rpl_live_1", "node_1")
	require.NoError(t, err)
	_, err = c.AddReplica(a.Name, s.Id, "rpl_live_2", "node_2")
	require.NoError(t, err)
	_, err = c.AddReplica(a.Name, s.Id, "rpl_live_3", "node_3")
	require.NoError(t, err)

	require.NoError(t, c.Validate())
	return c
}
