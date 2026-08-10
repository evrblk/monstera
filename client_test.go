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
		MaxReadPayloadBytes:       7,
		MaxUpdatePayloadBytes:     8,
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

// recordingDataPlane is a no-op transport.DataPlane that counts Read/Update
// calls, so a test can assert whether a request reached the transport.
type recordingDataPlane struct {
	reads   int
	updates int
}

func (r *recordingDataPlane) Read(ctx context.Context, nodeId string, req *transport.ReadRequest) (*transport.ReadResponse, error) {
	r.reads++
	return &transport.ReadResponse{Payload: []byte("ok")}, nil
}

func (r *recordingDataPlane) Update(ctx context.Context, nodeId string, req *transport.UpdateRequest) (*transport.UpdateResponse, error) {
	r.updates++
	return &transport.UpdateResponse{Payload: []byte("ok")}, nil
}

func (r *recordingDataPlane) ListReplicaStates(ctx context.Context, nodeId string) ([]*transport.ReplicaState, error) {
	return nil, nil
}

func (r *recordingDataPlane) RaftMessage(ctx context.Context, nodeId string, req *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
	return nil, nil
}

func (r *recordingDataPlane) Close() error { return nil }

// TestClientPayloadSizeLimit checks that oversized Read/Update payloads are
// rejected on the client with ErrPayloadTooLarge before any node is contacted,
// while within-limit and unlimited requests reach the transport.
func TestClientPayloadSizeLimit(t *testing.T) {
	cfg := CreateEmptyClientTestConfig(t)

	newClient := func(rc ClientConfig) (*Client, *recordingDataPlane) {
		rc.MaxRetriesOnSingleReplica = 1 // so a passing request actually calls the transport
		fake := &recordingDataPlane{}
		c := &Client{
			config:        rc,
			trans:         fake,
			replicaStates: make(map[string]*transport.ReplicaState),
		}
		c.onConfig(cfg)
		return c, fake
	}

	ctx := context.Background()

	t.Run("update over limit rejected before transport", func(t *testing.T) {
		c, fake := newClient(ClientConfig{MaxUpdatePayloadBytes: 10})
		_, err := c.Update(ctx, "app", 0, make([]byte, 11))
		require.ErrorIs(t, err, ErrPayloadTooLarge)
		require.Zero(t, fake.updates, "transport must not be reached")
	})

	t.Run("update at limit reaches transport", func(t *testing.T) {
		c, fake := newClient(ClientConfig{MaxUpdatePayloadBytes: 10})
		_, err := c.Update(ctx, "app", 0, make([]byte, 10))
		require.NoError(t, err)
		require.Positive(t, fake.updates)
	})

	t.Run("read over limit rejected before transport", func(t *testing.T) {
		c, fake := newClient(ClientConfig{MaxReadPayloadBytes: 8})
		_, err := c.Read(ctx, "app", 0, true, make([]byte, 9))
		require.ErrorIs(t, err, ErrPayloadTooLarge)
		require.Zero(t, fake.reads, "transport must not be reached")
	})

	t.Run("zero limit falls back to 1 MiB default", func(t *testing.T) {
		c, fake := newClient(ClientConfig{}) // size limits unset -> default 1 MiB

		_, err := c.Update(ctx, "app", 0, make([]byte, (1<<20)+1))
		require.ErrorIs(t, err, ErrPayloadTooLarge)
		require.Zero(t, fake.updates)

		_, err = c.Update(ctx, "app", 0, make([]byte, 1<<20))
		require.NoError(t, err)
		require.Positive(t, fake.updates)

		_, err = c.Read(ctx, "app", 0, true, make([]byte, (1<<20)+1))
		require.ErrorIs(t, err, ErrPayloadTooLarge)
		require.Zero(t, fake.reads)

		_, err = c.Read(ctx, "app", 0, true, make([]byte, 1<<20))
		require.NoError(t, err)
		require.Positive(t, fake.reads)
	})

	t.Run("enforced on shard-id variants too", func(t *testing.T) {
		c, fake := newClient(ClientConfig{MaxReadPayloadBytes: 4, MaxUpdatePayloadBytes: 4})
		shard, err := c.currentRouter().FindShardByShardKey("app", 0)
		require.NoError(t, err)

		_, err = c.UpdateShard(ctx, "app", shard.Id, make([]byte, 5))
		require.ErrorIs(t, err, ErrPayloadTooLarge)
		_, err = c.ReadShard(ctx, "app", shard.Id, true, make([]byte, 5))
		require.ErrorIs(t, err, ErrPayloadTooLarge)
		require.Zero(t, fake.updates)
		require.Zero(t, fake.reads)
	})
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
