package monstera

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/transport"
)

// nopSplitCore is a trivial ApplicationCore for split-lifecycle tests: it holds
// no state and every operation is a no-op.
type nopSplitCore struct{}

func (nopSplitCore) Read(req []byte, log *slog.Logger) (*ReadResponse, error) {
	return &ReadResponse{}, nil
}
func (nopSplitCore) Update(req []byte, log *slog.Logger) (*UpdateResponse, error) {
	return &UpdateResponse{}, nil
}
func (nopSplitCore) Snapshot() ApplicationCoreSnapshot { return nopSplitSnapshot{} }
func (nopSplitCore) Restore(readers ...io.ReadCloser) error {
	for _, r := range readers {
		_ = r.Close()
	}
	return nil
}
func (nopSplitCore) Close() {}

type nopSplitSnapshot struct{}

func (nopSplitSnapshot) Write(w io.Writer) error { return nil }
func (nopSplitSnapshot) Release()                {}

// errTransport is a transport.DataPlane whose peer RPCs all fail, so a single
// bootstrapped node never reaches quorum but also never panics dialing peers.
type errTransport struct{}

var _ transport.DataPlane = errTransport{}

func (errTransport) Read(ctx context.Context, nodeId string, req *transport.ReadRequest) (*transport.ReadResponse, error) {
	return nil, errors.New("errTransport")
}
func (errTransport) Update(ctx context.Context, nodeId string, req *transport.UpdateRequest) (*transport.UpdateResponse, error) {
	return nil, errors.New("errTransport")
}
func (errTransport) ListReplicaStates(ctx context.Context, nodeId string) ([]*transport.ReplicaState, error) {
	return nil, errors.New("errTransport")
}
func (errTransport) RaftMessage(ctx context.Context, nodeId string, req *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
	return nil, errors.New("errTransport")
}
func (errTransport) Close() error { return nil }

// midSplitConfig builds a valid config with the "p" shard SPLITTING into two
// co-located ACTIVATING children ("c1", "c2") across three nodes.
func midSplitConfig(version int64) *cluster.Config {
	replicasFor := func(shardId string) []*cluster.Replica {
		rs := make([]*cluster.Replica, 3)
		for i := 0; i < 3; i++ {
			rs[i] = &cluster.Replica{Id: fmt.Sprintf("%s_r%d", shardId, i+1), NodeId: fmt.Sprintf("node_%d", i+1)}
		}
		return rs
	}
	return &cluster.Config{
		Version: version,
		Applications: []*cluster.Application{{
			Name:              "Core",
			Implementation:    "Core",
			ReplicationFactor: 3,
			Shards: []*cluster.Shard{
				{Id: "p", LowerBound: 0x00000000, UpperBound: 0xffffffff, State: cluster.ShardState_SHARD_STATE_SPLITTING, Replicas: replicasFor("p")},
				{Id: "c1", LowerBound: 0x00000000, UpperBound: 0x7fffffff, State: cluster.ShardState_SHARD_STATE_ACTIVATING, ParentId: "p", Replicas: replicasFor("c1")},
				{Id: "c2", LowerBound: 0x80000000, UpperBound: 0xffffffff, State: cluster.ShardState_SHARD_STATE_ACTIVATING, ParentId: "p", Replicas: replicasFor("c2")},
			},
		}},
		Nodes: []*cluster.Node{
			{Id: "node_1", GrpcAddress: "addr1"},
			{Id: "node_2", GrpcAddress: "addr2"},
			{Id: "node_3", GrpcAddress: "addr3"},
		},
	}
}

func splitterFor(n *Node, replicaId string) *splitter {
	n.splittersMu.Lock()
	defer n.splittersMu.Unlock()
	return n.splitters[replicaId]
}

// TestUpdateClusterConfigRestartsSplittersOnPersistFailure is the regression test
// for M6 (the UpdateClusterConfig half): a config apply that fails after
// stopSplitters — here an injected persist failure — must still restart split
// seeding. Without the fix the splitters stay stopped until the next successful
// apply or a restart.
func TestUpdateClusterConfigRestartsSplittersOnPersistFailure(t *testing.T) {
	descriptors := ApplicationCoreDescriptors{
		"Core": {
			CoreType:        CoreTypeInMemory,
			CoreFactoryFunc: func(*cluster.Shard, *cluster.Replica) ApplicationCore { return nopSplitCore{} },
		},
	}

	n, err := NewNode(t.TempDir(), descriptors, NodeConfig{UseInMemoryRaftStore: true}, errTransport{})
	require.NoError(t, err)
	t.Cleanup(n.Stop)

	// Fresh data dir + no config: Start comes up UNPROVISIONED, then Bootstrap
	// provisions the node with the mid-split config.
	n.Start()
	require.Equal(t, NodeStateUnprovisioned, n.NodeState())

	cfg := midSplitConfig(1)
	require.NoError(t, cfg.Validate())
	require.NoError(t, n.Bootstrap(context.Background(), "node_1", cfg))

	// Bootstrap started the seeding pipeline for the parent replica this node hosts.
	before := splitterFor(n, "p_r1")
	require.NotNil(t, before, "bootstrap should have started the parent's splitter")

	// Inject a persist failure so UpdateClusterConfig aborts right after it has
	// already stopped the splitters.
	n.persistConfig = func(*cluster.Config) error { return errors.New("disk full") }

	err = n.UpdateClusterConfig(context.Background(), midSplitConfig(2))
	require.Error(t, err)
	require.Contains(t, err.Error(), "persisting cluster config")

	// The failed apply must not leave seeding halted: startSplitters ran on the
	// error path, producing a fresh splitter instance (stopSplitters had closed
	// the previous one).
	after := splitterFor(n, "p_r1")
	require.NotNil(t, after, "splitters must be restarted after a persist failure")
	require.NotSame(t, before, after, "restart should create a new splitter instance")
}
