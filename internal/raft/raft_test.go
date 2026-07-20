package raft

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/transport"
)

// TestMembershipWrappers verifies Phase 2: GetConfiguration reflects the group
// membership and AddVoter grows it. A single-node group can commit an AddVoter
// under its old (single-voter) majority, so this exercises the wrappers without
// a multi-node harness. RemoveServer (which needs the new quorum) is covered by
// the multi-node integration suite.
func TestMembershipWrappers(t *testing.T) {
	raftStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	t.Cleanup(raftStore.Close)

	r := NewRaft(t.TempDir(), "node_1", "Core", "s1", "r1", nopAppCore{}, &nopTransport{}, raftStore, false, 5*time.Second)
	t.Cleanup(func() { _ = r.Close() })

	require.NoError(t, r.Bootstrap([]RaftServer{{ReplicaId: "r1", NodeId: "node_1"}}))

	// Wait until this replica has elected itself leader.
	require.Eventually(t, func() bool {
		return r.GetRaftState() == Leader
	}, 10*time.Second, 100*time.Millisecond, "single-node replica never became leader")

	// GetConfiguration reports the bootstrapped member with id + node mapped back.
	cfg, err := r.GetConfiguration()
	require.NoError(t, err)
	require.Equal(t, []RaftServer{{ReplicaId: "r1", NodeId: "node_1"}}, cfg)

	// AddVoter grows the group; the change commits under the old single-voter majority.
	require.NoError(t, r.AddVoter("r2", "node_2"))

	require.Eventually(t, func() bool {
		cfg, err := r.GetConfiguration()
		if err != nil {
			return false
		}
		return len(cfg) == 2 && containsServer(cfg, RaftServer{ReplicaId: "r2", NodeId: "node_2"})
	}, 10*time.Second, 100*time.Millisecond, "AddVoter not reflected in configuration")
}

func containsServer(servers []RaftServer, want RaftServer) bool {
	for _, s := range servers {
		if s == want {
			return true
		}
	}
	return false
}

// nopAppCore is a trivial AppCore for a raft group under test.
type nopAppCore struct{}

func (nopAppCore) Apply(request []byte) any           { return nil }
func (nopAppCore) Snapshot() AppCoreSnapshot          { return nopAppSnapshot{} }
func (nopAppCore) Restore(reader io.ReadCloser) error { return nil }

type nopAppSnapshot struct{}

func (nopAppSnapshot) Write(w io.Writer) error { return nil }
func (nopAppSnapshot) Release()                {}

// nopTransport is a transport.DataPlane stub; a single-node group performs no
// outbound peer RPCs, so its methods are never exercised.
type nopTransport struct{}

var _ transport.DataPlane = &nopTransport{}

func (*nopTransport) Read(ctx context.Context, nodeId string, req *transport.ReadRequest) (*transport.ReadResponse, error) {
	return nil, fmt.Errorf("not implemented")
}
func (*nopTransport) Update(ctx context.Context, nodeId string, req *transport.UpdateRequest) (*transport.UpdateResponse, error) {
	return nil, fmt.Errorf("not implemented")
}
func (*nopTransport) ListReplicaStates(ctx context.Context, nodeId string) ([]*transport.ReplicaState, error) {
	return nil, fmt.Errorf("not implemented")
}
func (*nopTransport) ListReplicaSnapshots(ctx context.Context, nodeId string, replicaId string) ([]*transport.RaftSnapshot, error) {
	return nil, fmt.Errorf("not implemented")
}
func (*nopTransport) GetClusterConfig(ctx context.Context, nodeId string) (*cluster.Config, error) {
	return nil, fmt.Errorf("not implemented")
}
func (*nopTransport) UpdateClusterConfig(ctx context.Context, nodeId string, config *cluster.Config) error {
	return fmt.Errorf("not implemented")
}
func (*nopTransport) Bootstrap(ctx context.Context, nodeId string, config *cluster.Config) error {
	return fmt.Errorf("not implemented")
}
func (*nopTransport) TriggerSnapshot(ctx context.Context, nodeId string, replicaId string) error {
	return fmt.Errorf("not implemented")
}
func (*nopTransport) LeadershipTransfer(ctx context.Context, nodeId string, replicaId string) error {
	return fmt.Errorf("not implemented")
}
func (*nopTransport) RaftMessage(ctx context.Context, nodeId string, req *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
	return nil, fmt.Errorf("not implemented")
}
func (*nopTransport) Close() error { return nil }
