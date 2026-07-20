package local

import (
	"context"
	"fmt"
	"sync"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/internal/raft"
	"github.com/evrblk/monstera/transport"
)

// LocalTransport is an in-memory transport that dispatches calls directly to registered Node instances.
// It is intended for testing and local development. It implements both planes:
// data-plane calls address a node by its nodeId, and admin-plane calls address a
// node by "address" — which, in the in-memory registry, is the same key (nodes are
// registered by NodeId).
type LocalTransport struct {
	mu    sync.RWMutex
	nodes map[string]*monstera.Node
}

var _ transport.DataPlane = &LocalTransport{}
var _ transport.AdminPlane = &LocalTransport{}

func NewLocalTransport() *LocalTransport {
	return &LocalTransport{
		nodes: make(map[string]*monstera.Node),
	}
}

// Register adds a node to the transport's registry.
func (t *LocalTransport) Register(node *monstera.Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[node.NodeId()] = node
}

func (t *LocalTransport) getNode(nodeId string) (*monstera.Node, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	node, ok := t.nodes[nodeId]
	if !ok {
		return nil, fmt.Errorf("no node registered with id %s", nodeId)
	}
	return node, nil
}

func (t *LocalTransport) Read(ctx context.Context, nodeId string, req *transport.ReadRequest) (*transport.ReadResponse, error) {
	node, err := t.getNode(nodeId)
	if err != nil {
		return nil, err
	}
	return node.Read(ctx, req)
}

func (t *LocalTransport) Update(ctx context.Context, nodeId string, req *transport.UpdateRequest) (*transport.UpdateResponse, error) {
	node, err := t.getNode(nodeId)
	if err != nil {
		return nil, err
	}
	return node.Update(ctx, req)
}

func (t *LocalTransport) ListReplicaStates(ctx context.Context, nodeId string) ([]*transport.ReplicaState, error) {
	node, err := t.getNode(nodeId)
	if err != nil {
		return nil, err
	}

	replicas := node.ListReplicas()
	states := make([]*transport.ReplicaState, len(replicas))
	for i, r := range replicas {
		var raftState transport.RaftState
		switch s := r.GetRaftState(); s {
		case raft.Follower:
			raftState = transport.RaftStateFollower
		case raft.Candidate:
			raftState = transport.RaftStateCandidate
		case raft.Leader:
			raftState = transport.RaftStateLeader
		case raft.Shutdown:
			raftState = transport.RaftStateDead
		default:
			return nil, fmt.Errorf("unknown raft state: %v", s)
		}

		rs := r.GetRaftStats()
		states[i] = &transport.ReplicaState{
			ReplicaId: r.GetReplicaId(),
			RaftState: raftState,
			Stats: transport.RaftStats{
				Term:              rs.Term,
				LastLogIndex:      rs.LastLogIndex,
				LastLogTerm:       rs.LastLogTerm,
				CommitIndex:       rs.CommitIndex,
				AppliedIndex:      rs.AppliedIndex,
				FSMPending:        rs.FSMPending,
				LastSnapshotIndex: rs.LastSnapshotIndex,
				LastSnapshotTerm:  rs.LastSnapshotTerm,
				NumPeers:          rs.NumPeers,
				LastContact:       rs.LastContact,
			},
		}
	}
	return states, nil
}

func (t *LocalTransport) GetClusterConfig(ctx context.Context, address string) (*cluster.Config, error) {
	node, err := t.getNode(address)
	if err != nil {
		return nil, err
	}
	return node.GetClusterConfig(), nil
}

func (t *LocalTransport) UpdateClusterConfig(ctx context.Context, address string, config *cluster.Config) error {
	node, err := t.getNode(address)
	if err != nil {
		return err
	}
	return node.UpdateClusterConfig(ctx, config)
}

func (t *LocalTransport) Bootstrap(ctx context.Context, address string, nodeId string, config *cluster.Config) error {
	node, err := t.getNode(address)
	if err != nil {
		return err
	}
	return node.Bootstrap(ctx, nodeId, config)
}

func (t *LocalTransport) TriggerSnapshot(ctx context.Context, address string, replicaId string) error {
	node, err := t.getNode(address)
	if err != nil {
		return err
	}
	return node.TriggerSnapshot(replicaId)
}

func (t *LocalTransport) LeadershipTransfer(ctx context.Context, address string, replicaId string) error {
	node, err := t.getNode(address)
	if err != nil {
		return err
	}
	return node.LeadershipTransfer(replicaId)
}

func (t *LocalTransport) ListReplicaSnapshots(ctx context.Context, address string, replicaId string) ([]*transport.RaftSnapshot, error) {
	node, err := t.getNode(address)
	if err != nil {
		return nil, err
	}

	metas, err := node.ListSnapshots(replicaId)
	if err != nil {
		return nil, err
	}

	snapshots := make([]*transport.RaftSnapshot, len(metas))
	for i, m := range metas {
		snapshots[i] = &transport.RaftSnapshot{
			Id:    m.Id,
			Index: m.Index,
			Term:  m.Term,
			Size:  m.Size,
		}
	}
	return snapshots, nil
}

func (t *LocalTransport) RaftMessage(ctx context.Context, nodeId string, req *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
	if nodeId == "" {
		return nil, fmt.Errorf("nodeId is required")
	}

	if req.ReplicaId == "" {
		return nil, fmt.Errorf("replicaId is required")
	}

	node, err := t.getNode(nodeId)
	if err != nil {
		return nil, err
	}
	return node.RaftMessage(ctx, req)
}

func (t *LocalTransport) Close() error {
	return nil
}
