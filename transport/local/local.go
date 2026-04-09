package local

import (
	"context"
	"fmt"
	"sync"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/transport"
)

// LocalTransport is an in-memory transport that dispatches calls directly to registered Node instances.
// It is intended for testing and local development.
type LocalTransport struct {
	mu    sync.RWMutex
	nodes map[string]*monstera.Node
}

var _ transport.Transport = &LocalTransport{}

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

func (t *LocalTransport) Read(ctx context.Context, nodeId string, request *transport.ReadRequest) (*transport.ReadResponse, error) {
	node, err := t.getNode(nodeId)
	if err != nil {
		return nil, err
	}
	return node.Read(ctx, request)
}

func (t *LocalTransport) Update(ctx context.Context, nodeId string, request *transport.UpdateRequest) (*transport.UpdateResponse, error) {
	node, err := t.getNode(nodeId)
	if err != nil {
		return nil, err
	}
	return node.Update(ctx, request)
}

func (t *LocalTransport) TriggerSnapshot(ctx context.Context, nodeId string, request *transport.TriggerSnapshotRequest) error {
	node, err := t.getNode(nodeId)
	if err != nil {
		return err
	}
	return node.TriggerSnapshot(request.ReplicaId)
}

func (t *LocalTransport) LeadershipTransfer(ctx context.Context, nodeId string, request *transport.LeadershipTransferRequest) error {
	node, err := t.getNode(nodeId)
	if err != nil {
		return err
	}
	return node.LeadershipTransfer(request.ReplicaId)
}

func (t *LocalTransport) HealthCheck(ctx context.Context, nodeId string) ([]*transport.ReplicaState, error) {
	node, err := t.getNode(nodeId)
	if err != nil {
		return nil, err
	}

	replicas := node.ListReplicas()
	states := make([]*transport.ReplicaState, len(replicas))
	for i, r := range replicas {
		states[i] = &transport.ReplicaState{
			ReplicaId: r.GetReplicaId(),
			IsLeader:  r.IsLeader(),
		}
	}
	return states, nil
}

func (t *LocalTransport) RaftMessage(ctx context.Context, nodeId string, request *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
	node, err := t.getNode(nodeId)
	if err != nil {
		return nil, err
	}
	return node.RaftMessage(ctx, request)
}

func (t *LocalTransport) Close() {}
