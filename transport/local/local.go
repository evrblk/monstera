package local

import (
	"context"
	"fmt"
	"sync"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/transport"
)

// localNode is the subset of *monstera.Node the local transport dispatches to.
// Declaring the registry against an interface (instead of *monstera.Node) lets
// tests register a fake node that injects slow, hanging, or failing calls.
type localNode interface {
	NodeId() string
	Read(ctx context.Context, req *transport.ReadRequest) (*transport.ReadResponse, error)
	Update(ctx context.Context, req *transport.UpdateRequest) (*transport.UpdateResponse, error)
	RaftMessage(ctx context.Context, req *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error)
	TriggerSnapshot(replicaId string) error
	LeadershipTransfer(replicaId string) error
	SplitCutoff(ctx context.Context, shardId string) (uint64, error)
	ReplicaStates() []*transport.ReplicaState
	ListSnapshots(replicaId string) ([]*transport.RaftSnapshot, error)
	UpdateClusterConfig(ctx context.Context, config *cluster.Config) error
	GetClusterConfig() *cluster.Config
	Bootstrap(ctx context.Context, nodeId string, config *cluster.Config) error
}

// LocalTransport is an in-memory transport that dispatches calls directly to registered Node instances.
// It is intended for testing and local development. It implements both planes:
// data-plane calls address a node by its nodeId, and admin-plane calls address a
// node by "address" — which, in the in-memory registry, is the same key (nodes are
// registered by NodeId).
type LocalTransport struct {
	mu    sync.RWMutex
	nodes map[string]localNode
}

var _ transport.DataPlane = &LocalTransport{}
var _ transport.AdminPlane = &LocalTransport{}

func NewLocalTransport() *LocalTransport {
	return &LocalTransport{
		nodes: make(map[string]localNode),
	}
}

// Register adds a node to the transport's registry.
func (t *LocalTransport) Register(node *monstera.Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[node.NodeId()] = node
}

func (t *LocalTransport) getNode(nodeId string) (localNode, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	node, ok := t.nodes[nodeId]
	if !ok {
		return nil, fmt.Errorf("no node registered with id %s", nodeId)
	}
	return node, nil
}

// The local transport dispatches calls in-process, so without copying it would
// alias the caller's payload slices and config pointers straight into node state
// (and hand node-owned buffers straight back to callers) — whereas the gRPC
// transport always serializes, giving each side a fresh, independent copy.
// Sharing them means a mutation after a call corrupts node state under one
// transport but not the other, so tests on the local transport would not
// reproduce production behavior. These helpers reproduce gRPC's copy semantics.

// cloneBytes returns an independent copy of b (nil stays nil).
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// cloneConfig deep-copies a cluster config via a proto marshal/unmarshal round
// trip — the same transformation the gRPC transport applies on the wire.
func cloneConfig(c *cluster.Config) (*cluster.Config, error) {
	if c == nil {
		return nil, nil
	}
	data, err := c.MarshalVT()
	if err != nil {
		return nil, fmt.Errorf("cloning config: %w", err)
	}
	clone := &cluster.Config{}
	if err := clone.UnmarshalVT(data); err != nil {
		return nil, fmt.Errorf("cloning config: %w", err)
	}
	return clone, nil
}

func (t *LocalTransport) Read(ctx context.Context, nodeId string, req *transport.ReadRequest) (*transport.ReadResponse, error) {
	node, err := t.getNode(nodeId)
	if err != nil {
		return nil, err
	}

	reqCopy := *req
	reqCopy.Payload = cloneBytes(req.Payload)
	resp, err := node.Read(ctx, &reqCopy)
	if err != nil {
		return nil, err
	}
	return &transport.ReadResponse{Payload: cloneBytes(resp.Payload)}, nil
}

func (t *LocalTransport) Update(ctx context.Context, nodeId string, req *transport.UpdateRequest) (*transport.UpdateResponse, error) {
	node, err := t.getNode(nodeId)
	if err != nil {
		return nil, err
	}

	reqCopy := *req
	reqCopy.Payload = cloneBytes(req.Payload)
	resp, err := node.Update(ctx, &reqCopy)
	if err != nil {
		return nil, err
	}
	return &transport.UpdateResponse{Payload: cloneBytes(resp.Payload)}, nil
}

func (t *LocalTransport) ListReplicaStates(ctx context.Context, nodeId string) ([]*transport.ReplicaState, error) {
	node, err := t.getNode(nodeId)
	if err != nil {
		return nil, err
	}
	return node.ReplicaStates(), nil
}

func (t *LocalTransport) GetClusterConfig(ctx context.Context, address string) (*cluster.Config, error) {
	node, err := t.getNode(address)
	if err != nil {
		return nil, err
	}
	config := node.GetClusterConfig()
	if config == nil {
		return nil, fmt.Errorf("node %s is not provisioned: no cluster config", address)
	}
	// Node.GetClusterConfig already returns an independent deep copy, so unlike
	// the inbound config paths there is nothing to clone here.
	return config, nil
}

func (t *LocalTransport) UpdateClusterConfig(ctx context.Context, address string, config *cluster.Config) error {
	node, err := t.getNode(address)
	if err != nil {
		return err
	}
	// Hand the node its own copy so the caller cannot later mutate what the node
	// may retain.
	clone, err := cloneConfig(config)
	if err != nil {
		return err
	}
	return node.UpdateClusterConfig(ctx, clone)
}

func (t *LocalTransport) Bootstrap(ctx context.Context, address string, nodeId string, config *cluster.Config) error {
	node, err := t.getNode(address)
	if err != nil {
		return err
	}
	clone, err := cloneConfig(config)
	if err != nil {
		return err
	}
	return node.Bootstrap(ctx, nodeId, clone)
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

func (t *LocalTransport) SplitCutoff(ctx context.Context, address string, shardId string) error {
	node, err := t.getNode(address)
	if err != nil {
		return err
	}
	_, err = node.SplitCutoff(ctx, shardId)
	return err
}

func (t *LocalTransport) ListReplicaSnapshots(ctx context.Context, address string, replicaId string) ([]*transport.RaftSnapshot, error) {
	node, err := t.getNode(address)
	if err != nil {
		return nil, err
	}

	// Node.ListSnapshots already returns exportable transport DTOs, freshly built
	// per call (no aliasing of node state), so pass them straight through.
	return node.ListSnapshots(replicaId)
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

	reqCopy := *req
	reqCopy.Message = cloneBytes(req.Message)
	resp, err := node.RaftMessage(ctx, &reqCopy)
	if err != nil {
		return nil, err
	}
	return &transport.RaftMessageResponse{
		MessageType: resp.MessageType,
		Message:     cloneBytes(resp.Message),
	}, nil
}

func (t *LocalTransport) Close() error {
	return nil
}
