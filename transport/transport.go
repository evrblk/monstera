package transport

import (
	"context"
)

// Transport handles low-level communication with cluster nodes.
// Each method receives a nodeId; the implementation resolves routing internally.
type Transport interface {
	// Read forwards a read request to the specified node. If AllowReadFromFollowers
	// is set on the request, the node may serve the read locally without redirecting
	// to the leader.
	Read(ctx context.Context, nodeId string, request *ReadRequest) (*ReadResponse, error)

	// Update forwards a write request to the specified node. The node is expected
	// to be, or redirect to, the current Raft leader for the target shard.
	Update(ctx context.Context, nodeId string, request *UpdateRequest) (*UpdateResponse, error)

	// TriggerSnapshot instructs the specified node to initiate a Raft snapshot for
	// the given replica.
	TriggerSnapshot(ctx context.Context, nodeId string, request *TriggerSnapshotRequest) error

	// LeadershipTransfer asks the specified node to step down and transfer Raft
	// leadership for the given replica to another peer.
	LeadershipTransfer(ctx context.Context, nodeId string, request *LeadershipTransferRequest) error

	// HealthCheck returns the observed state of all replicas hosted on the
	// specified node, including which replica is currently the leader.
	HealthCheck(ctx context.Context, nodeId string) ([]*ReplicaState, error)

	// RaftMessage delivers a raw Raft protocol message to the specified node.
	RaftMessage(ctx context.Context, nodeId string, request *RaftMessageRequest) (*RaftMessageResponse, error)

	// Close releases any resources held by the transport (connections, goroutines, etc.).
	Close()
}

// ReadRequest carries the parameters for a read operation routed to a specific node.
type ReadRequest struct {
	ApplicationName string
	ShardId         string
	ReplicaId       string
	ShardKey        []byte
	// Payload is the opaque, application-defined read request body.
	Payload []byte
	// AllowReadFromFollowers permits the receiving node to serve the read without
	// forwarding to the leader, accepting potentially stale data.
	AllowReadFromFollowers bool
	// Hops tracks how many times the request has been forwarded; used to detect
	// redirect loops.
	Hops int32
}

// ReadResponse carries the opaque result of a read operation.
type ReadResponse struct {
	Payload []byte
}

// UpdateRequest carries the parameters for a write operation routed to a specific node.
type UpdateRequest struct {
	ApplicationName string
	ShardId         string
	ReplicaId       string
	ShardKey        []byte
	// Payload is the opaque, application-defined write request body.
	Payload []byte
	// Hops tracks how many times the request has been forwarded; used to detect
	// redirect loops.
	Hops int32
}

// UpdateResponse carries the opaque result of a write operation.
type UpdateResponse struct {
	Payload []byte
}

// TriggerSnapshotRequest identifies the replica for which a snapshot should be taken.
type TriggerSnapshotRequest struct {
	ReplicaId string
}

// LeadershipTransferRequest identifies the replica that should transfer leadership.
type LeadershipTransferRequest struct {
	ReplicaId string
}

// ReplicaState holds the observed state of a replica.
type ReplicaState struct {
	ReplicaId string
	IsLeader  bool
}

// RaftMessageRequest wraps a raw Raft protocol message destined for a specific replica.
type RaftMessageRequest struct {
	ReplicaId   string
	MessageType int32
	// Message is the serialized Raft message body.
	Message []byte
}

// RaftMessageResponse carries the node's reply to a Raft protocol message.
type RaftMessageResponse struct {
	MessageType int32
	// Message is the serialized Raft reply body.
	Message []byte
}
