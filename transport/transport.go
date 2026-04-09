package transport

import (
	"context"
)

// Transport handles low-level communication with cluster nodes.
// Each method receives a nodeId; the implementation resolves routing internally.
type Transport interface {
	Read(ctx context.Context, nodeId string, request *ReadRequest) (*ReadResponse, error)
	Update(ctx context.Context, nodeId string, request *UpdateRequest) (*UpdateResponse, error)
	TriggerSnapshot(ctx context.Context, nodeId string, request *TriggerSnapshotRequest) error
	LeadershipTransfer(ctx context.Context, nodeId string, request *LeadershipTransferRequest) error
	HealthCheck(ctx context.Context, nodeId string) ([]*ReplicaState, error)

	RaftMessage(ctx context.Context, nodeId string, request *RaftMessageRequest) (*RaftMessageResponse, error)

	Close()
}

type ReadRequest struct {
	ApplicationName        string
	ShardId                string
	ReplicaId              string
	ShardKey               []byte
	Payload                []byte
	AllowReadFromFollowers bool
	Hops                   int32
}

type ReadResponse struct {
	Payload []byte
}

type UpdateRequest struct {
	ApplicationName string
	ShardId         string
	ReplicaId       string
	ShardKey        []byte
	Payload         []byte
	Hops            int32
}

type UpdateResponse struct {
	Payload []byte
}

type TriggerSnapshotRequest struct {
	ReplicaId string
}

type LeadershipTransferRequest struct {
	ReplicaId string
}

// ReplicaState holds the observed state of a replica.
type ReplicaState struct {
	ReplicaId string
	IsLeader  bool
}

type RaftMessageRequest struct {
	ReplicaId   string
	MessageType int32
	Message     []byte
}

type RaftMessageResponse struct {
	MessageType int32
	Message     []byte
}
