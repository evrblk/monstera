package transport

import (
	"context"
	"time"

	"github.com/evrblk/monstera/cluster"
)

// DataPlane is the hot-path node-to-node link. Every call names its target by
// nodeId; the implementation resolves nodeId -> address itself (gRPC: from the
// cluster config pushed via ClusterConfigConsumer; local: from its in-memory
// registry). It carries request routing (Read/Update forwarding) and the Raft
// protocol traffic between replicas of the same shard.
//
// Required by monstera.Node (RaftMessage + read/update forwarding) and
// monstera.Client (Read, Update, ListReplicaStates). It deliberately excludes
// the control-plane calls (config, bootstrap, snapshot/leadership ops), which
// are addressed by raw address and live on AdminPlane.
type DataPlane interface {
	// Read forwards a read request to the specified node. If AllowReadFromFollowers
	// is set on the request, the node may serve the read locally without redirecting
	// to the leader.
	Read(ctx context.Context, nodeId string, req *ReadRequest) (*ReadResponse, error)

	// Update forwards a write request to the specified node. The node is expected
	// to be, or redirect to, the current Raft leader for the target shard.
	Update(ctx context.Context, nodeId string, req *UpdateRequest) (*UpdateResponse, error)

	// ListReplicaStates returns the observed state of all replicas hosted on the
	// specified node, including which replica is currently the leader. It is a
	// lightweight, frequently-polled call (no disk I/O). The Client uses it for
	// leader discovery, driven by the node list in its cluster config. (The same
	// RPC is also exposed on AdminPlane, addressed by raw address, for tooling.)
	ListReplicaStates(ctx context.Context, nodeId string) ([]*ReplicaState, error)

	// RaftMessage delivers a raw Raft protocol message to the specified node over a
	// persistent, multiplexed bidirectional stream.
	RaftMessage(ctx context.Context, nodeId string, req *RaftMessageRequest) (*RaftMessageResponse, error)

	// Close releases any resources held by the transport (connections, goroutines, etc.).
	Close() error
}

// AdminPlane is the control-plane link. Every call names its target by raw gRPC
// address, so it works with no cluster config at all — this is what lets config
// bootstrap and node provisioning proceed before any config is available (you
// cannot resolve a node's address through the very config you are fetching or
// installing).
//
// Required by monstera's polling config provider (GetClusterConfig), the admin
// CLI (Bootstrap/UpdateClusterConfig), the sequence executor (the control
// package), and debug/ops tooling. Traffic is infrequent.
type AdminPlane interface {
	// Bootstrap provisions an unprovisioned node at address: it assigns the node
	// nodeId and installs the initial cluster config. Rejected once provisioned.
	Bootstrap(ctx context.Context, address string, nodeId string, config *cluster.Config) error

	// GetClusterConfig returns the cluster config the node at address is currently
	// running with (including its version). The returned config is read-only.
	GetClusterConfig(ctx context.Context, address string) (*cluster.Config, error)

	// UpdateClusterConfig installs a new cluster config on the node at address.
	UpdateClusterConfig(ctx context.Context, address string, config *cluster.Config) error

	// ListReplicaStates returns the observed state of all replicas hosted on the
	// node at address. Same RPC as DataPlane.ListReplicaStates, addressed by raw
	// address instead of nodeId — used by the sequence executor for catch-up
	// detection and by ops tooling.
	ListReplicaStates(ctx context.Context, address string) ([]*ReplicaState, error)

	// ListReplicaSnapshots returns the snapshots stored for a single replica on the
	// node at address. It reads the replica's snapshot store from disk, so it is
	// meant for on-demand admin/ops use rather than frequent polling.
	ListReplicaSnapshots(ctx context.Context, address string, replicaId string) ([]*RaftSnapshot, error)

	// TriggerSnapshot asks the given replica on the node at address to take a Raft snapshot.
	TriggerSnapshot(ctx context.Context, address string, replicaId string) error

	// LeadershipTransfer asks the given replica on the node at address to hand off
	// Raft leadership to another replica in its group (used for graceful drain).
	LeadershipTransfer(ctx context.Context, address string, replicaId string) error

	// SplitCutoff proposes the shard-split CUTOFF command through the given
	// shard's replica on the node at address. The replica must be the shard's
	// Raft leader (the caller locates it via ListReplicaStates). Idempotent:
	// a cutoff on an already-frozen shard is a no-op success.
	SplitCutoff(ctx context.Context, address string, shardId string) error

	// Close releases any resources held by the admin client (connections, etc.).
	Close() error
}

// ClusterConfigConsumer is an optional capability of a DataPlane implementation
// that resolves node addresses from the cluster config. Its owner (a Node, or a
// Client via its config provider) calls SetClusterConfig when the applied config
// changes so the plane can dial newly added nodes and drop connections to removed
// or re-addressed ones. It is intentionally not part of DataPlane (which mirrors
// node RPCs): this is local bookkeeping, not a call to a remote node. Data planes
// that route without a config (e.g. the in-memory local transport) simply do not
// implement it.
type ClusterConfigConsumer interface {
	SetClusterConfig(config *cluster.Config)
}

// ReadRequest carries the parameters for a read operation routed to a specific node.
// The receiving node resolves the target replica itself: by ShardKey against its
// own cluster config for sharded reads (correct even if the sender's config is a
// different version), or by ShardId for direct-shard reads (empty ShardKey).
type ReadRequest struct {
	ApplicationName string
	ShardId         string
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
// The receiving node resolves the target replica itself: by ShardKey against its own
// cluster config for sharded updates, or by ShardId for direct-shard updates (empty
// ShardKey).
type UpdateRequest struct {
	ApplicationName string
	ShardId         string
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

type RaftState int32

const (
	RaftStateFollower RaftState = iota
	RaftStateCandidate
	RaftStateLeader
	RaftStateDead
	// RaftStateSeeding is reported by DORMANT replicas: replicas of an
	// ACTIVATING shard that run no Raft yet and are being seeded locally by
	// the shard-split pipeline. Their Stats are zero; seeding progress is in
	// ReplicaState.SeededIndex.
	RaftStateSeeding
)

// ReplicaState holds the observed state of a replica.
type ReplicaState struct {
	ReplicaId string
	RaftState RaftState
	Stats     RaftStats

	// Seeding is true for dormant replicas of an ACTIVATING shard (split in
	// progress). SeededIndex is the parent log index this replica's durable
	// seed has reached; compare against the parent replicas' CommitIndex to
	// observe catch-up.
	Seeding     bool
	SeededIndex uint64

	// Frozen is true once this (parent) replica applied the split CUTOFF: the
	// shard no longer accepts reads or updates and re-routes them to its
	// children.
	Frozen bool
}

// RaftStats is a transport-level snapshot of a replica's Raft progress. It is
// used to observe catch-up during reconfiguration — e.g. comparing a new
// follower's AppliedIndex against the leader's CommitIndex.
type RaftStats struct {
	Term              uint64
	LastLogIndex      uint64
	LastLogTerm       uint64
	CommitIndex       uint64
	AppliedIndex      uint64
	FSMPending        uint64
	LastSnapshotIndex uint64
	LastSnapshotTerm  uint64
	NumPeers          int
	// LastContact is the time since this replica last heard from the leader:
	// 0 on the leader itself, -1 when there has been no contact yet.
	LastContact time.Duration
}

// RaftSnapshot describes a stored Raft snapshot for a replica.
type RaftSnapshot struct {
	Id    string
	Index uint64
	Term  uint64
	Size  int64
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
