package monstera

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/transport"
)

var (
	errNodeNotReady = errors.New("node is not in READY state")
	// errLeaderUnknown is returned when the leader for a shard cannot be resolved
	// (no leader elected yet, or the forwarding hop budget was exhausted). The
	// Monstera client treats it as retryable by matching on this message string,
	// so keep the two in sync.
	errLeaderUnknown = errors.New("leader is unknown")
)

// Node is a single Monstera server process. It hosts the shard replicas assigned
// to it by the cluster config, exposes Read/Update entry points that route to the
// replica owning a shard (forwarding to the Raft leader when necessary), and
// carries Raft traffic between replicas of the same shard. A Node moves through
// the INITIAL -> READY -> STOPPED lifecycle (see NodeState).
type Node struct {
	baseDir         string
	nodeId          string
	coreDescriptors ApplicationCoreDescriptors

	// mu protects replicas and clusterConfig together. They are a matched pair:
	// every replica in the map corresponds to a replica assigned to this node in
	// clusterConfig (a replica may be inactive or still initializing, but it
	// exists in the map). They change only on config reload. Readers hold RLock
	// just long enough to snapshot the pointers they need, never for the duration
	// of a read/update, so concurrent reads and updates are not serialized.
	mu            sync.RWMutex
	replicas      map[string]*replica
	clusterConfig *cluster.Config

	smu       sync.Mutex
	nodeState NodeState

	trans transport.Transport

	// raftStore is a persistent store shared by all replicas to store Raft log entries.
	raftStore *store.BadgerStore

	nodeConfig NodeConfig

	logger *log.Logger
}

// NodeState is the lifecycle state of a Node.
type NodeState = int

const (
	// INITIAL is the state before Start finishes; the node does not serve yet.
	INITIAL NodeState = iota
	// READY means replicas are loaded and the node serves reads and updates.
	READY
	// STOPPED means the node has been shut down and rejects further requests.
	STOPPED
)

type NodeConfig struct {
	// MaxHops bounds how many times a read/update may be forwarded between nodes
	// while chasing the current leader before giving up with errLeaderUnknown.
	MaxHops int32

	// MaxReadTimeout bounds the total time a Node.Read may take, including leader
	// discovery and forwarding to the leader.
	MaxReadTimeout time.Duration

	// MaxUpdateTimeout bounds the total time a Node.Update may take. It is also
	// the timeout passed to Raft when applying a committed log entry.
	MaxUpdateTimeout time.Duration

	// UseInMemoryRaftStore set to `true` should be used only in unit tests or dev
	// environment and is not recommended for production use, since in-memory Raft
	// store is not durable.
	UseInMemoryRaftStore bool
}

var DefaultMonsteraNodeConfig = NodeConfig{
	MaxHops:          5,
	MaxReadTimeout:   10 * time.Second,
	MaxUpdateTimeout: 30 * time.Second,

	UseInMemoryRaftStore: false,
}

// Stop shuts the node down: it stops serving, closes the transport and every
// hosted replica, and closes the shared Raft store. It is safe to call more than
// once.
func (n *Node) Stop() {
	n.smu.Lock()
	defer n.smu.Unlock()

	if n.nodeState == STOPPED {
		n.logger.Printf("Monstera Node already stopped")
		return
	}

	n.logger.Printf("Stopping Monstera Node")

	n.nodeState = STOPPED

	n.trans.Close()

	for _, b := range n.replicas {
		b.Close()
	}

	n.logger.Printf("Monstera Node stopped")

	n.raftStore.Close()
}

// Start loads the replicas assigned to this node from the cluster config,
// bootstraps their Raft groups where needed, and marks the node READY. It panics
// if loading or bootstrapping fails: the node cannot serve without its replicas.
func (n *Node) Start() {
	n.smu.Lock()
	defer n.smu.Unlock()

	// Populate replicas from clusterConfig under mu so the pair stays consistent
	// even if a config reload races the initial load. There is no reader
	// contention here: reads/updates bail out on the non-READY state first.
	n.mu.Lock()
	defer n.mu.Unlock()

	n.logger.Printf("Starting Monstera Node. Config version: %d", n.clusterConfig.Version)

	n.logger.Printf("Loading cores...")
	err := n.loadCores()
	if err != nil {
		panic(err)
	}

	err = n.bootstrapShards()
	if err != nil {
		panic(err)
	}

	n.nodeState = READY

	n.logger.Printf("Node loaded %d replicas", len(n.replicas))
	n.logger.Printf("Node is ready")
}

// Read serves a read for the shard that owns req. When follower reads are allowed
// it is served from the local replica directly (possibly stale); otherwise it is
// served locally only if this replica is the Raft leader, and forwarded to the
// leader's node otherwise. It returns errLeaderUnknown once the forwarding hop
// budget (MaxHops) is exhausted.
func (n *Node) Read(ctx context.Context, req *transport.ReadRequest) (*transport.ReadResponse, error) {
	if n.NodeState() != READY {
		return nil, errNodeNotReady
	}

	ctx, cancel := context.WithTimeout(ctx, n.nodeConfig.MaxReadTimeout)
	defer cancel()

	r, clusterConfig, err := n.replicaForShard(req.ApplicationName, req.ShardId, req.ShardKey)
	if err != nil {
		return nil, err
	}

	// Follower reads accept stale data, so any replica (including this one) can serve.
	if req.AllowReadFromFollowers {
		resp, err := r.Read(req.Payload)
		if err != nil {
			return nil, err
		}
		return &transport.ReadResponse{
			Payload: resp.Data,
		}, nil
	}

	// Otherwise only the leader may serve, to avoid returning stale data.
	if r.IsLeader() {
		resp, err := r.Read(req.Payload)
		if err != nil {
			return nil, err
		}
		return &transport.ReadResponse{
			Payload: resp.Data,
		}, nil
	}

	// This replica is a follower: forward to the leader, unless the request has
	// already been forwarded too many times (guards against redirect loops).
	if req.Hops >= n.nodeConfig.MaxHops {
		return nil, errLeaderUnknown
	}

	leaderReplicaId, err := r.GetRaftLeader(ctx)
	if err != nil {
		return nil, errLeaderUnknown
	}

	leaderReplica, err := clusterConfig.GetReplica(leaderReplicaId)
	if err != nil {
		return nil, errLeaderUnknown
	}

	// Forward to the leader's node. Pin the target by shard id (the leader hosts
	// this exact shard's replica) and drop the shard key so the receiving node
	// does not re-resolve it against a possibly different config version.
	forward := &transport.ReadRequest{
		ApplicationName:        req.ApplicationName,
		ShardId:                r.shardId,
		Payload:                req.Payload,
		AllowReadFromFollowers: req.AllowReadFromFollowers,
		Hops:                   req.Hops + 1,
	}

	resp, err := n.trans.Read(ctx, leaderReplica.NodeId, forward)
	// If the leader we forwarded to was unreachable it likely just failed; wait
	// for a new election (excluding the old leader) and retry once against it.
	if err != nil && isUnavailableError(err) {
		newLeaderReplicaId, waitErr := r.WaitForNewLeader(ctx, leaderReplicaId)
		if waitErr != nil {
			return nil, errLeaderUnknown
		}
		newLeaderReplica, clusterErr := clusterConfig.GetReplica(newLeaderReplicaId)
		if clusterErr != nil {
			return nil, errLeaderUnknown
		}
		return n.trans.Read(ctx, newLeaderReplica.NodeId, forward)
	}
	return resp, err
}

// Update applies a write to the shard that owns req. Writes must go through the
// Raft leader: if this replica is the leader the write is applied (and replicated)
// locally, otherwise the request is forwarded to the leader's node. It returns
// errLeaderUnknown once the forwarding hop budget (MaxHops) is exhausted.
func (n *Node) Update(ctx context.Context, req *transport.UpdateRequest) (*transport.UpdateResponse, error) {
	if n.NodeState() != READY {
		return nil, errNodeNotReady
	}

	ctx, cancel := context.WithTimeout(ctx, n.nodeConfig.MaxUpdateTimeout)
	defer cancel()

	r, clusterConfig, err := n.replicaForShard(req.ApplicationName, req.ShardId, req.ShardKey)
	if err != nil {
		return nil, err
	}

	// Writes are applied only on the leader.
	if r.IsLeader() {
		resp, err := r.Update(req.Payload)
		if err != nil {
			return nil, err
		}
		return &transport.UpdateResponse{
			Payload: resp.Data,
		}, nil
	}

	// This replica is a follower: forward to the leader, unless the request has
	// already been forwarded too many times (guards against redirect loops).
	if req.Hops >= n.nodeConfig.MaxHops {
		return nil, errLeaderUnknown
	}

	leaderReplicaId, err := r.GetRaftLeader(ctx)
	if err != nil {
		return nil, errLeaderUnknown
	}

	leaderReplica, err := clusterConfig.GetReplica(leaderReplicaId)
	if err != nil {
		return nil, errLeaderUnknown
	}

	// Forward to the leader's node. Pin the target by shard id (the leader hosts
	// this exact shard's replica) and drop the shard key so the receiving node
	// does not re-resolve it against a possibly different config version.
	forward := &transport.UpdateRequest{
		ApplicationName: req.ApplicationName,
		ShardId:         r.shardId,
		Payload:         req.Payload,
		Hops:            req.Hops + 1,
	}

	resp, err := n.trans.Update(ctx, leaderReplica.NodeId, forward)
	// If the leader we forwarded to was unreachable it likely just failed; wait
	// for a new election (excluding the old leader) and retry once against it.
	if err != nil && isUnavailableError(err) {
		newLeaderReplicaId, waitErr := r.WaitForNewLeader(ctx, leaderReplicaId)
		if waitErr != nil {
			return nil, errLeaderUnknown
		}
		newLeaderReplica, clusterErr := clusterConfig.GetReplica(newLeaderReplicaId)
		if clusterErr != nil {
			return nil, errLeaderUnknown
		}
		return n.trans.Update(ctx, newLeaderReplica.NodeId, forward)
	}
	return resp, err
}

// TriggerSnapshot asks the replica with the given id to take a Raft snapshot.
func (n *Node) TriggerSnapshot(replicaId string) error {
	if n.NodeState() != READY {
		return errNodeNotReady
	}

	r, err := n.getReplica(replicaId)
	if err != nil {
		return err
	}

	r.TriggerSnapshot()

	return nil
}

// LeadershipTransfer asks the replica with the given id to hand off Raft
// leadership to another replica in its group (used for graceful node drain).
func (n *Node) LeadershipTransfer(replicaId string) error {
	if n.NodeState() != READY {
		return errNodeNotReady
	}

	r, err := n.getReplica(replicaId)
	if err != nil {
		return err
	}

	return r.LeadershipTransfer()
}

// RaftMessage delivers a raw Raft protocol message to the target replica hosted
// on this node. It is the receiving end of the Raft transport between replicas of
// the same shard.
func (n *Node) RaftMessage(ctx context.Context, req *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
	if n.NodeState() != READY {
		return nil, errNodeNotReady
	}

	r, err := n.getReplica(req.ReplicaId)
	if err != nil {
		return nil, err
	}

	return r.RaftMessage(req)
}

// ListReplicas returns a snapshot of the replicas currently hosted on this node.
func (n *Node) ListReplicas() []*replica {
	n.mu.RLock()
	defer n.mu.RUnlock()

	result := make([]*replica, 0, len(n.replicas))
	for _, r := range n.replicas {
		result = append(result, r)
	}

	return result
}

// NodeState returns the node's current lifecycle state.
func (n *Node) NodeState() NodeState {
	n.smu.Lock()
	defer n.smu.Unlock()

	return n.nodeState
}

// NodeId returns this node's id in the cluster config.
func (n *Node) NodeId() string {
	return n.nodeId
}

// UpdateClusterConfig installs a new cluster config. This is the only place
// replicas and clusterConfig change after startup; the swap happens under mu so
// readers always observe a config that matches the replica map.
//
// TODO: it does not yet create or close replicas for shards added or removed by
// the new config (replicasToAdd/replicasToRemove are computed but not applied),
// nor evict pooled connections for nodes that were removed.
func (n *Node) UpdateClusterConfig(ctx context.Context, newConfig *cluster.Config) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// TODO: remove connections from pool when nodes are removed

	replicasToAdd := make(map[string]bool)
	replicasToRemove := make(map[string]bool)

	for _, nr := range n.replicas {
		replicasToRemove[nr.replicaId] = true
		for _, a := range newConfig.Applications {
			for _, s := range a.Shards {
				for _, r := range s.Replicas {
					if r.NodeId == n.nodeId && r.Id == nr.replicaId {
						delete(replicasToRemove, nr.replicaId)
						goto found
					}
				}
			}
		}
	found:
	}

	for _, a := range newConfig.Applications {
		for _, s := range a.Shards {
			for _, r := range s.Replicas {
				if r.NodeId == n.nodeId {
					_, ok := n.replicas[r.Id]
					if !ok {
						replicasToAdd[r.Id] = true
					}
				}
			}
		}
	}

	// replicas and clusterConfig must be updated together under mu so they stay a
	// matched pair. The replica add/remove implied by replicasToAdd/replicasToRemove
	// is not done yet (see TODO), but the config swap already holds the lock.
	n.clusterConfig = newConfig
	// TODO implement config loading

	return nil
}

// getReplica looks up a replica hosted on this node by its replica id. It is used
// by the Raft and admin paths (RaftMessage, TriggerSnapshot, LeadershipTransfer)
// that address a specific replica; the read/update path uses replicaForShard.
func (n *Node) getReplica(replicaId string) (*replica, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	r, ok := n.replicas[replicaId]
	if !ok {
		return nil, fmt.Errorf("no replica %s found on this node %s", replicaId, n.nodeId)
	}
	return r, nil
}

// replicaForShard resolves the local replica that owns the request together with
// the cluster config used to resolve it, as a consistent snapshot taken under a
// single read lock. Because replicas and clusterConfig are only ever mutated
// together (on config reload), the returned pair is guaranteed to match, and the
// lock is released before the caller performs the actual read/update so it never
// serializes them.
//
// For sharded requests (shardKey is non-empty) the owning shard is resolved from
// shardKey against this node's own config, so routing is correct even when the
// caller's config is a different version (e.g. mid-split or during a rolling
// config rollout). For direct-shard requests (empty shardKey) the shard is taken
// from shardId as-is. A node hosts at most one replica per shard, so the shard
// determines the replica uniquely.
func (n *Node) replicaForShard(applicationName string, shardId string, shardKey []byte) (*replica, *cluster.Config, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	clusterConfig := n.clusterConfig

	targetShardId := shardId
	if len(shardKey) > 0 {
		shard, err := clusterConfig.FindShardByShardKey(applicationName, shardKey)
		if err != nil {
			return nil, nil, err
		}
		targetShardId = shard.Id
	}

	for _, r := range n.replicas {
		if r.shardId == targetShardId {
			return r, clusterConfig, nil
		}
	}

	return nil, nil, fmt.Errorf("no replica for shard %s (application %s) found on this node %s", targetShardId, applicationName, n.nodeId)
}

// loadCores populates the replicas map from clusterConfig. It must be called
// with n.mu held (see Start).
func (n *Node) loadCores() error {
	clusterConfig := n.clusterConfig

	found := false
	for _, nd := range clusterConfig.Nodes {
		if nd.Id == n.nodeId {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("node not found in cluster config")
	}

	for _, a := range clusterConfig.Applications {
		for _, s := range a.Shards {
			for _, r := range s.Replicas {
				if r.NodeId == n.nodeId {
					// Find core descriptor
					coreDescriptor, ok := n.coreDescriptors[a.Implementation]
					if !ok {
						return fmt.Errorf("no core registered for %s", a.Implementation)
					}

					// Create replica
					applicationCore := coreDescriptor.CoreFactoryFunc(s, r)
					replica := newReplica(n.baseDir, a.Name, s.Id, r.Id, n.nodeId, applicationCore, n.trans, n.raftStore, coreDescriptor.RestoreSnapshotOnStart, n.nodeConfig.MaxUpdateTimeout)

					n.replicas[r.Id] = replica
				}
			}
		}
	}

	return nil
}

// bootstrapShards bootstraps the Raft groups for replicas owned by this node. It
// must be called with n.mu held (see Start).
func (n *Node) bootstrapShards() error {
	clusterConfig := n.clusterConfig

	for _, r := range n.replicas {
		s, err := clusterConfig.GetShard(r.shardId)
		if err != nil {
			return err
		}

		// A Raft group must be bootstrapped exactly once, so only the shard's first
		// replica does it (a deterministic choice every node agrees on); the other
		// replicas join once they receive Raft messages from the leader.
		if s.Replicas[0].NodeId == n.nodeId {
			// Skip if this replica already has persisted Raft state.
			if !r.IsBootstrapped() {
				// The bootstrap configuration is the full set of replicas in the shard.
				// The Raft server address is the node id; the transport resolves it to
				// an actual network address via the cluster config.
				servers := make([]hraft.Server, len(s.Replicas))
				for i, r := range s.Replicas {
					for _, nd := range clusterConfig.Nodes {
						if nd.Id == r.NodeId {
							servers[i] = hraft.Server{
								Suffrage: hraft.Voter,
								ID:       hraft.ServerID(r.Id),
								Address:  hraft.ServerAddress(nd.Id),
							}
							break
						}
					}
				}
				r.Bootstrap(servers)
			}
		}
	}

	return nil
}

// isUnavailableError reports whether err is a gRPC "unavailable" status, i.e. the
// forwarded-to node could not be reached and forwarding should wait for a new
// leader before retrying.
func isUnavailableError(err error) bool {
	if st, ok := status.FromError(err); ok {
		return st.Code() == codes.Unavailable
	}
	return false
}

// NewNode creates a Node for nodeId from the given cluster config and the core
// implementations registered in coreDescriptors. It opens the shared Raft store
// (durable on disk, or in-memory when NodeConfig.UseInMemoryRaftStore is set) and
// verifies that every application in the config has a registered core
// implementation. Call Start to load replicas and begin serving.
func NewNode(baseDir string, nodeId string, clusterConfig *cluster.Config, coreDescriptors ApplicationCoreDescriptors, nodeConfig NodeConfig, trans transport.Transport) (*Node, error) {
	var raftStore *store.BadgerStore
	var err error
	if nodeConfig.UseInMemoryRaftStore {
		raftStore, err = store.NewBadgerInMemoryStore()
	} else {
		raftStore, err = store.NewBadgerStore(store.DefaultOptions(filepath.Join(baseDir, "raft")).WithSyncWrites(true))
	}
	if err != nil {
		return nil, err
	}

	for _, a := range clusterConfig.GetApplications() {
		if _, ok := coreDescriptors[a.Implementation]; !ok {
			return nil, fmt.Errorf("no core implementation registered for %s", a.Implementation)
		}
	}

	node := &Node{
		baseDir:         baseDir,
		nodeId:          nodeId,
		coreDescriptors: coreDescriptors,
		clusterConfig:   clusterConfig,
		nodeState:       INITIAL,
		replicas:        make(map[string]*replica),
		trans:           trans,
		raftStore:       raftStore,
		nodeConfig:      nodeConfig,
		logger:          log.New(os.Stderr, fmt.Sprintf("[%s] ", nodeId), log.LstdFlags),
	}

	return node, nil
}
