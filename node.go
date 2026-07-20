package monstera

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/internal/raft"
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

	trans transport.DataPlane

	// raftStore is a persistent store shared by all replicas to store Raft log entries.
	raftStore *store.BadgerStore

	nodeConfig NodeConfig

	// reconcilerCancel stops the background Raft-membership reconcile loop;
	// reconcilerDone is closed when that loop has exited.
	reconcilerCancel context.CancelFunc
	reconcilerDone   chan struct{}

	logger *log.Logger
}

// NodeState is the lifecycle state of a Node.
type NodeState = int

const (
	// INITIAL is the state before Start finishes; the node does not serve yet.
	INITIAL NodeState = iota
	// UNPROVISIONED means the node started without a cluster config: it serves
	// only Bootstrap (and read-only status calls), awaiting provisioning.
	UNPROVISIONED
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

	// MembershipReconcileInterval is how often a node re-checks, for each shard it
	// leads, that the Raft group membership matches the cluster config (adding or
	// removing voters as needed). The reconcile is idempotent and cheap when there
	// is nothing to do.
	MembershipReconcileInterval time.Duration
}

var DefaultMonsteraNodeConfig = NodeConfig{
	MaxHops:          5,
	MaxReadTimeout:   10 * time.Second,
	MaxUpdateTimeout: 30 * time.Second,

	UseInMemoryRaftStore: false,

	MembershipReconcileInterval: 1 * time.Second,
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
	n.setReadyMetric(false)

	// Stop the background reconcile loop before tearing down replicas so it never
	// touches a replica that is being closed.
	if n.reconcilerCancel != nil {
		n.reconcilerCancel()
		<-n.reconcilerDone
	}

	n.trans.Close()

	n.mu.Lock()
	for _, b := range n.replicas {
		b.Close()
	}
	n.mu.Unlock()

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
	// mu is released explicitly (not deferred) before starting the reconciler.
	n.mu.Lock()

	if n.clusterConfig == nil {
		// No config yet: come up UNPROVISIONED and serve only Bootstrap.
		n.mu.Unlock()
		n.nodeState = UNPROVISIONED
		n.logger.Printf("Node is unprovisioned; awaiting Bootstrap")
		return
	}

	n.logger.Printf("Starting Monstera Node. Config version: %d", n.clusterConfig.Version)

	n.logger.Printf("Loading cores...")
	if err := n.reconcileReplicasLocked(); err != nil {
		n.mu.Unlock()
		panic(err)
	}

	if err := n.bootstrapShards(); err != nil {
		n.mu.Unlock()
		panic(err)
	}

	n.logger.Printf("Node loaded %d replicas", len(n.replicas))
	config := n.clusterConfig
	n.mu.Unlock()

	// Feed the transport the config we came up with, so it can resolve peer
	// addresses for Raft traffic and request forwarding. The transport is built
	// without a config; this is the provisioned-on-restart counterpart to the
	// pushes in Bootstrap and UpdateClusterConfig.
	n.refreshTransportConfig(config)

	n.nodeState = READY
	n.setReadyMetric(true)
	n.setConfigVersionMetric(config.Version)
	n.logger.Printf("Node is ready")

	// Continuously converge Raft membership to the config for shards this node
	// leads (also picks up config changes and leadership handoffs over time).
	n.startReconciler()
}

// setReadyMetric publishes the monstera_node_ready gauge for this node (1 when
// serving, 0 otherwise). It is a no-op until the node has an id — a
// never-bootstrapped node has none to label yet.
func (n *Node) setReadyMetric(ready bool) {
	if n.nodeId == "" {
		return
	}
	v := 0.0
	if ready {
		v = 1.0
	}
	nodeReady.WithLabelValues(n.nodeId).Set(v)
}

// setConfigVersionMetric publishes the monstera_config_version_number gauge for
// this node. Like setReadyMetric it is a no-op until the node has an id.
func (n *Node) setConfigVersionMetric(version int64) {
	if n.nodeId == "" {
		return
	}
	configVersion.WithLabelValues(n.nodeId).Set(float64(version))
}

// Bootstrap provisions an UNPROVISIONED node: it assigns the node its id, installs
// the cluster config, creates the node's replicas, and transitions to READY. The
// id is assigned here (this is the only place a node's identity is set) and must
// name a node present in the config. Subsequent config changes go through
// UpdateClusterConfig.
//
// Bootstrap is idempotent with respect to identity: calling it on a node already
// provisioned as the same nodeId is a no-op success (so it is safe for an operator
// to bootstrap manually and for a control action to retry). It does not change the
// applied config in that case — the node may already be at a newer version. A
// bootstrap for a different nodeId is rejected: identity is immutable once set.
func (n *Node) Bootstrap(ctx context.Context, nodeId string, config *cluster.Config) error {
	n.smu.Lock()
	defer n.smu.Unlock()

	if n.nodeState != UNPROVISIONED {
		if nodeId != "" && nodeId != n.nodeId {
			return fmt.Errorf("node is already provisioned as %q, cannot bootstrap as %q", n.nodeId, nodeId)
		}
		n.logger.Printf("Bootstrap is a no-op: node already provisioned as %q", n.nodeId)
		return nil
	}

	if nodeId == "" {
		return fmt.Errorf("bootstrap requires a node id")
	}
	if _, err := config.GetNode(nodeId); err != nil {
		return fmt.Errorf("node %s not found in bootstrap config", nodeId)
	}
	for _, a := range config.GetApplications() {
		if _, ok := n.coreDescriptors[a.Implementation]; !ok {
			return fmt.Errorf("no core implementation registered for %s", a.Implementation)
		}
	}

	// Persist the config, then the identity (identity is the commit marker: a
	// crash between the two leaves the node unprovisioned and re-bootstrappable).
	if err := cluster.WriteConfigToFile(config, clusterConfigPath(n.baseDir)); err != nil {
		return fmt.Errorf("persisting cluster config: %w", err)
	}
	if err := writeNodeIdentity(n.baseDir, nodeId); err != nil {
		return fmt.Errorf("persisting node identity: %w", err)
	}

	n.mu.Lock()
	n.nodeId = nodeId
	// Rebuild the logger now that the node has an identity, so subsequent log lines
	// (reconcile, membership, serving) carry the node id prefix instead of the empty
	// "[]" a freshly-started unprovisioned node had. Safe to reassign here: the node
	// is not serving yet and nothing else reads n.logger while UNPROVISIONED.
	n.logger = log.New(os.Stderr, fmt.Sprintf("[%s] ", nodeId), log.LstdFlags)
	n.clusterConfig = config
	if err := n.reconcileReplicasLocked(); err != nil {
		n.mu.Unlock()
		return err
	}
	if err := n.bootstrapShards(); err != nil {
		n.mu.Unlock()
		return err
	}
	n.mu.Unlock()

	// The transport was built before this node had a config; give it the config
	// now so it can reach peers.
	n.refreshTransportConfig(config)

	n.nodeState = READY
	n.setReadyMetric(true)
	n.setConfigVersionMetric(config.Version)
	n.logger.Printf("Node bootstrapped at config version %d; ready", config.Version)

	n.startReconciler()

	return nil
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

// ListSnapshots returns the snapshots stored for the replica with the given id.
// It reads the replica's snapshot store from disk, so it is meant for on-demand
// admin/ops use rather than frequent polling.
func (n *Node) ListSnapshots(replicaId string) ([]raft.SnapshotMetadata, error) {
	if n.NodeState() != READY {
		return nil, errNodeNotReady
	}

	r, err := n.getReplica(replicaId)
	if err != nil {
		return nil, err
	}

	return r.ListSnapshots()
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

// GetClusterConfig returns the cluster config this node is currently running
// with. Callers must treat it as read-only (it is the live pointer, swapped
// wholesale by UpdateClusterConfig). Useful for inspecting the applied config
// version on each node.
func (n *Node) GetClusterConfig() *cluster.Config {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.clusterConfig
}

// UpdateClusterConfig installs a new cluster config: it persists the config,
// swaps it in, and reconciles this node's replicas and Raft group membership to
// match. This is the only place replicas and clusterConfig change after startup;
// the persist + swap + replica reconcile happen under mu so readers always
// observe a config that matches the replica map. It also refreshes the
// transport's view of the cluster so it can dial added nodes and drop
// connections to removed ones.
func (n *Node) UpdateClusterConfig(ctx context.Context, newConfig *cluster.Config) error {
	if n.NodeState() != READY {
		return errNodeNotReady
	}

	// The new config must be internally valid on its own.
	if err := newConfig.Validate(); err != nil {
		return fmt.Errorf("invalid cluster config: %w", err)
	}

	n.mu.Lock()

	// The transition from the currently applied config to the new one must be
	// safe (no shard removal, no replica reassignment, monotonic version, ...).
	// This is checked under mu against the live config so concurrent updates
	// can't race past each other.
	if err := cluster.ValidateTransition(n.clusterConfig, newConfig); err != nil {
		n.mu.Unlock()
		return fmt.Errorf("invalid cluster config transition: %w", err)
	}

	// Persist the new config durably before acknowledging the swap, so a restart
	// resumes at the applied version rather than a stale seed. The write is atomic
	// (temp + fsync + rename), so a crash never leaves a torn config.
	if err := cluster.WriteConfigToFile(newConfig, clusterConfigPath(n.baseDir)); err != nil {
		n.mu.Unlock()
		return fmt.Errorf("persisting cluster config: %w", err)
	}

	// replicas and clusterConfig must be updated together under mu so they stay a
	// matched pair.
	n.clusterConfig = newConfig

	if err := n.reconcileReplicasLocked(); err != nil {
		n.mu.Unlock()
		return err
	}
	if err := n.bootstrapShards(); err != nil {
		n.mu.Unlock()
		return err
	}
	n.mu.Unlock()

	// Refresh the transport's view of the cluster so it can dial newly added nodes
	// (and drop connections to removed ones) before membership changes trigger
	// replication to them.
	n.refreshTransportConfig(newConfig)

	// Apply any Raft membership changes for shards this node leads (add/remove
	// voters). Runs without n.mu since it makes replicated Raft calls.
	n.reconcileRaftMembership()

	n.setConfigVersionMetric(newConfig.Version)

	return nil
}

// refreshTransportConfig tells the transport about the current cluster config, if
// the transport resolves node addresses from it (see transport.ClusterConfigConsumer).
func (n *Node) refreshTransportConfig(config *cluster.Config) {
	if c, ok := n.trans.(transport.ClusterConfigConsumer); ok {
		c.SetClusterConfig(config)
	}
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

// reconcileReplicasLocked brings the replica map into agreement with the cluster
// config: it creates replicas newly assigned to this node and closes+deletes
// replicas no longer assigned here. It is idempotent (safe to run on Start and on
// every config change) and must be called with n.mu held.
//
// New replicas are NOT bootstrapped here — an added replica joins an existing
// Raft group when that group's leader adds it as a voter (see
// reconcileRaftMembership); brand-new shards are bootstrapped by bootstrapShards.
func (n *Node) reconcileReplicasLocked() error {
	clusterConfig := n.clusterConfig

	if _, err := clusterConfig.GetNode(n.nodeId); err != nil {
		return fmt.Errorf("node %s not found in cluster config", n.nodeId)
	}

	// Desired replicas assigned to this node.
	type placement struct {
		app     *cluster.Application
		shard   *cluster.Shard
		replica *cluster.Replica
	}
	desired := make(map[string]placement)
	for _, a := range clusterConfig.Applications {
		for _, s := range a.Shards {
			for _, r := range s.Replicas {
				if r.NodeId == n.nodeId {
					desired[r.Id] = placement{app: a, shard: s, replica: r}
				}
			}
		}
	}

	// Create replicas newly assigned to this node.
	for id, p := range desired {
		if _, ok := n.replicas[id]; ok {
			continue
		}
		coreDescriptor, ok := n.coreDescriptors[p.app.Implementation]
		if !ok {
			return fmt.Errorf("no core registered for %s", p.app.Implementation)
		}
		applicationCore := coreDescriptor.CoreFactoryFunc(p.shard, p.replica)
		rep := newReplica(n.baseDir, p.app.Name, p.shard.Id, id, n.nodeId, applicationCore, n.trans, n.raftStore, coreDescriptor.RestoreSnapshotOnStart, n.nodeConfig.MaxUpdateTimeout)
		n.replicas[id] = rep
		n.logger.Printf("Created replica %s (shard %s)", id, p.shard.Id)
	}

	// Close and delete replicas no longer assigned to this node. Deleting the
	// durable data is safe here (delete-last): the replica is gone from the
	// desired config, and replica ids are never reused, so a later recreate would
	// start fresh and catch up rather than read stale bytes.
	for id, rep := range n.replicas {
		if _, ok := desired[id]; ok {
			continue
		}
		n.logger.Printf("Removing replica %s (shard %s)", id, rep.shardId)
		rep.Close()
		delete(n.replicas, id)
		if err := n.raftStore.DropPrefix([]byte(id)); err != nil {
			n.logger.Printf("error dropping raft data for replica %s: %v", id, err)
		}
		if err := os.RemoveAll(filepath.Join(n.baseDir, "snapshots", id)); err != nil {
			n.logger.Printf("error removing snapshots for replica %s: %v", id, err)
		}
	}

	return nil
}

// startReconciler launches the background loop that periodically converges Raft
// group membership to the cluster config. Stop cancels it.
func (n *Node) startReconciler() {
	ctx, cancel := context.WithCancel(context.Background())
	n.reconcilerCancel = cancel
	n.reconcilerDone = make(chan struct{})

	go func() {
		defer close(n.reconcilerDone)

		ticker := time.NewTicker(n.nodeConfig.MembershipReconcileInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n.reconcileRaftMembership()
			}
		}
	}()
}

// reconcileRaftMembership brings each shard this node currently LEADS into
// agreement with the desired cluster config: it adds voters that the config lists
// but the Raft group is missing, and removes servers the group has but the config
// no longer lists. Only the leader can change membership, so this is a no-op on
// followers; it is idempotent and safe to call repeatedly and from every node.
//
// The slow Raft membership RPCs run without holding n.mu: leaders and their
// desired member sets are snapshotted under a brief read lock first.
func (n *Node) reconcileRaftMembership() {
	type task struct {
		r       *replica
		desired map[string]string // replicaId -> nodeId
	}

	n.mu.RLock()
	var tasks []task
	for _, r := range n.replicas {
		if !r.IsLeader() {
			continue
		}
		s, err := n.clusterConfig.GetShard(r.shardId)
		if err != nil {
			continue
		}
		desired := make(map[string]string, len(s.Replicas))
		for _, rep := range s.Replicas {
			desired[rep.Id] = rep.NodeId
		}
		tasks = append(tasks, task{r: r, desired: desired})
	}
	n.mu.RUnlock()

	for _, t := range tasks {
		actual, err := t.r.GetConfiguration()
		if err != nil {
			continue
		}

		actualSet := make(map[string]bool, len(actual))
		for _, s := range actual {
			actualSet[s.ReplicaId] = true
		}

		// Add voters that should be members but aren't yet.
		for id, nodeId := range t.desired {
			if !actualSet[id] {
				if err := t.r.AddVoter(id, nodeId); err != nil {
					n.logger.Printf("reconcile: AddVoter(%s) on %s: %v", id, t.r.replicaId, err)
				}
			}
		}

		// Remove servers no longer in the config. Never remove self here — a leader
		// is handed off (LeadershipTransfer) before it is removed.
		for _, s := range actual {
			if _, ok := t.desired[s.ReplicaId]; ok {
				continue
			}
			if s.ReplicaId == t.r.replicaId {
				continue
			}
			if err := t.r.RemoveServer(s.ReplicaId); err != nil {
				n.logger.Printf("reconcile: RemoveServer(%s) on %s: %v", s.ReplicaId, t.r.replicaId, err)
			}
		}
	}
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
				servers := make([]raft.RaftServer, len(s.Replicas))
				for i, r := range s.Replicas {
					servers[i] = raft.RaftServer{
						ReplicaId: r.Id,
						NodeId:    r.NodeId,
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

// NewNode creates a Node backed by baseDir. Identity and config are discovered
// from disk. config/node.json is the provisioning marker: when it is present the
// node loads its applied cluster config (config/cluster.json) and runs as that node.
// When it is absent the node comes up UNPROVISIONED and serves only Bootstrap, which
// assigns its identity and installs the initial config. It opens the shared Raft
// store (durable on disk, or in-memory when NodeConfig.UseInMemoryRaftStore is set).
// Call Start to load replicas and begin serving.
func NewNode(baseDir string, coreDescriptors ApplicationCoreDescriptors, nodeConfig NodeConfig, trans transport.DataPlane) (*Node, error) {
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

	// config/node.json is the provisioning commit marker. Its presence means the
	// node was bootstrapped: load the applied config, which must exist. Its absence
	// means unprovisioned — ignore any orphan cluster.json left by a bootstrap that
	// crashed before writing the identity; Bootstrap will rewrite both.
	persistedId, hasId, err := readNodeIdentity(baseDir)
	if err != nil {
		return nil, err
	}

	var effectiveConfig *cluster.Config
	if hasId {
		effectiveConfig, err = loadConfig(clusterConfigPath(baseDir))
		if err != nil {
			return nil, err
		}
		if effectiveConfig == nil {
			return nil, fmt.Errorf("node identity present but no cluster config at %s", clusterConfigPath(baseDir))
		}
		for _, a := range effectiveConfig.GetApplications() {
			if _, ok := coreDescriptors[a.Implementation]; !ok {
				return nil, fmt.Errorf("no core implementation registered for %s", a.Implementation)
			}
		}
	}

	node := &Node{
		baseDir:         baseDir,
		nodeId:          persistedId,
		coreDescriptors: coreDescriptors,
		clusterConfig:   effectiveConfig,
		nodeState:       INITIAL,
		replicas:        make(map[string]*replica),
		trans:           trans,
		raftStore:       raftStore,
		nodeConfig:      nodeConfig,
		logger:          log.New(os.Stderr, fmt.Sprintf("[%s] ", persistedId), log.LstdFlags),
	}

	// A provisioned node exists but is not serving yet; publish 0 so the gauge has
	// a series from process start. (An unprovisioned node has no id to label yet.)
	node.setReadyMetric(false)

	return node, nil
}

// clusterConfigPath returns the path of the node's persisted applied cluster
// config within its data dir.
func clusterConfigPath(baseDir string) string {
	return filepath.Join(baseDir, "config", "cluster.json")
}

// loadConfig returns the node's persisted cluster config, or (nil, nil) when no
// config has been persisted yet (the node is unprovisioned and awaits Bootstrap).
// A persisted config/cluster.json is authoritative: it is how a node resumes the
// config it last applied across restarts.
func loadConfig(path string) (*cluster.Config, error) {
	_, err := os.Stat(path)
	switch {
	case err == nil:
		return cluster.LoadConfigFromFile(path)
	case errors.Is(err, os.ErrNotExist):
		return nil, nil
	default:
		return nil, err
	}
}

// nodeIdentity is the node's stable local identity, persisted in config/node.json.
type nodeIdentity struct {
	NodeId  string `json:"node_id"`
	Version int    `json:"version"`
}

func nodeIdentityPath(baseDir string) string {
	return filepath.Join(baseDir, "config", "node.json")
}

// readNodeIdentity reads the persisted node id, reporting whether node.json exists.
func readNodeIdentity(baseDir string) (string, bool, error) {
	data, err := os.ReadFile(nodeIdentityPath(baseDir))
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	var id nodeIdentity
	if err := json.Unmarshal(data, &id); err != nil {
		return "", false, err
	}
	return id.NodeId, true, nil
}

// writeNodeIdentity persists the node id to config/node.json.
func writeNodeIdentity(baseDir string, nodeId string) error {
	if err := os.MkdirAll(filepath.Join(baseDir, "config"), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(nodeIdentity{NodeId: nodeId, Version: 1}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(nodeIdentityPath(baseDir), data, 0644)
}
