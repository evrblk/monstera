package monstera

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
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
	// errShardFrozen is returned when a request reaches a shard frozen by a
	// split cutoff. Node-side re-routing matches on this message string when
	// the error comes back over the transport from a forwarded request, so
	// keep the two in sync.
	errShardFrozen = errors.New("shard is frozen by a split cutoff")
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

	// mu protects replicas, clusterConfig and router together. They are a matched
	// set: every replica in the map corresponds to a replica assigned to this node
	// in clusterConfig (a replica may be inactive or still initializing, but it
	// exists in the map), and router is the index built from clusterConfig. They
	// change only on config reload. Readers hold RLock just long enough to
	// snapshot the pointers they need, never for the duration of a read/update, so
	// concurrent reads and updates are not serialized.
	mu            sync.RWMutex
	replicas      map[string]*replica
	dormant       map[string]*dormantReplica
	clusterConfig *cluster.Config
	// router is the routing index built from clusterConfig; the two are always
	// swapped together via setClusterConfigLocked.
	router *Router

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

	// splittersMu guards splitters: the running shard-split seeding pipelines,
	// keyed by parent replica id. Splitters are stopped before every replica
	// reconcile and (re)started from the applied config after it; they resume
	// from durable progress, so the churn is cheap.
	splittersMu sync.Mutex
	splitters   map[string]*splitter

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

	// Stop split seeding pipelines before closing the parent replicas they read.
	n.stopSplitters()

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

	// Start split seeding pipelines for any splitting shards in the applied
	// config (a node restart mid-split resumes from durable progress).
	n.startSplitters()
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
	n.setClusterConfigLocked(config)
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

	r, router, err := n.replicaForShard(req.ApplicationName, req.ShardId, req.ShardKey)
	if err != nil {
		return nil, err
	}

	// A shard frozen by a split cutoff serves nothing anymore: its children own
	// the range. Re-route by shard key to the local child (co-location
	// guarantees it is here). Key-less (forwarded) requests propagate the
	// typed error back to the origin node, which has the key.
	if r.frozenAt() > 0 {
		return n.rerouteRead(ctx, req, r.shardId)
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

	leaderReplica, err := router.GetReplica(leaderReplicaId)
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
	// The leader may have frozen the shard (split cutoff) before our forward
	// arrived; we still hold the shard key, so re-route to the child here.
	if err != nil && isShardFrozenError(err) {
		return n.rerouteRead(ctx, req, r.shardId)
	}
	// If the leader we forwarded to was unreachable it likely just failed; wait
	// for a new election (excluding the old leader) and retry once against it.
	if err != nil && isUnavailableError(err) {
		newLeaderReplicaId, waitErr := r.WaitForNewLeader(ctx, leaderReplicaId)
		if waitErr != nil {
			return nil, errLeaderUnknown
		}
		newLeaderReplica, clusterErr := router.GetReplica(newLeaderReplicaId)
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

	r, router, err := n.replicaForShard(req.ApplicationName, req.ShardId, req.ShardKey)
	if err != nil {
		return nil, err
	}

	// A shard frozen by a split cutoff accepts no writes anymore: its children
	// own the range (see Read for the routing rules).
	if r.frozenAt() > 0 {
		return n.rerouteUpdate(ctx, req, r.shardId)
	}

	// Writes are applied only on the leader.
	if r.IsLeader() {
		resp, err := r.Update(req.Payload, req.ShardKey)
		if err != nil {
			// The cutoff may have committed between the frozen check above and
			// the propose: the write mutated nothing; re-route it.
			if errors.Is(err, errShardFrozen) {
				return n.rerouteUpdate(ctx, req, r.shardId)
			}
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

	leaderReplica, err := router.GetReplica(leaderReplicaId)
	if err != nil {
		return nil, errLeaderUnknown
	}

	// Forward to the leader's node, pinned by shard id (the leader hosts this
	// exact shard's replica). The shard key is kept: the receiving node still
	// re-resolves by key against its own config, which is always correct
	// because a key's owning routable shard only changes at a split cutoff —
	// and a splitting leader needs the key to stamp the replicated command.
	forward := &transport.UpdateRequest{
		ApplicationName: req.ApplicationName,
		ShardId:         r.shardId,
		ShardKey:        req.ShardKey,
		Payload:         req.Payload,
		Hops:            req.Hops + 1,
	}

	resp, err := n.trans.Update(ctx, leaderReplica.NodeId, forward)
	// The leader may have frozen the shard (split cutoff) before our forward
	// arrived; we still hold the shard key, so re-route to the child here.
	if err != nil && isShardFrozenError(err) {
		return n.rerouteUpdate(ctx, req, r.shardId)
	}
	// If the leader we forwarded to was unreachable it likely just failed; wait
	// for a new election (excluding the old leader) and retry once against it.
	if err != nil && isUnavailableError(err) {
		newLeaderReplicaId, waitErr := r.WaitForNewLeader(ctx, leaderReplicaId)
		if waitErr != nil {
			return nil, errLeaderUnknown
		}
		newLeaderReplica, clusterErr := router.GetReplica(newLeaderReplicaId)
		if clusterErr != nil {
			return nil, errLeaderUnknown
		}
		return n.trans.Update(ctx, newLeaderReplica.NodeId, forward)
	}
	return resp, err
}

// rerouteRead re-dispatches a read that hit a frozen (split) parent shard to
// the child that owns its shard key. Key-less requests (forwards dropped the
// key) return errShardFrozen so the origin node — which has the key — can
// re-route instead.
func (n *Node) rerouteRead(ctx context.Context, req *transport.ReadRequest, parentShardId string) (*transport.ReadResponse, error) {
	childShardId, err := n.childShardOwningKey(parentShardId, req.ShardKey)
	if err != nil {
		return nil, err
	}
	if err := n.awaitLocalReplica(ctx, req.ApplicationName, childShardId); err != nil {
		return nil, err
	}
	return n.Read(ctx, &transport.ReadRequest{
		ApplicationName:        req.ApplicationName,
		ShardId:                childShardId,
		Payload:                req.Payload,
		AllowReadFromFollowers: req.AllowReadFromFollowers,
		Hops:                   req.Hops + 1,
	})
}

// rerouteUpdate is the write-side counterpart of rerouteRead. The rejected
// write mutated nothing on the parent, so re-dispatching it to the child is
// the same at-least-once delivery the system already has.
func (n *Node) rerouteUpdate(ctx context.Context, req *transport.UpdateRequest, parentShardId string) (*transport.UpdateResponse, error) {
	childShardId, err := n.childShardOwningKey(parentShardId, req.ShardKey)
	if err != nil {
		return nil, err
	}
	if err := n.awaitLocalReplica(ctx, req.ApplicationName, childShardId); err != nil {
		return nil, err
	}
	return n.Update(ctx, &transport.UpdateRequest{
		ApplicationName: req.ApplicationName,
		ShardId:         childShardId,
		Payload:         req.Payload,
		Hops:            req.Hops + 1,
	})
}

// childShardOwningKey resolves the child of a (frozen) parent shard that owns
// shardKey, from this node's own applied config. Children are identified by
// ParentId, independent of the config version's routing states, so this works
// on both sides of the split's config flip.
func (n *Node) childShardOwningKey(parentShardId string, shardKey []byte) (string, error) {
	if len(shardKey) == 0 {
		return "", errShardFrozen
	}

	n.mu.RLock()
	clusterConfig := n.clusterConfig
	n.mu.RUnlock()

	var children []*cluster.Shard
	for _, a := range clusterConfig.Applications {
		for _, sh := range a.Shards {
			if sh.ParentId == parentShardId {
				children = append(children, sh)
			}
		}
	}
	child := shardOwningKey(children, shardKey)
	if child == nil {
		// The children partition the frozen parent's range; a routed key
		// always has an owner on a valid config.
		return "", fmt.Errorf("no child of frozen shard %s owns shard key %x", parentShardId, shardKey)
	}
	return child.Id, nil
}

// awaitLocalReplica waits (bounded by ctx) for a serving local replica of the
// given shard to exist. It bridges the short window between the cutoff freeze
// and the promotion of the seeded children, so requests in that window are
// delayed rather than failed.
func (n *Node) awaitLocalReplica(ctx context.Context, applicationName string, shardId string) error {
	for {
		if _, _, err := n.replicaForShard(applicationName, shardId, nil); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for promoted replica of shard %s: %w", shardId, ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

// SplitCutoff proposes the shard-split CUTOFF command through this node's
// replica of the given shard, which must be the Raft leader (callers locate
// the leader via ListReplicaStates). It freezes the shard at the returned log
// index. Idempotent: an already-frozen shard returns its original cutoff
// index.
func (n *Node) SplitCutoff(ctx context.Context, shardId string) (uint64, error) {
	if n.NodeState() != READY {
		return 0, errNodeNotReady
	}

	n.mu.RLock()
	var r *replica
	for _, rep := range n.replicas {
		if rep.shardId == shardId {
			r = rep
			break
		}
	}
	var shardState cluster.ShardState
	var childShardIds []string
	if r != nil {
		if shard, err := n.clusterConfig.GetShard(shardId); err == nil {
			shardState = shard.State
		}
		for _, a := range n.clusterConfig.Applications {
			for _, sh := range a.Shards {
				if sh.ParentId == shardId && sh.State == cluster.ShardState_SHARD_STATE_ACTIVATING {
					childShardIds = append(childShardIds, sh.Id)
				}
			}
		}
	}
	n.mu.RUnlock()

	if r == nil {
		return 0, fmt.Errorf("no replica for shard %s on this node", shardId)
	}
	if m := r.frozenAt(); m > 0 {
		return m, nil
	}
	// Defense in depth: a cutoff for a shard the local config does not show as
	// splitting (with children) indicates an operational bug.
	if shardState != cluster.ShardState_SHARD_STATE_SPLITTING {
		return 0, fmt.Errorf("shard %s is not splitting in the applied config", shardId)
	}
	if len(childShardIds) < 2 {
		return 0, fmt.Errorf("shard %s has %d activating children in the applied config, need at least 2", shardId, len(childShardIds))
	}
	if !r.IsLeader() {
		return 0, errLeaderUnknown
	}
	return r.SplitCutoff(childShardIds)
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
// ReplicaStates returns the observed state of every replica hosted on this
// node: serving replicas with their live Raft state and stats, and dormant
// (seeding) replicas of activating shards with their seeding progress. It is
// the single producer behind ListReplicaStates on both transports.
func (n *Node) ReplicaStates() []*transport.ReplicaState {
	n.mu.RLock()
	serving := make([]*replica, 0, len(n.replicas))
	for _, r := range n.replicas {
		serving = append(serving, r)
	}
	dormant := make([]*dormantReplica, 0, len(n.dormant))
	for _, d := range n.dormant {
		dormant = append(dormant, d)
	}
	// Local parent replicas' applied indexes, for PersistedShared seeding
	// progress (their children's data IS the parent's rows).
	parentApplied := make(map[string]uint64)
	for _, d := range dormant {
		if d.coreType != CoreTypePersistedShared {
			continue
		}
		for _, r := range n.replicas {
			if r.shardId == d.parentShardId {
				parentApplied[d.parentShardId] = r.GetRaftStats().AppliedIndex
			}
		}
	}
	n.mu.RUnlock()

	states := make([]*transport.ReplicaState, 0, len(serving)+len(dormant))
	for _, r := range serving {
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
			panic(fmt.Sprintf("unknown raft state: %v", s))
		}

		rs := r.GetRaftStats()
		states = append(states, &transport.ReplicaState{
			ReplicaId: r.replicaId,
			RaftState: raftState,
			Frozen:    r.frozenAt() > 0,
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
		})
	}
	for _, d := range dormant {
		states = append(states, &transport.ReplicaState{
			ReplicaId:   d.replicaId,
			RaftState:   transport.RaftStateSeeding,
			Seeding:     true,
			SeededIndex: d.seededIndex(parentApplied),
		})
	}
	return states
}

// seededIndex reports how far this dormant replica's durable seed has
// progressed, in parent log indexes. parentApplied carries local parent
// replicas' applied indexes (used for PersistedShared, whose children alias
// the parent's rows and are therefore always exactly as fresh as the parent).
func (d *dormantReplica) seededIndex(parentApplied map[string]uint64) uint64 {
	switch d.coreType {
	case CoreTypeInMemory:
		last, err := d.seeder.LastSeededIndex()
		if err != nil {
			return 0
		}
		return last
	case CoreTypePersistedExclusive:
		idx, err := d.seeder.CatchUpIndex()
		if err != nil {
			return 0
		}
		return idx
	case CoreTypePersistedShared:
		return parentApplied[d.parentShardId]
	default:
		return 0
	}
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

// setClusterConfigLocked swaps in a new cluster config together with the routing
// index built from it, keeping the pair consistent. Must be called with n.mu
// held for writing.
func (n *Node) setClusterConfigLocked(cfg *cluster.Config) {
	n.clusterConfig = cfg
	n.router = NewRouter(cfg)
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

	// Stop all split seeding pipelines before touching the replica maps; they
	// are restarted from the new config (and resume from durable progress).
	n.stopSplitters()

	n.mu.Lock()

	// The transition from the currently applied config to the new one must be
	// safe (no shard removal, no replica reassignment, monotonic version, ...).
	// This is checked under mu against the live config so concurrent updates
	// can't race past each other.
	if err := cluster.ValidateTransition(n.clusterConfig, newConfig); err != nil {
		n.mu.Unlock()
		n.startSplitters()
		return fmt.Errorf("invalid cluster config transition: %w", err)
	}

	// Persist the new config durably before acknowledging the swap, so a restart
	// resumes at the applied version rather than a stale seed. The write is atomic
	// (temp + fsync + rename), so a crash never leaves a torn config.
	if err := cluster.WriteConfigToFile(newConfig, clusterConfigPath(n.baseDir)); err != nil {
		n.mu.Unlock()
		return fmt.Errorf("persisting cluster config: %w", err)
	}

	// replicas, clusterConfig and router must be updated together under mu so they
	// stay a matched set.
	n.setClusterConfigLocked(newConfig)

	if err := n.reconcileReplicasLocked(); err != nil {
		n.mu.Unlock()
		return err
	}
	if err := n.bootstrapShards(); err != nil {
		n.mu.Unlock()
		return err
	}
	n.mu.Unlock()

	n.startSplitters()

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
// the routing index used to resolve it, as a consistent snapshot taken under a
// single read lock. Because replicas and router are only ever mutated together
// (on config reload), the returned pair is guaranteed to match, and the lock is
// released before the caller performs the actual read/update so it never
// serializes them. The returned router is what the caller must use to resolve
// the shard's leader replica, so leader resolution sees the same config version
// that resolved the shard.
//
// For sharded requests (shardKey is non-empty) the owning shard is resolved from
// shardKey against this node's own config, so routing is correct even when the
// caller's config is a different version (e.g. mid-split or during a rolling
// config rollout). For direct-shard requests (empty shardKey) the shard is taken
// from shardId as-is. A node hosts at most one replica per shard, so the shard
// determines the replica uniquely.
func (n *Node) replicaForShard(applicationName string, shardId string, shardKey []byte) (*replica, *Router, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	router := n.router

	targetShardId := shardId
	if len(shardKey) > 0 {
		shard, err := router.FindShardByShardKey(applicationName, shardKey)
		if err != nil {
			return nil, nil, err
		}
		targetShardId = shard.Id
	}

	for _, r := range n.replicas {
		if r.shardId == targetShardId {
			return r, router, nil
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

	// Create replicas newly assigned to this node. Replicas of ACTIVATING
	// shards are created DORMANT: no core, no Raft — just their durable stores,
	// which the split seeding pipeline fills locally. They are promoted to
	// serving replicas when their shard leaves the activating state.
	for id, p := range desired {
		coreDescriptor, ok := n.coreDescriptors[p.app.Implementation]
		if !ok {
			return fmt.Errorf("no core registered for %s", p.app.Implementation)
		}

		if p.shard.State == cluster.ShardState_SHARD_STATE_ACTIVATING {
			if _, ok := n.dormant[id]; ok {
				continue
			}
			if _, ok := n.replicas[id]; ok {
				// Already promoted by cutoff finalization; the config flip
				// (activating -> active) will catch up with reality.
				continue
			}
			n.dormant[id] = &dormantReplica{
				applicationName: p.app.Name,
				shardId:         p.shard.Id,
				parentShardId:   p.shard.ParentId,
				replicaId:       id,
				coreType:        coreDescriptor.CoreType,
				seeder:          raft.NewSeeder(n.baseDir, id, n.raftStore),
			}
			n.logger.Printf("Created dormant replica %s (activating shard %s, parent %s)", id, p.shard.Id, p.shard.ParentId)
			continue
		}

		if _, ok := n.replicas[id]; ok {
			continue
		}
		// Promotion: a previously dormant replica whose shard is no longer
		// activating starts as a regular replica over its seeded stores. The
		// Seeder must be discarded before the live Raft is constructed.
		if _, ok := n.dormant[id]; ok {
			delete(n.dormant, id)
			n.logger.Printf("Promoting seeded replica %s (shard %s)", id, p.shard.Id)
		}
		applicationCore := coreDescriptor.CoreFactoryFunc(p.shard, p.replica)
		rep := newReplica(n.baseDir, p.app.Name, p.shard.Id, id, n.nodeId, applicationCore, n.trans, n.raftStore, coreDescriptor.CoreType.RestoreSnapshotOnStart(), n.nodeConfig.MaxUpdateTimeout)
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
		n.dropReplicaData(id)
	}

	// Same for dormant replicas (e.g. an aborted split removing the children):
	// nothing to close, just the durable seed to drop.
	for id, d := range n.dormant {
		if _, ok := desired[id]; ok {
			continue
		}
		n.logger.Printf("Removing dormant replica %s (shard %s)", id, d.shardId)
		delete(n.dormant, id)
		n.dropReplicaData(id)
	}

	// Mark serving replicas of SPLITTING shards: a splitting leader stamps
	// every proposed update with its shard key so the seeding pipeline can
	// route entries to children.
	for id, rep := range n.replicas {
		p, ok := desired[id]
		if !ok {
			continue
		}
		rep.setSplitting(p.shard.State == cluster.ShardState_SHARD_STATE_SPLITTING)
	}

	return nil
}

// placement is one desired replica assignment on this node, with the config
// entities it derives from.
type placement struct {
	app     *cluster.Application
	shard   *cluster.Shard
	replica *cluster.Replica
}

// dormantReplica is a replica of an ACTIVATING shard: it exists in the config
// and owns durable Raft stores that the split seeding pipeline fills locally,
// but it runs no Raft and serves nothing until the split cutoff promotes it.
type dormantReplica struct {
	applicationName string
	shardId         string
	parentShardId   string
	replicaId       string
	coreType        CoreType

	// seeder is the write/read handle over this replica's durable stores,
	// shared between the splitter (writes) and observability (reads).
	seeder *raft.Seeder
}

// startSplitters launches a split seeding pipeline for every SPLITTING shard
// this node hosts a serving parent replica for, pairing it with the
// co-located dormant children from the applied config. Idempotent per config
// apply: it is only called after stopSplitters, so the map is empty.
func (n *Node) startSplitters() {
	n.splittersMu.Lock()
	defer n.splittersMu.Unlock()

	n.mu.RLock()
	clusterConfig := n.clusterConfig
	if clusterConfig == nil {
		n.mu.RUnlock()
		return
	}

	type pending struct {
		parent   *replica
		coreType CoreType
		factory  func(*cluster.Shard, *cluster.Replica) ApplicationCore
		children []*splitChild
	}
	var toStart []pending

	for _, a := range clusterConfig.Applications {
		descriptor, ok := n.coreDescriptors[a.Implementation]
		if !ok {
			continue
		}
		for _, shard := range a.Shards {
			if shard.State != cluster.ShardState_SHARD_STATE_SPLITTING {
				continue
			}
			var parent *replica
			for _, r := range n.replicas {
				if r.shardId == shard.Id {
					parent = r
					break
				}
			}
			if parent == nil {
				continue // this node hosts no parent replica
			}

			var children []*splitChild
			for _, ch := range a.Shards {
				if ch.ParentId != shard.Id || ch.State != cluster.ShardState_SHARD_STATE_ACTIVATING {
					continue
				}
				replicaSet := make([]raft.RaftServer, len(ch.Replicas))
				for i, r := range ch.Replicas {
					replicaSet[i] = raft.RaftServer{ReplicaId: r.Id, NodeId: r.NodeId}
				}
				for _, r := range ch.Replicas {
					if d, ok := n.dormant[r.Id]; ok {
						children = append(children, &splitChild{
							shard:      ch,
							replicaSet: replicaSet,
							dormant:    d,
						})
					}
				}
			}
			if len(children) == 0 {
				continue
			}
			toStart = append(toStart, pending{
				parent:   parent,
				coreType: descriptor.CoreType,
				factory:  descriptor.CoreFactoryFunc,
				children: children,
			})
		}
	}
	n.mu.RUnlock()

	for _, p := range toStart {
		childIds := make([]string, len(p.children))
		for i, ch := range p.children {
			childIds[i] = ch.dormant.replicaId
		}
		promote := func() error { return n.promoteSeededChildren(childIds) }
		sp := newSplitter(p.parent, p.coreType, p.children, p.factory, promote, n.logger)
		n.splitters[p.parent.replicaId] = sp
		sp.start()
		n.logger.Printf("Started split seeding of shard %s into %d children", p.parent.shardId, len(p.children))
	}
}

// promoteSeededChildren turns seeded dormant replicas into serving replicas
// in place: a regular Raft is constructed over the pre-baked stores (no
// Bootstrap; membership comes from the base snapshot metadata). Called by the
// splitter after cutoff finalization; idempotent (already-promoted or removed
// children are skipped). The cluster config still says ACTIVATING at this
// point — the split sequence's flip catches it up.
func (n *Node) promoteSeededChildren(childReplicaIds []string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, id := range childReplicaIds {
		d, ok := n.dormant[id]
		if !ok {
			continue
		}

		shard, err := n.clusterConfig.GetShard(d.shardId)
		if err != nil {
			return fmt.Errorf("promoting replica %s: %w", id, err)
		}
		replicaEntry := replicaOf(shard, id)
		if replicaEntry == nil {
			return fmt.Errorf("promoting replica %s: not in shard %s config", id, d.shardId)
		}
		var descriptor ApplicationCoreDescriptor
		found := false
		for _, a := range n.clusterConfig.Applications {
			if a.Name == d.applicationName {
				descriptor, found = n.coreDescriptors[a.Implementation], true
				break
			}
		}
		if !found {
			return fmt.Errorf("promoting replica %s: application %s not found", id, d.applicationName)
		}

		// The Seeder must be discarded before the live Raft is constructed.
		delete(n.dormant, id)

		core := descriptor.CoreFactoryFunc(shard, replicaEntry)
		rep := newReplica(n.baseDir, d.applicationName, d.shardId, id, n.nodeId, core, n.trans, n.raftStore, descriptor.CoreType.RestoreSnapshotOnStart(), n.nodeConfig.MaxUpdateTimeout)
		n.replicas[id] = rep
		n.logger.Printf("Promoted seeded replica %s (shard %s)", id, d.shardId)
	}
	return nil
}

// stopSplitters stops every running split seeding pipeline and waits for them
// to exit. Progress is durable; a subsequent startSplitters resumes it.
func (n *Node) stopSplitters() {
	n.splittersMu.Lock()
	defer n.splittersMu.Unlock()

	for id, sp := range n.splitters {
		sp.stop()
		delete(n.splitters, id)
	}
}

// dropReplicaData deletes a replica's durable state: its prefix in the shared
// raft store and its snapshot directory. Failures are logged, not returned —
// leftover data of a removed replica is unreachable (ids are never reused).
func (n *Node) dropReplicaData(id string) {
	if err := n.raftStore.DropPrefix([]byte(id)); err != nil {
		n.logger.Printf("error dropping raft data for replica %s: %v", id, err)
	}
	if err := os.RemoveAll(filepath.Join(n.baseDir, "snapshots", id)); err != nil {
		n.logger.Printf("error removing snapshots for replica %s: %v", id, err)
	}
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

// isShardFrozenError reports whether err indicates the target shard was frozen
// by a split cutoff — locally (errShardFrozen) or returned by a forwarded-to
// node over the transport (matched on the message, like errLeaderUnknown).
func isShardFrozenError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errShardFrozen) {
		return true
	}
	msg := err.Error()
	if st, ok := status.FromError(err); ok {
		msg = st.Message()
	}
	return strings.Contains(msg, errShardFrozen.Error())
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
	for name, d := range coreDescriptors {
		switch d.CoreType {
		case CoreTypeInMemory, CoreTypePersistedShared, CoreTypePersistedExclusive:
		default:
			return nil, fmt.Errorf("core descriptor %s: invalid CoreType %v (must be declared explicitly)", name, d.CoreType)
		}
	}

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
		router:          NewRouter(effectiveConfig),
		nodeState:       INITIAL,
		replicas:        make(map[string]*replica),
		dormant:         make(map[string]*dormantReplica),
		splitters:       make(map[string]*splitter),
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
