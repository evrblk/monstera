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
	errNodeNotReady  = errors.New("node is not in READY state")
	errLeaderUnknown = errors.New("leader is unknown")
)

type Node struct {
	baseDir         string
	nodeId          string
	coreDescriptors ApplicationCoreDescriptors

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

type NodeState = int

const (
	INITIAL NodeState = iota
	READY
	STOPPED
)

type NodeConfig struct {
	MaxHops          int32
	MaxReadTimeout   time.Duration
	MaxUpdateTimeout time.Duration
	ElectionWaitTime time.Duration

	// UseInMemoryRaftStore set to `true` should be used only in unit tests or dev environment and is not
	// recommended for production use, since in-memory Raft store is not durable.
	UseInMemoryRaftStore bool
}

var DefaultMonsteraNodeConfig = NodeConfig{
	MaxHops:          5,
	MaxReadTimeout:   10 * time.Second,
	MaxUpdateTimeout: 30 * time.Second,
	ElectionWaitTime: 1 * time.Second,

	UseInMemoryRaftStore: false,
}

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

func (n *Node) Start() {
	n.smu.Lock()
	defer n.smu.Unlock()

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

func (n *Node) Read(ctx context.Context, request *transport.ReadRequest) (*transport.ReadResponse, error) {
	if n.NodeState() != READY {
		return nil, errNodeNotReady
	}

	r, err := n.getReplica(request.ReplicaId)
	if err != nil {
		return nil, err
	}

	if request.AllowReadFromFollowers {
		payload, err := r.Read(request.Payload)
		if err != nil {
			return nil, err
		}
		return &transport.ReadResponse{Payload: payload}, nil
	}

	if r.IsLeader() {
		payload, err := r.Read(request.Payload)
		if err != nil {
			return nil, err
		}
		return &transport.ReadResponse{Payload: payload}, nil
	}

	if request.Hops >= n.nodeConfig.MaxHops {
		return nil, errLeaderUnknown
	}

	leaderReplicaId, err := r.GetRaftLeader(ctx)
	if err != nil {
		return nil, errLeaderUnknown
	}

	leaderReplica, err := n.clusterConfig.GetReplica(leaderReplicaId)
	if err != nil {
		return nil, errLeaderUnknown
	}

	result, err := n.trans.Read(ctx, leaderReplica.NodeId, &transport.ReadRequest{
		ApplicationName:        request.ApplicationName,
		ShardId:                request.ShardId,
		ReplicaId:              leaderReplica.Id,
		ShardKey:               request.ShardKey,
		Payload:                request.Payload,
		AllowReadFromFollowers: request.AllowReadFromFollowers,
		Hops:                   request.Hops + 1,
	})
	if err != nil && isUnavailableError(err) {
		newLeaderReplicaId, waitErr := r.WaitForNewLeader(ctx, leaderReplicaId)
		if waitErr != nil {
			return nil, errLeaderUnknown
		}
		newLeaderReplica, clusterErr := n.clusterConfig.GetReplica(newLeaderReplicaId)
		if clusterErr != nil {
			return nil, errLeaderUnknown
		}
		return n.trans.Read(ctx, newLeaderReplica.NodeId, &transport.ReadRequest{
			ApplicationName:        request.ApplicationName,
			ShardId:                request.ShardId,
			ReplicaId:              newLeaderReplica.Id,
			ShardKey:               request.ShardKey,
			Payload:                request.Payload,
			AllowReadFromFollowers: request.AllowReadFromFollowers,
			Hops:                   request.Hops + 1,
		})
	}
	return result, err
}

func (n *Node) Update(ctx context.Context, request *transport.UpdateRequest) (*transport.UpdateResponse, error) {
	if n.NodeState() != READY {
		return nil, errNodeNotReady
	}

	r, err := n.getReplica(request.ReplicaId)
	if err != nil {
		return nil, err
	}

	if r.IsLeader() {
		payload, err := r.Update(request.Payload)
		if err != nil {
			return nil, err
		}
		return &transport.UpdateResponse{Payload: payload}, nil
	}

	if request.Hops >= n.nodeConfig.MaxHops {
		return nil, errLeaderUnknown
	}

	leaderReplicaId, err := r.GetRaftLeader(ctx)
	if err != nil {
		return nil, errLeaderUnknown
	}

	leaderReplica, err := n.clusterConfig.GetReplica(leaderReplicaId)
	if err != nil {
		return nil, errLeaderUnknown
	}

	result, err := n.trans.Update(ctx, leaderReplica.NodeId, &transport.UpdateRequest{
		ApplicationName: request.ApplicationName,
		ShardId:         request.ShardId,
		ReplicaId:       leaderReplica.Id,
		ShardKey:        request.ShardKey,
		Payload:         request.Payload,
		Hops:            request.Hops + 1,
	})
	if err != nil && isUnavailableError(err) {
		newLeaderReplicaId, waitErr := r.WaitForNewLeader(ctx, leaderReplicaId)
		if waitErr != nil {
			return nil, errLeaderUnknown
		}
		newLeaderReplica, clusterErr := n.clusterConfig.GetReplica(newLeaderReplicaId)
		if clusterErr != nil {
			return nil, errLeaderUnknown
		}
		return n.trans.Update(ctx, newLeaderReplica.NodeId, &transport.UpdateRequest{
			ApplicationName: request.ApplicationName,
			ShardId:         request.ShardId,
			ReplicaId:       newLeaderReplica.Id,
			ShardKey:        request.ShardKey,
			Payload:         request.Payload,
			Hops:            request.Hops + 1,
		})
	}
	return result, err
}

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

func (n *Node) RaftMessage(ctx context.Context, request *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
	if n.NodeState() != READY {
		return nil, errNodeNotReady
	}

	r, err := n.getReplica(request.ReplicaId)
	if err != nil {
		return nil, err
	}

	return r.RaftMessage(request)
}

func (n *Node) ListReplicas() []*replica {
	n.mu.RLock()
	defer n.mu.RUnlock()

	result := make([]*replica, 0, len(n.replicas))
	for _, r := range n.replicas {
		result = append(result, r)
	}

	return result
}

func (n *Node) NodeState() NodeState {
	n.smu.Lock()
	defer n.smu.Unlock()

	return n.nodeState
}

func (n *Node) NodeId() string {
	return n.nodeId
}

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

	n.clusterConfig = newConfig
	// TODO implement config loading

	return nil
}

func (n *Node) getReplica(replicaId string) (*replica, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	r, ok := n.replicas[replicaId]
	if !ok {
		return nil, fmt.Errorf("no replica %s found on this node %s", replicaId, n.nodeId)
	}
	return r, nil
}

func (n *Node) loadCores() error {
	found := false
	for _, nd := range n.clusterConfig.Nodes {
		if nd.Id == n.nodeId {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("node not found in cluster config")
	}

	for _, a := range n.clusterConfig.Applications {
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
					replica := newReplica(n.baseDir, a.Name, s.Id, r.Id, n.nodeId, applicationCore, n.trans, n.raftStore, coreDescriptor.RestoreSnapshotOnStart)

					n.replicas[r.Id] = replica
				}
			}
		}
	}

	return nil
}

func (n *Node) bootstrapShards() error {
	for _, r := range n.replicas {
		s, err := n.clusterConfig.GetShard(r.shardId)
		if err != nil {
			return err
		}

		// Only the first replica in the shard can bootstrap
		if s.Replicas[0].NodeId == n.nodeId {
			// Bootstrap the shard if it's not bootstrapped yet
			if !r.IsBootstrapped() {
				// Add all replicas to the bootstrap list
				servers := make([]hraft.Server, len(s.Replicas))
				for i, r := range s.Replicas {
					for _, nd := range n.clusterConfig.Nodes {
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

func isUnavailableError(err error) bool {
	if st, ok := status.FromError(err); ok {
		return st.Code() == codes.Unavailable
	}
	return false
}

func NewNode(baseDir string, nodeId string, clusterConfig *cluster.Config, coreDescriptors ApplicationCoreDescriptors, nodeConfig NodeConfig, trans transport.Transport) (*Node, error) {
	var raftStore *store.BadgerStore
	var err error
	if nodeConfig.UseInMemoryRaftStore {
		raftStore, err = store.NewBadgerInMemoryStore()
	} else {
		raftStore, err = store.NewBadgerStore(filepath.Join(baseDir, "raft"))
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
