package monstera

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/samber/lo"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var (
	errNodeNotReady  = errors.New("node is not in READY state")
	errLeaderUnknown = errors.New("leader is unknown")
)

type MonsteraNode struct {
	baseDir         string
	nodeAddress     string
	coreDescriptors ApplicationCoreDescriptors

	mu            sync.RWMutex
	replicas      map[string]*MonsteraReplica
	clusterConfig *ClusterConfig

	smu       sync.Mutex
	nodeState MonsteraNodeState

	pool      *MonsteraConnectionPool
	raftStore *BadgerStore

	monsteraNodeConfig MonsteraNodeConfig

	logger *log.Logger
}

type MonsteraNodeState = int

const (
	INITIAL MonsteraNodeState = iota + 1
	READY
	STOPPED

	maxHops = 3
)

// ApplicationCoreDescriptors map is used to register application cores with Monstera.
// Key: the name of the application core, it should match Application.Implementation in ClusterConfig.
// Value: application core descriptor.
type ApplicationCoreDescriptors = map[string]ApplicationCoreDescriptor

// ApplicationCoreDescriptor is used to register an application core with Monstera.
type ApplicationCoreDescriptor struct {
	// CoreFactoryFunc is a function that creates a new application core. It is called when
	// Monstera node starts for every replica on this node, and also for every new replica that
	// is added to the node while it is running.
	CoreFactoryFunc func(shard *Shard, replica *Replica) ApplicationCore

	// RestoreSnapshotOnStart is a flag that indicates if the application core should restore its
	// state from a snapshot on start (via ApplicationCore.Restore). For fully in-memory applications,
	// this flag should be true. For applications that are backed by an on-disk embedded storage this
	// might or might not be necessary, depending on implementation.
	RestoreSnapshotOnStart bool
}

type MonsteraNodeConfig struct {
	MaxHops          int
	MaxReadTimeout   time.Duration
	MaxUpdateTimeout time.Duration

	// UseInMemoryRaftStore set to `true` should be used only in unit tests or dev environment and is not
	// recommended for production use, since in-memory Raft store is not durable.
	UseInMemoryRaftStore bool
}

var DefaultMonsteraNodeConfig = MonsteraNodeConfig{
	MaxHops:          10,
	MaxReadTimeout:   10 * time.Second,
	MaxUpdateTimeout: 30 * time.Second,

	UseInMemoryRaftStore: false,
}

func (n *MonsteraNode) Stop() {
	n.smu.Lock()
	defer n.smu.Unlock()

	if n.nodeState == STOPPED {
		n.logger.Printf("Monstera Node already stopped")
		return
	}

	n.logger.Printf("Stopping Monstera Node")

	n.nodeState = STOPPED

	n.pool.Close()

	for _, b := range n.replicas {
		b.Close()
	}

	n.logger.Printf("Monstera Node stopped")

	n.raftStore.Close()
}

func (n *MonsteraNode) Start() {
	n.smu.Lock()
	defer n.smu.Unlock()

	n.logger.Printf("Starting Monstera Node. Config version: %d", n.clusterConfig.UpdatedAt)

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

func (n *MonsteraNode) AddVoter(ctx context.Context, replicaId string, voterReplicaId string, voterAddress string) error {
	r, err := n.getReplica(replicaId)
	if err != nil {
		return err
	}

	err = r.AddVoter(voterReplicaId, voterAddress)
	if err != nil {
		return err
	}

	return nil
}

func (n *MonsteraNode) Read(ctx context.Context, request *ReadRequest) ([]byte, error) {
	if n.NodeState() != READY {
		return nil, errNodeNotReady
	}

	r, err := n.getReplica(request.ReplicaId)
	if err != nil {
		return nil, err
	}

	if request.AllowReadFromFollowers {
		return r.Read(request.Payload)
	} else {
		if r.GetRaftState() == hraft.Leader {
			return r.Read(request.Payload)
		} else {
			if request.Hops >= maxHops {
				return nil, errLeaderUnknown
			}

			address, id := r.GetRaftLeader()
			if address == "" || id == "" {
				// TODO wait?
				return nil, errLeaderUnknown
			}
			conn, err := n.pool.GetConnection(string(address))
			if err != nil {
				n.logger.Print(err)
				return nil, err
			}

			redirectedRequest := proto.Clone(request).(*ReadRequest)
			redirectedRequest.ReplicaId = string(id)
			redirectedRequest.Hops = redirectedRequest.Hops + 1

			resp, err := conn.Read(ctx, redirectedRequest, grpc.WaitForReady(true))
			if err != nil {
				return nil, err
			} else {
				return resp.Payload, nil
			}
		}
	}
}

func (n *MonsteraNode) Update(ctx context.Context, request *UpdateRequest) ([]byte, error) {
	if n.NodeState() != READY {
		return nil, errNodeNotReady
	}

	r, err := n.getReplica(request.ReplicaId)
	if err != nil {
		return nil, err
	}

	if r.GetRaftState() == hraft.Leader {
		return r.Update(request.Payload)
	} else {
		if request.Hops >= maxHops {
			return nil, errLeaderUnknown
		}

		address, id := r.GetRaftLeader()
		if address == "" || id == "" {
			// TODO wait?
			return nil, errLeaderUnknown
		}
		conn, err := n.pool.GetConnection(string(address))
		if err != nil {
			n.logger.Print(err)
			return nil, err
		}

		redirectedRequest := proto.Clone(request).(*UpdateRequest)
		redirectedRequest.ReplicaId = string(id)
		redirectedRequest.Hops = redirectedRequest.Hops + 1

		resp, err := conn.Update(ctx, redirectedRequest, grpc.WaitForReady(true))
		if err != nil {
			return nil, err
		} else {
			return resp.Payload, nil
		}
	}
}

func (n *MonsteraNode) AppendEntries(replicaId string, request *hraft.AppendEntriesRequest) (*hraft.AppendEntriesResponse, error) {
	if n.NodeState() != READY {
		return nil, errNodeNotReady
	}

	r, err := n.getReplica(replicaId)
	if err != nil {
		return nil, err
	}

	return r.AppendEntries(request)
}

func (n *MonsteraNode) TriggerSnapshot(replicaId string) error {
	r, err := n.getReplica(replicaId)
	if err != nil {
		return err
	}

	r.TriggerSnapshot()

	return nil
}

func (n *MonsteraNode) LeadershipTransfer(replicaId string) error {
	r, err := n.getReplica(replicaId)
	if err != nil {
		return err
	}

	return r.LeadershipTransfer()
}

func (n *MonsteraNode) RequestVote(ctx context.Context, replicaId string, request *hraft.RequestVoteRequest) (*hraft.RequestVoteResponse, error) {
	if n.NodeState() != READY {
		return nil, errNodeNotReady
	}

	r, err := n.getReplica(replicaId)
	if err != nil {
		return nil, err
	}

	return r.RequestVote(request)
}

func (n *MonsteraNode) TimeoutNow(ctx context.Context, replicaId string, request *hraft.TimeoutNowRequest) (*hraft.TimeoutNowResponse, error) {
	if n.NodeState() != READY {
		return nil, errNodeNotReady
	}

	r, err := n.getReplica(replicaId)
	if err != nil {
		return nil, err
	}

	return r.TimeoutNow(request)
}

func (n *MonsteraNode) InstallSnapshot(replicaId string, request *hraft.InstallSnapshotRequest, data io.Reader) (*hraft.InstallSnapshotResponse, error) {
	if n.NodeState() != READY {
		return nil, errNodeNotReady
	}

	r, err := n.getReplica(replicaId)
	if err != nil {
		return nil, err
	}

	return r.InstallSnapshot(request, data)
}

func (n *MonsteraNode) ListCores() []*MonsteraReplica {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return lo.Values(n.replicas)
}

func (n *MonsteraNode) NodeState() MonsteraNodeState {
	n.smu.Lock()
	defer n.smu.Unlock()

	return n.nodeState
}

func (n *MonsteraNode) UpdateClusterConfig(ctx context.Context, newConfig *ClusterConfig) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	replicasToAdd := make(map[string]bool)
	replicasToRemove := make(map[string]bool)

	for _, nr := range n.replicas {
		replicasToRemove[nr.ReplicaId] = true
		for _, a := range newConfig.Applications {
			for _, s := range a.Shards {
				for _, r := range s.Replicas {
					if r.NodeAddress == n.nodeAddress && r.Id == nr.ReplicaId {
						delete(replicasToRemove, nr.ReplicaId)
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
				if r.NodeAddress == n.nodeAddress {
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

func (n *MonsteraNode) getReplica(replicaId string) (*MonsteraReplica, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	r, ok := n.replicas[replicaId]
	if !ok {
		return nil, fmt.Errorf("no replica %s found on this node %s", replicaId, n.nodeAddress)
	}
	return r, nil
}

func (n *MonsteraNode) loadCores() error {
	for _, a := range n.clusterConfig.Applications {
		for _, s := range a.Shards {
			for _, r := range s.Replicas {
				if r.NodeAddress == n.nodeAddress {
					// Find core descriptor
					coreDescriptor, ok := n.coreDescriptors[a.Implementation]
					if !ok {
						return fmt.Errorf("no core registered for %s", a.Implementation)
					}

					// Create replica
					applicationCore := coreDescriptor.CoreFactoryFunc(s, r)
					replica := NewMonsteraReplica(n.baseDir, a.Name, s.Id, r.Id, n.nodeAddress, applicationCore, n.pool, n.raftStore, coreDescriptor.RestoreSnapshotOnStart)

					n.replicas[r.Id] = replica
				}
			}
		}
	}

	return nil
}

func (n *MonsteraNode) bootstrapShards() error {
	for _, r := range n.replicas {
		s, err := n.clusterConfig.GetShard(r.ShardId)
		if err != nil {
			return err
		}

		// Only the first replica in the shard can bootstrap
		if s.Replicas[0].NodeAddress == n.nodeAddress {
			// Bootstrap the shard if it's not bootstrapped yet
			if !r.IsBootstrapped() {
				// Add all replicas to the bootstrap list
				servers := make([]hraft.Server, len(s.Replicas))
				for i, r := range s.Replicas {
					servers[i] = hraft.Server{
						Suffrage: hraft.Voter,
						ID:       hraft.ServerID(r.Id),
						Address:  hraft.ServerAddress(r.NodeAddress),
					}
				}
				r.Bootstrap(servers)
			}
		}
	}

	return nil
}

func NewNode(baseDir string, nodeAddress string, clusterConfig *ClusterConfig, coreDescriptors ApplicationCoreDescriptors, monsteraNodeConfig MonsteraNodeConfig) (*MonsteraNode, error) {
	var raftStore *BadgerStore
	if monsteraNodeConfig.UseInMemoryRaftStore {
		raftStore = NewBadgerInMemoryStore()
	} else {
		raftStore = NewBadgerStore(filepath.Join(baseDir, "raft"))
	}

	for _, a := range clusterConfig.GetApplications() {
		if _, ok := coreDescriptors[a.Implementation]; !ok {
			return nil, fmt.Errorf("no core implementation registered for %s", a.Implementation)
		}
	}

	monsteraNode := &MonsteraNode{
		baseDir:            baseDir,
		nodeAddress:        nodeAddress,
		coreDescriptors:    coreDescriptors,
		clusterConfig:      clusterConfig,
		nodeState:          INITIAL,
		replicas:           make(map[string]*MonsteraReplica),
		pool:               NewMonsteraConnectionPool(),
		raftStore:          raftStore,
		monsteraNodeConfig: monsteraNodeConfig,
		logger:             log.New(os.Stderr, fmt.Sprintf("[%s] ", nodeAddress), log.LstdFlags),
	}

	return monsteraNode, nil
}
