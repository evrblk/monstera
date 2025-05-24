package monstera

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
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
	mu sync.RWMutex

	baseDir         string
	nodeId          string
	node            *Node
	coreDescriptors map[string]*ApplicationCoreDescriptor
	replicas        map[string]*MonsteraReplica

	clusterConfig *ClusterConfig
	nodeState     MonsteraNodeState
	cancel        context.CancelFunc

	pool      *MonsteraConnectionPool
	raftStore *BadgerStore
}

type MonsteraNodeState = int

const (
	INITIAL MonsteraNodeState = iota + 1
	READY
	STOPPED
)

// ApplicationCoreDescriptor is used to register an application core with Monstera framework.
type ApplicationCoreDescriptor struct {
	// Name is the name of the application core. It should match Application.Implementation
	// in ClusterConfig.
	Name string

	// CoreFactoryFunc is a function that creates a new application core. It is called when
	// Monstera node starts for every replica on this node, and also for every new replica that
	// is added to the node while it is running.
	CoreFactoryFunc func(application *Application, shard *Shard, replica *Replica) ApplicationCore

	// RestoreSnapshotOnStart is a flag that indicates if the application core should restore its
	// state from a snapshot on start (via ApplicationCore.Restore). For fully in-memory applications
	// this flag should be true. For applications that are backed by an on-disk embedded storage this
	// might or might not be necessary, depending on implementation.
	RestoreSnapshotOnStart bool
}

// RegisterApplicationCore registers an application core with Monstera framework.
// It should be called before MonsteraNode.Start.
func (n *MonsteraNode) RegisterApplicationCore(descriptor *ApplicationCoreDescriptor) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.nodeState != INITIAL {
		panic("cannot register application core after node has started")
	}

	if _, ok := n.coreDescriptors[descriptor.Name]; ok {
		panic(fmt.Sprintf("core %s already registered", descriptor.Name))
	}

	n.coreDescriptors[descriptor.Name] = descriptor
}

func (n *MonsteraNode) Stop() {
	log.Printf("Stopping Monstera Node")

	n.nodeState = STOPPED

	n.pool.Close()

	//n.mu.Lock()
	//defer n.mu.Unlock()

	if n.cancel != nil {
		n.cancel()
	}

	for _, b := range n.replicas {
		b.Close()
	}

	log.Printf("Stopped Monstera Node")
}

func (n *MonsteraNode) Start() {
	log.Printf("[%s] Starting Monstera Node", n.nodeId)

	mn, err := n.clusterConfig.GetNode(n.nodeId)
	if err != nil {
		panic(err)
	}

	n.node = mn

	log.Printf("[%s] Loading replicas...", n.nodeId)
	err = n.loadCores()
	if err != nil {
		panic(err)
	}

	err = n.bootstrapShards()
	if err != nil {
		panic(err)
	}

	n.nodeState = READY

	log.Printf("[%s] Node loaded %d replicas", n.nodeId, len(n.replicas))
	log.Printf("[%s] Node is ready", n.nodeId)
}

func (n *MonsteraNode) AddVoter(replicaId string, voterReplicaId string, voterAddress string) error {
	n.mu.RLock()
	defer n.mu.RUnlock()

	b, ok := n.replicas[replicaId]
	if !ok {
		return fmt.Errorf("no replica %s found on this node %s", replicaId, n.node.Id)
	}

	err := b.AddVoter(voterReplicaId, voterAddress)
	if err != nil {
		return err
	}

	return nil
}

func (n *MonsteraNode) Read(request *ReadRequest) ([]byte, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.nodeState != READY {
		return nil, errNodeNotReady
	}

	b, ok := n.replicas[request.ReplicaId]
	if !ok {
		return nil, fmt.Errorf("no replica %s found on this node %s", request.ReplicaId, n.node.Id)
	}

	if request.AllowReadFromFollowers {
		return b.Read(request.Payload)
	} else {
		if b.GetRaftState() == hraft.Leader {
			return b.Read(request.Payload)
		} else {
			address, id := b.GetRaftLeader()
			if address == "" || id == "" {
				// TODO wait?
				return nil, errLeaderUnknown
			}
			c, err := n.pool.GetPeer(string(id), string(address))
			if err != nil {
				log.Print(err)
				return nil, err
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			redirectedRequest := proto.Clone(request).(*ReadRequest)
			redirectedRequest.ReplicaId = string(id)
			// TODO increment hops

			resp, err := c.Read(ctx, redirectedRequest, grpc.WaitForReady(true))
			if err != nil {
				return nil, err
			} else {
				return resp.Payload, nil
			}
		}
	}
}

func (n *MonsteraNode) Update(request *UpdateRequest) ([]byte, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.nodeState != READY {
		return nil, errNodeNotReady
	}

	b, ok := n.replicas[request.ReplicaId]
	if !ok {
		return nil, fmt.Errorf("no replica %s found on this node %s", request.ReplicaId, n.node.Id)
	}

	if b.GetRaftState() == hraft.Leader {
		return b.Update(request.Payload)
	} else {
		address, id := b.GetRaftLeader()
		if address == "" || id == "" {
			// TODO wait?
			return nil, errLeaderUnknown
		}
		c, err := n.pool.GetPeer(string(id), string(address))
		if err != nil {
			log.Print(err)
			return nil, err
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		redirectedRequest := proto.Clone(request).(*UpdateRequest)
		redirectedRequest.ReplicaId = string(id)
		// TODO increment hops

		resp, err := c.Update(ctx, redirectedRequest, grpc.WaitForReady(true))
		if err != nil {
			return nil, err
		} else {
			return resp.Payload, nil
		}
	}
}

func (n *MonsteraNode) AppendEntries(replicaId string, request *hraft.AppendEntriesRequest) (*hraft.AppendEntriesResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.nodeState != READY {
		return nil, errNodeNotReady
	}

	b, ok := n.replicas[replicaId]
	if !ok {
		return nil, fmt.Errorf("no replica %s found on this node %s", replicaId, n.node.Id)
	}

	return b.AppendEntries(request)
}

func (n *MonsteraNode) RequestVote(replicaId string, request *hraft.RequestVoteRequest) (*hraft.RequestVoteResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.nodeState != READY {
		return nil, errNodeNotReady
	}

	b, ok := n.replicas[replicaId]
	if !ok {
		return nil, fmt.Errorf("no replica %s found on this node %s", replicaId, n.node.Id)
	}

	return b.RequestVote(request)
}

func (n *MonsteraNode) TimeoutNow(replicaId string, request *hraft.TimeoutNowRequest) (*hraft.TimeoutNowResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.nodeState != READY {
		return nil, errNodeNotReady
	}

	b, ok := n.replicas[replicaId]
	if !ok {
		return nil, fmt.Errorf("no replica %s found on this node %s", replicaId, n.node.Id)
	}

	return b.TimeoutNow(request)
}

func (n *MonsteraNode) InstallSnapshot(replicaId string, request *hraft.InstallSnapshotRequest, data io.Reader) (*hraft.InstallSnapshotResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.nodeState != READY {
		return nil, errNodeNotReady
	}

	b, ok := n.replicas[replicaId]
	if !ok {
		return nil, fmt.Errorf("no replica %s found on this node %s", replicaId, n.node.Id)
	}

	return b.InstallSnapshot(request, data)
}

func (n *MonsteraNode) ListCores() []*MonsteraReplica {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return lo.Values(n.replicas)
}

func (n *MonsteraNode) NodeState() MonsteraNodeState {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.nodeState
}

func (n *MonsteraNode) UpdateClusterConfig(newConfig *ClusterConfig) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	replicasToAdd := make(map[string]bool)
	replicasToRemove := make(map[string]bool)

	for _, nr := range n.replicas {
		replicasToRemove[nr.ReplicaId] = true
		for _, a := range newConfig.GetApplications() {
			for _, s := range a.GetShards() {
				for _, r := range s.GetReplicas() {
					if r.GetNodeId() == n.node.Id && r.Id == nr.ReplicaId {
						delete(replicasToRemove, nr.ReplicaId)
						goto found
					}
				}
			}
		}
	found:
	}

	for _, a := range newConfig.GetApplications() {
		for _, s := range a.GetShards() {
			for _, r := range s.GetReplicas() {
				if r.GetNodeId() == n.node.Id {
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

func (n *MonsteraNode) loadCores() error {
	for _, a := range n.clusterConfig.GetApplications() {
		for _, s := range a.GetShards() {
			for _, r := range s.GetReplicas() {
				if r.GetNodeId() == n.node.Id {
					// Find core descriptor
					coreDescriptor, ok := n.coreDescriptors[a.Implementation]
					if !ok {
						return fmt.Errorf("no core registered for %s", a.Name)
					}

					// Create replica
					applicationCore := coreDescriptor.CoreFactoryFunc(a, s, r)
					replica := NewMonsteraReplica(n.baseDir, a.Name, s.Id, r.Id, n.node.Address, applicationCore, n.pool, n.raftStore, coreDescriptor.RestoreSnapshotOnStart)

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
		if s.Replicas[0].NodeId == n.nodeId {
			// Bootstrap the shard if it's not bootstrapped yet
			if !r.IsBootstrapped() {
				// Add all replicas to the bootstrap list
				servers := make([]hraft.Server, len(s.Replicas))
				for i, r := range s.Replicas {
					n, err := n.clusterConfig.GetNode(r.NodeId)
					if err != nil {
						return err
					}
					servers[i] = hraft.Server{
						Suffrage: hraft.Voter,
						ID:       hraft.ServerID(r.Id),
						Address:  hraft.ServerAddress(n.Address),
					}
				}
				r.Bootstrap(servers)
			}
		}
	}

	return nil
}

func NewNode(baseDir string, nodeId string, clusterConfig *ClusterConfig, raftStore *BadgerStore) *MonsteraNode {
	monsteraNode := &MonsteraNode{
		baseDir:         baseDir,
		nodeId:          nodeId,
		coreDescriptors: make(map[string]*ApplicationCoreDescriptor),
		clusterConfig:   clusterConfig,
		nodeState:       INITIAL,
		replicas:        make(map[string]*MonsteraReplica),
		pool:            NewMonsteraConnectionPool(),
		raftStore:       raftStore,
	}

	return monsteraNode
}
