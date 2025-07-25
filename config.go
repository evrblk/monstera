package monstera

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"

	"errors"

	"github.com/samber/lo"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var (
	errNodeAlreadyExists        = errors.New("node already exists")
	errNodeNotFound             = errors.New("node not found")
	errApplicationNotFound      = errors.New("application not found")
	errShardNotFound            = errors.New("shard not found")
	errApplicationAlreadyExists = errors.New("application already exists")
)

// LoadConfigFromFile loads monstera cluster config from either a binary Protobuf `.pb` or a ProtoJSON `.json` file.
func LoadConfigFromFile(path string) (*ClusterConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	ext := filepath.Ext(path)
	if ext == ".pb" {
		return LoadConfigFromProto(data)
	} else if ext == ".json" {
		return LoadConfigFromJson(data)
	} else {
		return nil, fmt.Errorf("unsupported file extension: %s", ext)
	}
}

// LoadConfigFromProto loads binary serialized Protobuf monstera cluster config.
func LoadConfigFromProto(data []byte) (*ClusterConfig, error) {
	config := &ClusterConfig{}

	if err := proto.Unmarshal(data, config); err != nil {
		return nil, err
	}

	return LoadConfig(config.Applications, config.Nodes, config.UpdatedAt)
}

// LoadConfigFromJson loads JSON serialized monstera cluster config.
func LoadConfigFromJson(data []byte) (*ClusterConfig, error) {
	config := &ClusterConfig{}

	if err := protojson.Unmarshal(data, config); err != nil {
		return nil, err
	}

	return LoadConfig(config.Applications, config.Nodes, config.UpdatedAt)
}

// LoadConfig loads monstera cluster config from separate components.
func LoadConfig(applications []*Application, nodes []*Node, updatedAt int64) (*ClusterConfig, error) {
	config := &ClusterConfig{
		UpdatedAt:    updatedAt,
		Applications: applications,
		Nodes:        nodes,
	}

	err := config.Validate()
	if err != nil {
		return nil, err
	}

	return config, nil
}

// WriteConfigToFile writes monstera cluster config into either a binary Protobuf `.pb` or a ProtoJSON `.json` file.
func WriteConfigToFile(config *ClusterConfig, path string) error {
	ext := filepath.Ext(path)
	if ext == ".pb" {
		data, err := WriteConfigToProto(config)
		if err != nil {
			return err
		}
		return os.WriteFile(path, data, 0666)
	} else if ext == ".json" {
		data, err := WriteConfigToJson(config)
		if err != nil {
			return err

		}
		return os.WriteFile(path, data, 0666)
	} else {
		return fmt.Errorf("unsupported file extension: %s", ext)
	}
}

func WriteConfigToJson(config *ClusterConfig) ([]byte, error) {
	data, err := protojson.MarshalOptions{Indent: "  ", Multiline: true}.Marshal(config)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func WriteConfigToProto(config *ClusterConfig) ([]byte, error) {
	data, err := proto.Marshal(config)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func CreateEmptyConfig() *ClusterConfig {
	return &ClusterConfig{
		Applications: make([]*Application, 0),
		Nodes:        make([]*Node, 0),
		UpdatedAt:    time.Now().UnixMilli(),
	}
}

// Validate checks if the config is valid according to the following invariants:
//
// - UpdatedAt is not 0
// - There are at least 3 nodes
// - Nodes have non-empty id
// - Nodes have non-empty address
// - Nodes have unique ids
// - Applications have non-empty names
// - Applications have globally unique names
// - Applications have non-empty implementation
// - Applications have replication factor of at least 3
// - Shards have non-empty id
// - Shards have globally unique ids
// - Shards have globally unique global index prefixes
// - Shards have no overlap in range
// - Shards have 4 bytes ranges
// - All shards together cover the full range of keys
// - Number of replicas is greater or equal to replication factor
// - Replicas have non-empty id
// - Replicas have globally unique ids
// - Replicas are assigned to existing nodes
// - Replicas are assigned to different nodes
//
// Returns an error if any invariant is violated.
func (c *ClusterConfig) Validate() error {
	nodesByIds := make(map[string]*Node)

	if c.UpdatedAt == 0 {
		return fmt.Errorf("updated at is required")
	}

	if len(c.Nodes) < 3 {
		return fmt.Errorf("at least 3 nodes are required")
	}

	for _, n := range c.Nodes {
		if n.Address == "" {
			return fmt.Errorf("empty node address")
		}

		if n.Id == "" {
			return fmt.Errorf("empty node id")
		}

		_, ok := nodesByIds[n.Id]
		if ok {
			return fmt.Errorf("duplicate node id %s", n.Id)
		}

		nodesByIds[n.Id] = n
	}

	applicationsByNames := make(map[string]*Application)
	shardsByIds := make(map[string]*Shard)
	replicasByIds := make(map[string]*Replica)
	shardsByGlobalIndexPrefixes := make(map[string]*Shard)

	for _, a := range c.Applications {
		if a.Name == "" {
			return fmt.Errorf("empty application name")
		}

		if a.Implementation == "" {
			return fmt.Errorf("empty application implementation")
		}

		_, ok := applicationsByNames[a.Name]
		if ok {
			return fmt.Errorf("duplicate application name %s", a.Name)
		}
		applicationsByNames[a.Name] = a

		if a.ReplicationFactor < 3 {
			return fmt.Errorf("invalid replication factor for application %s", a.Name)
		}

		if len(a.Shards) == 0 {
			return fmt.Errorf("no shards for %s", a.Name)
		}

		for _, s := range a.Shards {
			if s.Id == "" {
				return fmt.Errorf("empty shard id")
			}

			_, ok := shardsByIds[s.Id]
			if ok {
				return fmt.Errorf("duplicate shard id %s", s.Id)
			}
			shardsByIds[s.Id] = s

			_, ok = shardsByGlobalIndexPrefixes[string(s.GlobalIndexPrefix)]
			if ok {
				return fmt.Errorf("duplicate global index prefix for shard %s", s.Id)
			}
			shardsByGlobalIndexPrefixes[string(s.GlobalIndexPrefix)] = s

			if len(s.Replicas) < int(a.ReplicationFactor) {
				return fmt.Errorf("not enough replicas for shard %s", s.Id)
			}

			if len(s.LowerBound) != 4 || len(s.UpperBound) != 4 {
				return fmt.Errorf("invalid lower bound/upper bounds for shard %s", s.Id)
			}

			if bytes.Compare(s.LowerBound, s.UpperBound) >= 0 {
				return fmt.Errorf("invalid lower bound/upper bounds for shard %s", s.Id)
			}

			for _, r := range s.Replicas {
				if r.Id == "" {
					return fmt.Errorf("empty replica id")
				}

				_, ok := replicasByIds[r.Id]
				if ok {
					return fmt.Errorf("duplicate replica id %s", r.Id)
				}
				replicasByIds[r.Id] = r

				_, ok = nodesByIds[r.NodeId]
				if !ok {
					return fmt.Errorf("node %s for replica %s not found", r.NodeId, r.Id)
				}
			}

			uniqueNodes := lo.UniqBy(s.Replicas, func(r *Replica) string {
				return r.NodeId
			})
			if len(uniqueNodes) < len(s.Replicas) {
				return fmt.Errorf("replicas are not assigned to different nodes for shard %s", s.Id)
			}
		}

		// Sort shards by LowerBound
		sortedShards := make([]*Shard, len(a.Shards))
		copy(sortedShards, a.Shards)
		sort.Slice(sortedShards, func(i, j int) bool {
			return bytes.Compare(sortedShards[i].LowerBound, sortedShards[j].LowerBound) < 0
		})

		// Check first LowerBound == 0x00000000
		if !bytes.Equal(sortedShards[0].LowerBound, []byte{0x00, 0x00, 0x00, 0x00}) {
			return fmt.Errorf("shards do not start at 0x00000000 for application %s", a.Name)
		}
		// Check last UpperBound == 0xffffffff
		if !bytes.Equal(sortedShards[len(sortedShards)-1].UpperBound, []byte{0xff, 0xff, 0xff, 0xff}) {
			return fmt.Errorf("shards do not end at 0xffffffff for application %s", a.Name)
		}
		// Check contiguous coverage
		for i := 1; i < len(sortedShards); i++ {
			prev := sortedShards[i-1]
			curr := sortedShards[i]
			// prev.UpperBound + 1 == curr.LowerBound
			prevUpper := binary.BigEndian.Uint32(prev.UpperBound)
			currLower := binary.BigEndian.Uint32(curr.LowerBound)
			if prevUpper+1 != currLower {
				return fmt.Errorf("shards are not contiguous between %x and %x for application %s", prev.UpperBound, curr.LowerBound, a.Name)
			}
		}
	}

	return nil
}

func (c *ClusterConfig) ListApplications() []*Application {
	return c.Applications
}

func (c *ClusterConfig) ListNodes() []*Node {
	return c.Nodes
}

func (c *ClusterConfig) CreateNode(address string) (*Node, error) {
	for _, n := range c.Nodes {
		if n.Address == address {
			return nil, errNodeAlreadyExists
		}
	}

	var id string
	for {
		id = generateId("nd")
		_, ok := lo.Find(c.Nodes, func(n *Node) bool {
			return n.Id == id
		})
		if !ok {
			break
		}
	}

	node := &Node{
		Id:      id,
		Address: address,
	}

	c.Nodes = append(c.Nodes, node)

	c.UpdatedAt = time.Now().UnixMilli()

	return node, nil
}

func (c *ClusterConfig) GetNode(nodeId string) (*Node, error) {
	node, ok := lo.Find(c.Nodes, func(n *Node) bool {
		return n.Id == nodeId
	})
	if !ok {
		return nil, errNodeNotFound
	}

	return node, nil
}

func (c *ClusterConfig) ListShards(applicationName string) ([]*Shard, error) {
	application, ok := lo.Find(c.Applications, func(a *Application) bool {
		return a.Name == applicationName
	})
	if !ok {
		return nil, errApplicationNotFound
	}

	return application.Shards, nil
}

func (c *ClusterConfig) CreateApplication(applicationName string, implementation string, replicationFactor int32) (*Application, error) {
	_, ok := lo.Find(c.Applications, func(a *Application) bool {
		return a.Name == applicationName
	})
	if ok {
		return nil, errApplicationAlreadyExists
	}

	application := &Application{
		Name:              applicationName,
		Implementation:    implementation,
		ReplicationFactor: replicationFactor,
	}

	c.Applications = append(c.Applications, application)

	c.UpdatedAt = time.Now().UnixMilli()

	return application, nil
}

func (c *ClusterConfig) CreateShard(applicationName string, lowerBound []byte, upperBound []byte, parentId string) (*Shard, error) {
	application, ok := lo.Find(c.Applications, func(a *Application) bool {
		return a.Name == applicationName
	})
	if !ok {
		return nil, errApplicationNotFound
	}

	if application.Shards == nil {
		application.Shards = make([]*Shard, 0)
	}

	var id string
	for {
		id = generateId("shrd")
		_, ok := lo.Find(application.Shards, func(s *Shard) bool {
			return s.Id == id
		})
		if !ok {
			break
		}
	}

	globalIndexPrefix := make([]byte, 8)
	for {
		binary.BigEndian.PutUint64(globalIndexPrefix[0:8], rand.Uint64())
		_, ok := lo.Find(application.Shards, func(s *Shard) bool {
			return bytes.Equal(s.GlobalIndexPrefix, globalIndexPrefix)
		})
		if !ok {
			break
		}
	}

	shard := &Shard{
		Id:                id,
		LowerBound:        lowerBound,
		UpperBound:        upperBound,
		GlobalIndexPrefix: globalIndexPrefix,
		ParentId:          parentId,
	}

	application.Shards = append(application.Shards, shard)

	c.UpdatedAt = time.Now().UnixMilli()

	return shard, nil
}

func (c *ClusterConfig) CreateReplica(applicationName string, shardId string, nodeId string) (*Replica, error) {
	application, ok := lo.Find(c.Applications, func(a *Application) bool {
		return a.Name == applicationName
	})
	if !ok {
		return nil, errApplicationNotFound
	}

	shard, ok := lo.Find(application.Shards, func(s *Shard) bool {
		return s.Id == shardId
	})
	if !ok {
		return nil, errShardNotFound
	}
	if shard.Replicas == nil {
		shard.Replicas = make([]*Replica, 0)
	}

	var id string
	for {
		id = generateId("rpl")
		_, ok := lo.Find(shard.Replicas, func(r *Replica) bool {
			return r.Id == id
		}) // TODO globally
		if !ok {
			break
		}
	}

	replica := &Replica{
		Id:     id,
		NodeId: nodeId,
	}
	shard.Replicas = append(shard.Replicas, replica)

	c.UpdatedAt = time.Now().UnixMilli()

	return replica, nil
}

func (c *ClusterConfig) FindShard(applicationName string, shardKey []byte) (*Shard, error) {
	application, ok := lo.Find(c.Applications, func(a *Application) bool {
		return a.Name == applicationName
	})
	if !ok {
		return nil, errApplicationNotFound
	}

	for _, shard := range application.Shards {
		// TODO check state
		if isWithinRange(shardKey, shard.LowerBound, shard.UpperBound) {
			return shard, nil
		}
	}

	return nil, errShardNotFound
}

func (c *ClusterConfig) GetShard(shardId string) (*Shard, error) {
	for _, a := range c.Applications {
		for _, s := range a.Shards {
			if s.Id == shardId {
				return s, nil
			}
		}
	}

	return nil, errShardNotFound
}

// ValidateTransition checks if the transition from old to new config is valid according to the following invariants:
//
//   - New nodes can be added, but existing nodes cannot be removed if they have at least one assigned replica in the
//     old config.
//   - New applications can be added, but existing applications cannot be removed.
//   - Shards cannot be removed or have their IDs, bounds, or unique global index prefixes changed.
//   - New replicas can be added (even exceeding the replication factor), but replicas cannot be both added and removed
//     in the same transition.
//   - All existing replicas must remain assigned to the same nodes (no reassignment of existing replicas).
//   - New config has newer UpdatedAt timestamp
//
// Returns an error if any invariant is violated.
func ValidateTransition(old, new *ClusterConfig) error {
	if new.UpdatedAt <= old.UpdatedAt {
		return fmt.Errorf("the new config must have newer UpdatedAt than the old config")
	}

	// New nodes can be added, but existing nodes cannot be removed
	// if they have at least one assigned replica in the old config
	oldNodes := make(map[string]*Node)
	for _, n := range old.Nodes {
		oldNodes[n.Id] = n
	}
	newNodes := make(map[string]*Node)
	for _, n := range new.Nodes {
		newNodes[n.Id] = n
	}

	// Find removed nodes
	for oldNodeId := range oldNodes {
		if _, exists := newNodes[oldNodeId]; !exists {
			// Check if this node had any replicas in the old config
			hadReplica := false
			for _, a := range old.Applications {
				for _, s := range a.Shards {
					for _, r := range s.Replicas {
						if r.NodeId == oldNodeId {
							hadReplica = true
							break
						}
					}
					if hadReplica {
						break
					}
				}
				if hadReplica {
					break
				}
			}
			if hadReplica {
				return fmt.Errorf("cannot remove node %s: it has assigned replicas in the old config", oldNodeId)
			}
		}
	}

	// New applications can be added, but existing cannot be removed
	oldApps := make(map[string]*Application)
	for _, a := range old.Applications {
		oldApps[a.Name] = a
	}
	newApps := make(map[string]*Application)
	for _, a := range new.Applications {
		newApps[a.Name] = a
	}
	for oldAppName := range oldApps {
		if _, exists := newApps[oldAppName]; !exists {
			return fmt.Errorf("cannot remove application %s", oldAppName)
		}
	}

	// Shards cannot be removed or have their IDs, bounds, or unique prefixes changed
	for appName, oldApp := range oldApps {
		newApp := newApps[appName]
		if newApp == nil {
			continue // already checked above
		}
		oldShards := make(map[string]*Shard)
		for _, s := range oldApp.Shards {
			oldShards[s.Id] = s
		}
		newShards := make(map[string]*Shard)
		for _, s := range newApp.Shards {
			newShards[s.Id] = s
		}
		for shardId, oldShard := range oldShards {
			newShard, exists := newShards[shardId]
			if !exists {
				return fmt.Errorf("cannot remove shard %s from application %s", shardId, appName)
			}
			if !bytes.Equal(oldShard.LowerBound, newShard.LowerBound) ||
				!bytes.Equal(oldShard.UpperBound, newShard.UpperBound) ||
				!bytes.Equal(oldShard.GlobalIndexPrefix, newShard.GlobalIndexPrefix) {
				return fmt.Errorf("cannot change bounds or global index prefix for shard %s in application %s", shardId, appName)
			}
		}
	}

	// New replicas can be added, but cannot add and remove in the same transition;
	// all existing replicas must remain assigned to the same nodes
	addedReplicas := 0
	removedReplicas := 0
	oldReplicaMap := make(map[string]*Replica) // key: app|shard|replica
	newReplicaMap := make(map[string]*Replica)
	for appName, oldApp := range oldApps {
		newApp := newApps[appName]
		if newApp == nil {
			continue
		}
		oldShards := make(map[string]*Shard)
		for _, s := range oldApp.Shards {
			oldShards[s.Id] = s
		}
		newShards := make(map[string]*Shard)
		for _, s := range newApp.Shards {
			newShards[s.Id] = s
		}
		for shardId, oldShard := range oldShards {
			newShard := newShards[shardId]
			if newShard == nil {
				continue
			}
			for _, oldReplica := range oldShard.Replicas {
				key := appName + "|" + shardId + "|" + oldReplica.Id
				oldReplicaMap[key] = oldReplica
			}
			for _, newReplica := range newShard.Replicas {
				key := appName + "|" + shardId + "|" + newReplica.Id
				newReplicaMap[key] = newReplica
			}
		}
	}
	// Check for removed and added replicas
	for key, oldReplica := range oldReplicaMap {
		newReplica, exists := newReplicaMap[key]
		if !exists {
			removedReplicas++
		} else {
			// Must be assigned to the same node
			if oldReplica.NodeId != newReplica.NodeId {
				return fmt.Errorf("replica %s changed node assignment: %s -> %s", key, oldReplica.NodeId, newReplica.NodeId)
			}
		}
	}
	for key := range newReplicaMap {
		if _, exists := oldReplicaMap[key]; !exists {
			addedReplicas++
		}
	}
	if addedReplicas > 0 && removedReplicas > 0 {
		return fmt.Errorf("cannot add and remove replicas in the same transition (added: %d, removed: %d)", addedReplicas, removedReplicas)
	}

	return nil
}

// generateId generates a random hex id
func generateId(prefix string) string {
	return fmt.Sprintf("%s_%x", prefix, rand.Uint32())
}

// isWithinRange checks if a key is within boundaries, left and right included
func isWithinRange(key []byte, lowerBound []byte, upperBound []byte) bool {
	return bytes.Compare(key, upperBound) <= 0 &&
		bytes.Compare(key, lowerBound) >= 0
}
