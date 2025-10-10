package monstera

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"

	"errors"

	"github.com/samber/lo"
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

	if err := json.Unmarshal(data, config); err != nil {
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
	data, err := json.MarshalIndent(config, "", "  ")
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
// - Nodes have non-empty address
// - Nodes have unique addresses
// - Applications have non-empty names
// - Applications have globally unique names
// - Applications have non-empty implementation
// - Applications have replication factor of at least 3
// - Shards have non-empty id
// - Shards have globally unique ids
// - Shards have no overlap in range
// - Shards have 4 bytes ranges
// - All shards together cover the full range of keys
// - Number of replicas is greater or equal to replication factor
// - Replicas have non-empty id
// - Replicas have globally unique ids
// - Replicas are assigned to existing nodes
// - Replicas are assigned to different nodes
// - Metadata has unique keys
//
// Returns an error if any invariant is violated.
func (c *ClusterConfig) Validate() error {
	nodesByAddress := make(map[string]*Node)

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

		_, ok := nodesByAddress[n.Address]
		if ok {
			return fmt.Errorf("duplicate node address %s", n.Address)
		}

		nodesByAddress[n.Address] = n

		err := validateMetadata(n.Metadata, fmt.Sprintf("node address %s", n.Address))
		if err != nil {
			return err
		}
	}

	applicationsByNames := make(map[string]*Application)
	shardsByIds := make(map[string]*Shard)
	replicasByIds := make(map[string]*Replica)

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

		err := validateMetadata(a.Metadata, fmt.Sprintf("application %s", a.Name))
		if err != nil {
			return err
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

			if len(s.Replicas) < int(a.ReplicationFactor) {
				return fmt.Errorf("not enough replicas for shard %s", s.Id)
			}

			if len(s.LowerBound) != 4 || len(s.UpperBound) != 4 {
				return fmt.Errorf("invalid lower bound/upper bounds for shard %s", s.Id)
			}

			if bytes.Compare(s.LowerBound, s.UpperBound) >= 0 {
				return fmt.Errorf("invalid lower bound/upper bounds for shard %s", s.Id)
			}

			err := validateMetadata(s.Metadata, fmt.Sprintf("shard %s", s.Id))
			if err != nil {
				return err
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

				_, ok = nodesByAddress[r.NodeAddress]
				if !ok {
					return fmt.Errorf("node %s for replica %s not found", r.NodeAddress, r.Id)
				}

				err := validateMetadata(r.Metadata, fmt.Sprintf("replica id %s", r.Id))
				if err != nil {
					return err
				}
			}

			uniqueNodes := lo.UniqBy(s.Replicas, func(r *Replica) string {
				return r.NodeAddress
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

func validateMetadata(metadata []*Metadata, parent string) error {
	metadataKeys := make(map[string]string)
	for _, m := range metadata {
		if _, ok := metadataKeys[m.Key]; ok {
			return fmt.Errorf("duplicate metadata key %s for %s", m.Key, parent)
		}
		metadataKeys[m.Key] = m.Value
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

	node := &Node{
		Address: address,
	}

	c.Nodes = append(c.Nodes, node)

	c.UpdatedAt = time.Now().UnixMilli()

	return node, nil
}

func (c *ClusterConfig) GetNode(nodeAddress string) (*Node, error) {
	node, ok := lo.Find(c.Nodes, func(n *Node) bool {
		return n.Address == nodeAddress
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

	shard := &Shard{
		Id:         id,
		LowerBound: lowerBound,
		UpperBound: upperBound,
		ParentId:   parentId,
	}

	application.Shards = append(application.Shards, shard)

	c.UpdatedAt = time.Now().UnixMilli()

	return shard, nil
}

func (c *ClusterConfig) CreateReplica(applicationName string, shardId string, nodeAddress string) (*Replica, error) {
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
		Id:          id,
		NodeAddress: nodeAddress,
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
		oldNodes[n.Address] = n
	}
	newNodes := make(map[string]*Node)
	for _, n := range new.Nodes {
		newNodes[n.Address] = n
	}

	// Find removed nodes
	for oldNode := range oldNodes {
		if _, exists := newNodes[oldNode]; !exists {
			// Check if this node had any replicas in the old config
			hadReplica := false
			for _, a := range old.Applications {
				for _, s := range a.Shards {
					for _, r := range s.Replicas {
						if r.NodeAddress == oldNode {
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
				return fmt.Errorf("cannot remove node %s: it has assigned replicas in the old config", oldNode)
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
				!bytes.Equal(oldShard.UpperBound, newShard.UpperBound) {
				return fmt.Errorf("cannot change bounds for shard %s in application %s", shardId, appName)
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
			if oldReplica.NodeAddress != newReplica.NodeAddress {
				return fmt.Errorf("replica %s changed node assignment: %s -> %s", key, oldReplica.NodeAddress, newReplica.NodeAddress)
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

// shardJsonProxy is used for human-readable Shard JSON representation, with HEX instead of Base64 for []byte
type shardJsonProxy struct {
	Id         string      `json:"id,omitempty"`
	LowerBound string      `json:"lower_bound,omitempty"`
	UpperBound string      `json:"upper_bound,omitempty"`
	ParentId   string      `json:"parent_id,omitempty"`
	Replicas   []*Replica  `json:"replicas,omitempty"`
	Metadata   []*Metadata `json:"metadata,omitempty"`
}

func (s *Shard) MarshalJSON() ([]byte, error) {
	i := len(s.LowerBound)
	for ; i > 0; i-- {
		if s.LowerBound[i-1] != 0x00 || s.UpperBound[i-1] != 0xff {
			break
		}
	}

	return json.Marshal(&shardJsonProxy{
		Id:         s.Id,
		LowerBound: hex.EncodeToString(s.LowerBound[:i]),
		UpperBound: hex.EncodeToString(s.UpperBound[:i]),
		ParentId:   s.ParentId,
		Replicas:   s.Replicas,
		Metadata:   s.Metadata,
	})
}

func (s *Shard) UnmarshalJSON(data []byte) error {
	var p shardJsonProxy

	err := json.Unmarshal(data, &p)
	if err != nil {
		return err
	}

	// Initialize with 0x00s
	s.LowerBound = []byte{0x00, 0x00, 0x00, 0x00}
	// Decode can rewrite less than 4 bytes leaving 0x00s in the end
	_, err = hex.Decode(s.LowerBound, []byte(p.LowerBound))
	if err != nil {
		return err
	}

	// Initialize with 0xffs
	s.UpperBound = []byte{0xff, 0xff, 0xff, 0xff}
	// Decode can rewrite less than 4 bytes leaving 0xffs in the end
	_, err = hex.Decode(s.UpperBound, []byte(p.UpperBound))
	if err != nil {
		return err
	}

	s.Id = p.Id
	s.ParentId = p.ParentId
	s.Replicas = p.Replicas
	s.Metadata = p.Metadata

	return nil
}
