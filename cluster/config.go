package cluster

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
)

var (
	errNodeAlreadyExists        = errors.New("node already exists")
	errNodeNotFound             = errors.New("node not found")
	errApplicationNotFound      = errors.New("application not found")
	errShardNotFound            = errors.New("shard not found")
	errReplicaNotFound          = errors.New("replica not found")
	errApplicationAlreadyExists = errors.New("application already exists")
)

const (
	// KeyspacePerApplication holds the total size of an application's keyspace, currently 4 bytes.
	// It is used for shard bounds calculation.
	KeyspacePerApplication = 1 << 32
)

// LoadConfigFromFile loads monstera cluster config from either a binary Protobuf `.pb` or a ProtoJSON `.json` file.
func LoadConfigFromFile(path string) (*Config, error) {
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
func LoadConfigFromProto(data []byte) (*Config, error) {
	config := &Config{}

	if err := config.UnmarshalVT(data); err != nil {
		return nil, err
	}

	return LoadConfig(config.Applications, config.Nodes, config.Version)
}

// LoadConfigFromJson loads JSON serialized monstera cluster config.
func LoadConfigFromJson(data []byte) (*Config, error) {
	config := &Config{}

	if err := json.Unmarshal(data, config); err != nil {
		return nil, err
	}

	return LoadConfig(config.Applications, config.Nodes, config.Version)
}

// LoadConfig loads monstera cluster config from separate components.
func LoadConfig(applications []*Application, nodes []*Node, version int64) (*Config, error) {
	config := &Config{
		Version:      version,
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
func WriteConfigToFile(config *Config, path string) error {
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

func WriteConfigToJson(config *Config) ([]byte, error) {
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return nil, err
	}

	return data, nil
}

func WriteConfigToProto(config *Config) ([]byte, error) {
	data, err := config.MarshalVT()
	if err != nil {
		return nil, err
	}

	return data, nil
}

func CreateEmptyConfig() *Config {
	return &Config{
		Applications: make([]*Application, 0),
		Nodes:        make([]*Node, 0),
		Version:      1,
	}
}

// Validate checks if the config is valid according to the following invariants:
//
// - Version is greater than 0
// - There are at least 3 nodes
// - Nodes have non-empty id
// - Nodes have unique ids
// - Nodes have non-empty grpc address
// - Nodes have unique grpc addresses
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
func (c *Config) Validate() error {
	nodesById := make(map[string]*Node)
	nodesByGrpcAddress := make(map[string]*Node)

	if c.Version <= 0 {
		return fmt.Errorf("version must be greater than 0")
	}

	if len(c.Nodes) < 3 {
		return fmt.Errorf("at least 3 nodes are required")
	}

	for _, n := range c.Nodes {
		if n.Id == "" {
			return fmt.Errorf("empty node id")
		}

		if n.GrpcAddress == "" {
			return fmt.Errorf("empty node grpc address")
		}

		_, ok := nodesById[n.Id]
		if ok {
			return fmt.Errorf("duplicate node id %s", n.Id)
		}
		nodesById[n.Id] = n

		_, ok = nodesByGrpcAddress[n.GrpcAddress]
		if ok {
			return fmt.Errorf("duplicate node grpc address %s", n.GrpcAddress)
		}
		nodesByGrpcAddress[n.GrpcAddress] = n

		err := validateMetadata(n.Metadata, fmt.Sprintf("node id %s", n.Id))
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

				_, ok = nodesById[r.NodeId]
				if !ok {
					return fmt.Errorf("node %s for replica %s not found", r.NodeId, r.Id)
				}

				err := validateMetadata(r.Metadata, fmt.Sprintf("replica id %s", r.Id))
				if err != nil {
					return err
				}
			}

			uniqueNodes := make(map[string]struct{}, len(s.Replicas))
			for _, n := range s.Replicas {
				uniqueNodes[n.NodeId] = struct{}{}
			}
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

func (c *Config) ListApplications() []*Application {
	return c.Applications
}

func (c *Config) ListNodes() []*Node {
	return c.Nodes
}

func (c *Config) CreateNode(id string, grpcAddress string) (*Node, error) {
	for _, n := range c.Nodes {
		if n.Id == id {
			return nil, errNodeAlreadyExists
		}
	}

	node := &Node{
		Id:          id,
		GrpcAddress: grpcAddress,
	}

	c.Nodes = append(c.Nodes, node)

	c.Version++ // TODO

	return node, nil
}

func (c *Config) GetNode(nodeId string) (*Node, error) {
	var node *Node
	found := false
	for _, n := range c.Nodes {
		if n.Id == nodeId {
			node = n
			found = true
			break
		}
	}
	if !found {
		return nil, errNodeNotFound
	}

	return node, nil
}

func (c *Config) ListShards(applicationName string) ([]*Shard, error) {
	var application *Application
	found := false
	for _, a := range c.Applications {
		if a.Name == applicationName {
			application = a
			found = true
			break
		}
	}
	if !found {
		return nil, errApplicationNotFound
	}

	return application.Shards, nil
}

func (c *Config) CreateApplication(applicationName string, implementation string, replicationFactor int32) (*Application, error) {
	for _, a := range c.Applications {
		if a.Name == applicationName {
			return nil, errApplicationAlreadyExists
		}
	}

	application := &Application{
		Name:              applicationName,
		Implementation:    implementation,
		ReplicationFactor: replicationFactor,
	}

	c.Applications = append(c.Applications, application)

	c.Version++ // TODO

	return application, nil
}

func (c *Config) CreateShard(applicationName string, lowerBound []byte, upperBound []byte, parentId string) (*Shard, error) {
	var application *Application
	found := false
	for _, a := range c.Applications {
		if a.Name == applicationName {
			application = a
			found = true
			break
		}
	}
	if !found {
		return nil, errApplicationNotFound
	}

	if application.Shards == nil {
		application.Shards = make([]*Shard, 0)
	}

	sl, su := ShortenBounds(lowerBound, upperBound)
	id := fmt.Sprintf("%s_%x_%x", applicationName, sl, su)

	shard := &Shard{
		Id:         id,
		LowerBound: lowerBound,
		UpperBound: upperBound,
		ParentId:   parentId,
	}

	application.Shards = append(application.Shards, shard)

	c.Version++ // TODO

	return shard, nil
}

func (c *Config) CreateReplica(applicationName string, shardId string, nodeId string) (*Replica, error) {
	var application *Application
	found := false
	for _, a := range c.Applications {
		if a.Name == applicationName {
			application = a
			found = true
			break
		}
	}
	if !found {
		return nil, errApplicationNotFound
	}

	var shard *Shard
	found = false
	for _, s := range application.Shards {
		if s.Id == shardId {
			shard = s
			found = true
			break
		}
	}
	if !found {
		return nil, errShardNotFound
	}

	if shard.Replicas == nil {
		shard.Replicas = make([]*Replica, 0)
	}

	var id string
	for {
		id = generateId(shardId)
		for _, a := range c.Applications {
			for _, s := range a.Shards {
				for _, r := range s.Replicas {
					if r.Id == id {
						continue // try generating again
					}
				}
			}
		}

		break // did not find such id
	}

	replica := &Replica{
		Id:     id,
		NodeId: nodeId,
	}
	shard.Replicas = append(shard.Replicas, replica)

	c.Version++ // TODO

	return replica, nil
}

func (c *Config) FindShardByShardKey(applicationName string, shardKey []byte) (*Shard, error) {
	var application *Application
	found := false
	for _, a := range c.Applications {
		if a.Name == applicationName {
			application = a
			found = true
			break
		}
	}
	if !found {
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

func (c *Config) GetShard(shardId string) (*Shard, error) {
	for _, a := range c.Applications {
		for _, s := range a.Shards {
			if s.Id == shardId {
				return s, nil
			}
		}
	}

	return nil, errShardNotFound
}

func (c *Config) GetReplica(replicaId string) (*Replica, error) {
	for _, a := range c.Applications {
		for _, s := range a.Shards {
			for _, r := range s.Replicas {
				if r.Id == replicaId {
					return r, nil
				}
			}
		}
	}

	return nil, errReplicaNotFound
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
func ValidateTransition(old, new *Config) error {
	if new.Version <= old.Version {
		return fmt.Errorf("the new config must have newer version than the old config")
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
	for oldNode := range oldNodes {
		if _, exists := newNodes[oldNode]; !exists {
			// Check if this node had any replicas in the old config
			hadReplica := false
			for _, a := range old.Applications {
				for _, s := range a.Shards {
					for _, r := range s.Replicas {
						if r.NodeId == oldNode {
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
	sl, su := ShortenBounds(s.LowerBound, s.UpperBound)

	return json.Marshal(&shardJsonProxy{
		Id:         s.Id,
		LowerBound: hex.EncodeToString(sl),
		UpperBound: hex.EncodeToString(su),
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

func ShortenBounds(lower, upper []byte) ([]byte, []byte) {
	i := len(lower)
	for ; i > 0; i-- {
		if lower[i-1] != 0x00 || upper[i-1] != 0xff {
			break
		}
	}
	return lower[:i], upper[:i]
}
