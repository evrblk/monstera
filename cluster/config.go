// Package cluster models a monstera cluster's topology (nodes, applications,
// shards and replicas) as a Config, and provides loading, validation and
// mutation helpers for it.
//
// Concurrency: a Config is NOT safe for concurrent use. The mutating builder
// methods and the read methods must not run concurrently with each other on
// the same Config. Callers that share a Config across
// goroutines (e.g. a node serving requests while its config is updated) must
// provide their own synchronization.
//
// Ownership: the pointers and slices returned by the read methods alias the
// Config's internal state and must be treated as read-only. Mutating an entity
// mutates the Config and can violate its invariants.
package cluster

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sort"
)

var (
	errNodeAlreadyExists        = errors.New("node already exists")
	errNodeNotFound             = errors.New("node not found")
	errApplicationNotFound      = errors.New("application not found")
	errShardNotFound            = errors.New("shard not found")
	errReplicaNotFound          = errors.New("replica not found")
	errReplicaAlreadyExists     = errors.New("replica already exists")
	errApplicationAlreadyExists = errors.New("application already exists")
)

const (
	// KeyspacePerApplication holds the total size of an application's keyspace, currently 4 bytes.
	// It is used for shard bounds calculation.
	KeyspacePerApplication = 1 << 32
)

// LoadConfigFromFile loads monstera cluster config from either a binary Protobuf `.pb` or a JSON `.json` file.
func LoadConfigFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	ext := filepath.Ext(path)
	switch ext {
	case ".pb":
		return LoadConfigFromProto(data)
	case ".json":
		return LoadConfigFromJson(data)
	default:
		return nil, fmt.Errorf("unsupported file extension: %s", ext)
	}
}

// LoadConfigFromProto loads binary serialized Protobuf monstera cluster config.
func LoadConfigFromProto(data []byte) (*Config, error) {
	config := &Config{}

	if err := config.UnmarshalVT(data); err != nil {
		return nil, err
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}
	config.sortShards()

	return config, nil
}

// LoadConfigFromJson loads JSON serialized monstera cluster config.
func LoadConfigFromJson(data []byte) (*Config, error) {
	config := &Config{}

	if err := json.Unmarshal(data, config); err != nil {
		return nil, err
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}
	config.sortShards()

	return config, nil
}

// LoadConfig loads monstera cluster config from separate components.
func LoadConfig(applications []*Application, nodes []*Node, metadata []*Metadata, version int64) (*Config, error) {
	config := &Config{
		Version:      version,
		Applications: applications,
		Nodes:        nodes,
		Metadata:     metadata,
	}

	err := config.Validate()
	if err != nil {
		return nil, err
	}
	config.sortShards()

	return config, nil
}

// WriteConfigToFile writes monstera cluster config into either a binary Protobuf `.pb` or a JSON `.json` file.
//
// The write is atomic: the config is written to a temporary file in the same directory, fsynced, and renamed over
// the destination (with the directory fsynced afterwards), so a crash can never leave a torn or partially-written
// config. This makes it safe to use as the node's live, frequently-rewritten applied config.
func WriteConfigToFile(config *Config, path string) error {
	var data []byte
	var err error

	ext := filepath.Ext(path)
	switch ext {
	case ".pb":
		data, err = WriteConfigToProto(config)
	case ".json":
		data, err = WriteConfigToJson(config)
	default:
		return fmt.Errorf("unsupported file extension: %s", ext)
	}
	if err != nil {
		return err
	}

	return writeFileAtomic(path, data, 0666)
}

// writeFileAtomic writes data to path atomically: it writes to a temporary file in the same directory, fsyncs it,
// renames it over path, and fsyncs the directory so the rename is durable. The temporary file is on the same
// filesystem as path (same directory), which is what makes the rename atomic.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	// Best-effort cleanup: a no-op after a successful rename, removes the temp file on any error path.
	defer os.Remove(tmpName)

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpName, path); err != nil {
		return err
	}

	// fsync the directory so the rename entry itself survives a crash.
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func WriteConfigToJson(config *Config) ([]byte, error) {
	return json.MarshalIndent(config, "", "  ")
}

func WriteConfigToProto(config *Config) ([]byte, error) {
	return config.MarshalVT()
}

func CreateEmptyConfig() *Config {
	return &Config{
		Applications: make([]*Application, 0),
		Nodes:        make([]*Node, 0),
		Version:      1,
		Metadata:     make([]*Metadata, 0),
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
// - Metadata (at every level, including the config itself) has unique keys
//
// Returns an error if any invariant is violated.
func (c *Config) Validate() error {
	nodesById := make(map[string]*Node)
	nodesByGrpcAddress := make(map[string]*Node)

	if c.Version <= 0 {
		return fmt.Errorf("version must be greater than 0")
	}

	if err := validateMetadata(c.Metadata, "config"); err != nil {
		return err
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

// sortShards normalizes the config by sorting each application's shards by
// their lower bound in place. This establishes the invariant that
// FindShardByShardKey relies on for its binary search. It is called by the
// Load* functions after a successful Validate.
func (c *Config) sortShards() {
	for _, a := range c.Applications {
		sort.Slice(a.Shards, func(i, j int) bool {
			return bytes.Compare(a.Shards[i].LowerBound, a.Shards[j].LowerBound) < 0
		})
	}
}

func validateMetadata(metadata []*Metadata, parent string) error {
	metadataKeys := make(map[string]struct{})
	for _, m := range metadata {
		if _, ok := metadataKeys[m.Key]; ok {
			return fmt.Errorf("duplicate metadata key %s for %s", m.Key, parent)
		}
		metadataKeys[m.Key] = struct{}{}
	}
	return nil
}

func (c *Config) ListApplications() []*Application {
	return slices.Clone(c.Applications)
}

func (c *Config) ListNodes() []*Node {
	return slices.Clone(c.Nodes)
}

// getApplication returns the application with the given name, or
// errApplicationNotFound if there is none.
func (c *Config) getApplication(name string) (*Application, error) {
	for _, a := range c.Applications {
		if a.Name == name {
			return a, nil
		}
	}
	return nil, errApplicationNotFound
}

// findShard returns the shard with the given id within the application, or
// errShardNotFound if there is none.
func findShard(application *Application, shardId string) (*Shard, error) {
	for _, s := range application.Shards {
		if s.Id == shardId {
			return s, nil
		}
	}
	return nil, errShardNotFound
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
	application, err := c.getApplication(applicationName)
	if err != nil {
		return nil, err
	}

	sortedShards := make([]*Shard, len(application.Shards))
	copy(sortedShards, application.Shards)

	sort.Slice(sortedShards, func(i, j int) bool {
		return bytes.Compare(sortedShards[i].LowerBound, sortedShards[j].LowerBound) < 0
	})

	return sortedShards, nil
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

	return application, nil
}

func (c *Config) CreateShard(applicationName string, lowerBound []byte, upperBound []byte, parentId string) (*Shard, error) {
	application, err := c.getApplication(applicationName)
	if err != nil {
		return nil, err
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

	return shard, nil
}

func (c *Config) CreateReplica(applicationName string, shardId string, nodeId string) (*Replica, error) {
	application, err := c.getApplication(applicationName)
	if err != nil {
		return nil, err
	}

	shard, err := findShard(application, shardId)
	if err != nil {
		return nil, err
	}

	if shard.Replicas == nil {
		shard.Replicas = make([]*Replica, 0)
	}

	var id string
	for {
		id = generateId(shardId)
		// Regenerate on the (astronomically unlikely) chance of a collision with
		// an existing replica id anywhere in the config.
		if _, err := c.GetReplica(id); errors.Is(err, errReplicaNotFound) {
			break
		}
	}

	replica := &Replica{
		Id:     id,
		NodeId: nodeId,
	}
	shard.Replicas = append(shard.Replicas, replica)

	return replica, nil
}

// AddReplica appends a replica with a caller-provided id to a shard. Unlike
// CreateReplica, the id is not randomly generated. It fails if the
// application or shard does not exist, or if the replica id is already used
// anywhere in the config.
func (c *Config) AddReplica(applicationName string, shardId string, replicaId string, nodeId string) (*Replica, error) {
	application, err := c.getApplication(applicationName)
	if err != nil {
		return nil, err
	}

	shard, err := findShard(application, shardId)
	if err != nil {
		return nil, err
	}

	// Replica ids must be globally unique across the whole config.
	if _, err := c.GetReplica(replicaId); err == nil {
		return nil, errReplicaAlreadyExists
	}

	if shard.Replicas == nil {
		shard.Replicas = make([]*Replica, 0)
	}

	replica := &Replica{
		Id:     replicaId,
		NodeId: nodeId,
	}
	shard.Replicas = append(shard.Replicas, replica)

	return replica, nil
}

// Hash returns a stable content hash of the config: the hex-encoded SHA-256 of
// its canonical protobuf encoding. Stability relies on MarshalVT emitting fields
// in a deterministic order (the config uses only scalar and repeated fields, no maps).
func (c *Config) Hash() (string, error) {
	data, err := c.MarshalVT()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// FindShardByShardKey returns the shard whose [LowerBound, UpperBound] range
// contains shardKey. It assumes the application's shards are a sorted,
// contiguous, non-overlapping partition of the 4-byte keyspace (validated, and
// sorted by the Load* functions), and finds the shard with a binary search over
// the shard lower bounds.
//
// shardKey is compared byte-wise, so a key shorter than 4 bytes is treated as a
// prefix (padded conceptually with 0x00). A key longer than the 4-byte keyspace
// is rejected.
func (c *Config) FindShardByShardKey(applicationName string, shardKey []byte) (*Shard, error) {
	if len(shardKey) == 0 || len(shardKey) > 4 {
		return nil, fmt.Errorf("invalid shard key length %d: must be between 1 and 4 bytes", len(shardKey))
	}

	application, err := c.getApplication(applicationName)
	if err != nil {
		return nil, err
	}

	shards := application.Shards

	// shards are sorted by LowerBound (an invariant established by Validate).
	// Find the first shard whose LowerBound is strictly greater than shardKey;
	// the candidate containing shardKey is the one immediately before it.
	i := sort.Search(len(shards), func(i int) bool {
		return bytes.Compare(shards[i].LowerBound, shardKey) > 0
	})
	if i == 0 {
		return nil, errShardNotFound
	}
	candidate := shards[i-1]
	if bytes.Compare(shardKey, candidate.UpperBound) <= 0 {
		return candidate, nil
	}

	return nil, errShardNotFound
}

func (c *Config) GetShard(shardId string) (*Shard, error) {
	for _, a := range c.Applications {
		if s, err := findShard(a, shardId); err == nil {
			return s, nil
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

func (c *Config) IncrementVersion() {
	c.Version++
}

// ValidateTransition checks if the transition from old to new config is valid according to the following invariants:
//
//   - New nodes can be added, but existing nodes cannot be removed if they have at least one assigned replica in the
//     old config.
//   - New applications can be added, but existing applications cannot be removed.
//   - Active shards cannot be removed or have their bounds changed.
//   - New replicas can be added (even exceeding the replication factor), but replicas cannot be both added and removed
//     in the same transition.
//   - All existing replicas must remain assigned to the same nodes (no reassignment of existing replicas).
//   - The new config has a greater version than the old config.
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

	// Shards cannot be removed or have their bounds changed
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

	// Bounds are at most 4 bytes, i.e. at most 8 hex characters. Reject longer
	// strings explicitly: otherwise hex.Decode would write past the fixed-size
	// destination slice and panic.
	if len(p.LowerBound) > 8 {
		return fmt.Errorf("lower bound %q is too long: at most 8 hex characters", p.LowerBound)
	}
	if len(p.UpperBound) > 8 {
		return fmt.Errorf("upper bound %q is too long: at most 8 hex characters", p.UpperBound)
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

// ShortenBounds removes trailing 0x00 and 0xff from bounds. Returns slices backed by
// the same arrays passed as func arguments.
func ShortenBounds(lower, upper []byte) ([]byte, []byte) {
	i := len(lower)
	for ; i > 0; i-- {
		if lower[i-1] != 0x00 || upper[i-1] != 0xff {
			break
		}
	}
	return lower[:i], upper[:i]
}
