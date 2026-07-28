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
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
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

// WriteConfigToJson serializes the config to indented, human-readable JSON
// (shard bounds as hex, states as lowercase strings).
func WriteConfigToJson(config *Config) ([]byte, error) {
	return json.MarshalIndent(config, "", "  ")
}

// WriteConfigToProto serializes the config to its binary protobuf encoding.
func WriteConfigToProto(config *Config) ([]byte, error) {
	return config.MarshalVT()
}

// CreateEmptyConfig returns a new config at version 1 with no nodes or
// applications — the starting point for the builder methods (CreateNode,
// CreateApplication, ...). It does not pass Validate until populated.
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
//   - Version is greater than 0
//   - There are at least 3 nodes
//   - Nodes have non-empty id
//   - Nodes have unique ids
//   - Nodes have non-empty grpc address
//   - Nodes have unique grpc addresses
//   - Applications have non-empty names
//   - Applications have globally unique names
//   - Applications have non-empty implementation
//   - Applications have replication factor of at least 3
//   - Shards have non-empty id
//   - Shards have globally unique ids
//   - Shards have a known state (active, inactive, splitting or activating)
//   - Shards have non-degenerate ranges (LowerBound < UpperBound)
//   - Non-empty shard ParentId references an existing shard within the same application
//   - Active and splitting shards have no overlap in range
//   - All active and splitting shards together cover the full range of keys with no gaps
//   - Active shards have no children
//   - Inactive shards (retired after a completed split) have at least one child
//   - Splitting shards have 2 or more activating children (by ParentId) that exactly cover the parent's
//     range with no overlaps and no gaps; all their children are activating
//   - Activating children's replicas are on exactly the same nodes as their splitting parent's replicas
//     (split seeding is node-local)
//   - Activating shards are children of splitting shards and have no children
//   - Inactive and activating shards may overlap any other shards
//   - Number of replicas is greater or equal to replication factor
//   - Replicas have non-empty id
//   - Replicas have globally unique ids
//   - Replicas are assigned to existing nodes
//   - Replicas are assigned to different nodes
//   - Metadata (at every level, including the config itself) has unique keys
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

			if _, ok := ShardState_name[int32(s.State)]; !ok || s.State == ShardState_SHARD_STATE_INVALID {
				return fmt.Errorf("invalid state %d for shard %s", s.State, s.Id)
			}

			if len(s.Replicas) < int(a.ReplicationFactor) {
				return fmt.Errorf("not enough replicas for shard %s", s.Id)
			}

			if s.LowerBound >= s.UpperBound {
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

		// Active and splitting shards together must partition the whole
		// keyspace: no overlaps and no gaps. Inactive and activating shards
		// may overlap them.
		routableShards := make([]*Shard, 0, len(a.Shards))
		for _, s := range a.Shards {
			if s.IsRoutable() {
				routableShards = append(routableShards, s)
			}
		}
		if len(routableShards) == 0 {
			return fmt.Errorf("no active shards for %s", a.Name)
		}
		if err := validateContiguousCoverage(routableShards, 0x00000000, 0xffffffff); err != nil {
			return fmt.Errorf("%w for application %s", err, a.Name)
		}

		// A non-empty ParentId must reference an existing shard within the
		// same application; the parent/children rules below rely on it.
		appShardsById := make(map[string]*Shard, len(a.Shards))
		for _, s := range a.Shards {
			appShardsById[s.Id] = s
		}
		childrenByParentId := make(map[string][]*Shard)
		for _, s := range a.Shards {
			if s.ParentId == "" {
				continue
			}
			// A shard cannot be its own parent: self-parenting would otherwise
			// satisfy both "parent exists" and (for an inactive shard) "has
			// children", corrupting the split-lineage model.
			if s.ParentId == s.Id {
				return fmt.Errorf("shard %s is its own parent in application %s", s.Id, a.Name)
			}
			if _, ok := appShardsById[s.ParentId]; !ok {
				return fmt.Errorf("parent %s of shard %s not found in application %s", s.ParentId, s.Id, a.Name)
			}
			childrenByParentId[s.ParentId] = append(childrenByParentId[s.ParentId], s)
		}

		// Per-state parent/children rules:
		//   - active: serving, no children
		//   - inactive: retired after a completed split, has children
		//   - splitting: serving, has 2+ activating children exactly covering its range
		//   - activating: child of a splitting shard, no children
		for _, s := range a.Shards {
			children := childrenByParentId[s.Id]
			switch s.State {
			case ShardState_SHARD_STATE_ACTIVE:
				if len(children) > 0 {
					return fmt.Errorf("active shard %s must not have children", s.Id)
				}
			case ShardState_SHARD_STATE_INACTIVE:
				if len(children) == 0 {
					return fmt.Errorf("inactive shard %s must have children", s.Id)
				}
			case ShardState_SHARD_STATE_SPLITTING:
				for _, ch := range children {
					if ch.State != ShardState_SHARD_STATE_ACTIVATING {
						return fmt.Errorf("child %s of splitting shard %s must be activating", ch.Id, s.Id)
					}
				}
				if len(children) < 2 {
					return fmt.Errorf("splitting shard %s must have at least 2 activating children", s.Id)
				}
				if err := validateContiguousCoverage(children, s.LowerKey(), s.UpperKey()); err != nil {
					return fmt.Errorf("children of splitting shard %s do not cover its range: %w", s.Id, err)
				}
				// Split seeding is node-local: every node hosting a parent
				// replica seeds its own children, so the children's replicas
				// must live on exactly the same nodes as the parent's.
				parentNodes := replicaNodeSet(s.Replicas)
				for _, ch := range children {
					if !maps.Equal(parentNodes, replicaNodeSet(ch.Replicas)) {
						return fmt.Errorf("activating child %s must have replicas on the same nodes as its splitting parent %s", ch.Id, s.Id)
					}
				}
			case ShardState_SHARD_STATE_ACTIVATING:
				if len(children) > 0 {
					return fmt.Errorf("activating shard %s must not have children", s.Id)
				}
				parent := appShardsById[s.ParentId]
				if parent == nil || parent.State != ShardState_SHARD_STATE_SPLITTING {
					return fmt.Errorf("activating shard %s must be a child of a splitting shard", s.Id)
				}
			}
		}
	}

	return nil
}

// validateContiguousCoverage checks that shards exactly cover the range
// [lowerBound, upperBound] with no overlaps and no gaps. shards must be
// non-empty; the slice is not modified.
func validateContiguousCoverage(shards []*Shard, lowerBound ShardKey, upperBound ShardKey) error {
	// Sort shards by LowerBound
	sortedShards := make([]*Shard, len(shards))
	copy(sortedShards, shards)
	sort.Slice(sortedShards, func(i, j int) bool {
		return sortedShards[i].LowerBound < sortedShards[j].LowerBound
	})

	if sortedShards[0].LowerKey() != lowerBound {
		return fmt.Errorf("shards do not start at %s", lowerBound)
	}
	if sortedShards[len(sortedShards)-1].UpperKey() != upperBound {
		return fmt.Errorf("shards do not end at %s", upperBound)
	}
	// Check contiguous coverage
	for i := 1; i < len(sortedShards); i++ {
		prev := sortedShards[i-1]
		curr := sortedShards[i]
		if prev.UpperBound+1 != curr.LowerBound {
			return fmt.Errorf("shards are not contiguous between %s and %s", prev.UpperKey(), curr.LowerKey())
		}
	}
	return nil
}

// replicaNodeSet returns the set of node ids a shard's replicas are assigned to.
func replicaNodeSet(replicas []*Replica) map[string]struct{} {
	nodes := make(map[string]struct{}, len(replicas))
	for _, r := range replicas {
		nodes[r.NodeId] = struct{}{}
	}
	return nodes
}

// IsRoutable reports whether the shard currently serves its key range: it is
// either active or splitting (a splitting shard keeps serving until its
// activating children take over).
func (s *Shard) IsRoutable() bool {
	return s.State == ShardState_SHARD_STATE_ACTIVE || s.State == ShardState_SHARD_STATE_SPLITTING
}

// sortShards normalizes the config by sorting each application's shards by
// their lower bound in place (ties broken by shard id, since inactive shards
// may share a lower bound with the shard they overlap). This establishes the
// invariant that FindShardByShardKey relies on for its binary search, and
// keeps the order (and therefore Hash) stable across load/write cycles. It is
// called by the Load* functions after a successful Validate.
func (c *Config) sortShards() {
	for _, a := range c.Applications {
		sort.Slice(a.Shards, func(i, j int) bool {
			if a.Shards[i].LowerBound != a.Shards[j].LowerBound {
				return a.Shards[i].LowerBound < a.Shards[j].LowerBound
			}
			return a.Shards[i].Id < a.Shards[j].Id
		})
	}
}

// validateMetadata checks that metadata keys are unique; parent names the
// owning entity in the error message.
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

// ListApplications returns all applications in the config. The slice is a
// copy; the elements alias the config and must be treated as read-only.
func (c *Config) ListApplications() []*Application {
	return slices.Clone(c.Applications)
}

// ListNodes returns all nodes in the config. The slice is a copy; the
// elements alias the config and must be treated as read-only.
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

// CreateNode appends a new node to the config. It fails if a node with the
// same id already exists; other invariants (non-empty, unique gRPC address)
// are checked by Validate.
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

// GetNode returns the node with the given id, or an error if there is none.
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

// ListShards returns all shards of an application, in every state (including
// inactive parents and activating children — filter with Shard.IsRoutable for
// the serving set), sorted by lower bound. The slice is a copy; the elements
// alias the config and must be treated as read-only.
func (c *Config) ListShards(applicationName string) ([]*Shard, error) {
	application, err := c.getApplication(applicationName)
	if err != nil {
		return nil, err
	}

	sortedShards := make([]*Shard, len(application.Shards))
	copy(sortedShards, application.Shards)

	sort.Slice(sortedShards, func(i, j int) bool {
		if sortedShards[i].LowerBound != sortedShards[j].LowerBound {
			return sortedShards[i].LowerBound < sortedShards[j].LowerBound
		}
		return sortedShards[i].Id < sortedShards[j].Id
	})

	return sortedShards, nil
}

// CreateApplication appends a new application (with no shards yet) to the
// config. It fails if an application with the same name already exists.
// implementation names the registered application core that backs it.
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

// CreateShard appends a new active shard with the given bounds to the application.
func (c *Config) CreateShard(applicationName string, lowerBound ShardKey, upperBound ShardKey, parentId string) (*Shard, error) {
	application, err := c.getApplication(applicationName)
	if err != nil {
		return nil, err
	}

	if application.Shards == nil {
		application.Shards = make([]*Shard, 0)
	}

	if lowerBound >= upperBound {
		return nil, fmt.Errorf("invalid bounds for shard in application %s: lower bound must be less than upper bound", applicationName)
	}

	sl, su := ShortenBounds(lowerBound.Bytes(), upperBound.Bytes())
	id := fmt.Sprintf("%s_%x_%x", applicationName, sl, su)

	// Shard ids are globally unique. The id is derived from the bounds, so this
	// also rejects a duplicate-bounds shard in the same application.
	if _, err := c.GetShard(id); err == nil {
		return nil, fmt.Errorf("shard %s already exists", id)
	}

	shard := &Shard{
		Id:         id,
		LowerBound: uint32(lowerBound),
		UpperBound: uint32(upperBound),
		ParentId:   parentId,
		State:      ShardState_SHARD_STATE_ACTIVE,
	}

	application.Shards = append(application.Shards, shard)

	return shard, nil
}

// CreateReplica appends a replica with a freshly generated, globally unique id
// to a shard, assigned to the given node. See AddReplica for caller-provided ids.
func (c *Config) CreateReplica(applicationName string, shardId string, nodeId string) (*Replica, error) {
	application, err := c.getApplication(applicationName)
	if err != nil {
		return nil, err
	}

	shard, err := findShard(application, shardId)
	if err != nil {
		return nil, err
	}

	if _, err := c.GetNode(nodeId); err != nil {
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

	if _, err := c.GetNode(nodeId); err != nil {
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

// Key-based routing (resolving a shard key to its owning shard) lives on the
// Router type in the monstera package, not on Config: routing must not depend on
// the ordering of Config.Shards (a config applied over RPC is not normalized by
// the Load* functions), so the Router builds its own sorted index.

// GetShard returns the shard with the given id, searching every application,
// or an error if there is none.
func (c *Config) GetShard(shardId string) (*Shard, error) {
	for _, a := range c.Applications {
		if s, err := findShard(a, shardId); err == nil {
			return s, nil
		}
	}

	return nil, errShardNotFound
}

// GetReplica returns the replica with the given id, searching every shard of
// every application, or an error if there is none.
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

// IncrementVersion bumps the config version by one. Call it after a batch of
// mutations: ValidateTransition requires every applied config to have a
// strictly greater version than its predecessor.
func (c *Config) IncrementVersion() {
	c.Version++
}

// ValidateTransition checks if the transition from old to new config is valid according to the following invariants:
//
//   - New nodes can be added, but existing nodes cannot be removed if they have at least one assigned replica in the
//     old config.
//   - New applications can be added, but existing applications cannot be removed.
//   - Shards cannot be removed, have their bounds changed, or change their parent.
//   - Shard states follow the split lifecycle: an active shard can start splitting (there must be at least
//     2 activating children of it in the new config), a splitting shard can retire to inactive, and an
//     activating shard can become active; any other state change is forbidden. In particular, an active
//     shard cannot become inactive or activating directly, and an inactive shard never changes state again.
//   - A shard added to an existing application cannot be created active.
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

	// Shards cannot be removed, have their bounds changed, or change their
	// parent; state changes must follow the split lifecycle.
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
			if oldShard.LowerBound != newShard.LowerBound ||
				oldShard.UpperBound != newShard.UpperBound {
				return fmt.Errorf("cannot change bounds for shard %s in application %s", shardId, appName)
			}
			if oldShard.ParentId != newShard.ParentId {
				return fmt.Errorf("cannot change parent for shard %s in application %s", shardId, appName)
			}
			if err := validateShardStateTransition(oldShard, newShard, newApp); err != nil {
				return fmt.Errorf("%w in application %s", err, appName)
			}
		}
		// Shards cannot appear out of nowhere in a serving state: a shard
		// enters an existing application as an activating child of a split
		// and becomes active only through the activating state.
		for shardId, newShard := range newShards {
			if _, exists := oldShards[shardId]; !exists {
				if newShard.State == ShardState_SHARD_STATE_ACTIVE {
					return fmt.Errorf("new shard %s in application %s cannot be created active", shardId, appName)
				}
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
		for _, s := range oldApp.Shards {
			for _, r := range s.Replicas {
				oldReplicaMap[appName+"|"+s.Id+"|"+r.Id] = r
			}
		}
	}
	for appName, newApp := range newApps {
		for _, s := range newApp.Shards {
			for _, r := range s.Replicas {
				newReplicaMap[appName+"|"+s.Id+"|"+r.Id] = r
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

// validateShardStateTransition checks that an existing shard's state change
// between two config versions follows the split lifecycle:
// active -> splitting -> inactive for parents, activating -> active for
// children (a shard may also keep its state). An active shard may start
// splitting only if the new config already contains at least 2 activating
// children of it.
func validateShardStateTransition(oldShard, newShard *Shard, newApp *Application) error {
	allowed := false
	switch oldShard.State {
	case ShardState_SHARD_STATE_ACTIVE:
		switch newShard.State {
		case ShardState_SHARD_STATE_ACTIVE:
			allowed = true
		case ShardState_SHARD_STATE_SPLITTING:
			children := 0
			for _, ch := range newApp.Shards {
				if ch.ParentId == newShard.Id && ch.State == ShardState_SHARD_STATE_ACTIVATING {
					children++
				}
			}
			if children < 2 {
				return fmt.Errorf("shard %s cannot start splitting without at least 2 activating children", newShard.Id)
			}
			allowed = true
		}
	case ShardState_SHARD_STATE_SPLITTING:
		allowed = newShard.State == ShardState_SHARD_STATE_SPLITTING ||
			newShard.State == ShardState_SHARD_STATE_INACTIVE
	case ShardState_SHARD_STATE_ACTIVATING:
		allowed = newShard.State == ShardState_SHARD_STATE_ACTIVATING ||
			newShard.State == ShardState_SHARD_STATE_ACTIVE
	case ShardState_SHARD_STATE_INACTIVE:
		allowed = newShard.State == ShardState_SHARD_STATE_INACTIVE
	}
	if !allowed {
		return fmt.Errorf("invalid state transition for shard %s: %s -> %s",
			newShard.Id, shardStateName(oldShard.State), shardStateName(newShard.State))
	}
	return nil
}

// shardStateName returns the human-readable name of a shard state, as used in
// the JSON representation and error messages.
func shardStateName(state ShardState) string {
	switch state {
	case ShardState_SHARD_STATE_ACTIVE:
		return "active"
	case ShardState_SHARD_STATE_INACTIVE:
		return "inactive"
	case ShardState_SHARD_STATE_SPLITTING:
		return "splitting"
	case ShardState_SHARD_STATE_ACTIVATING:
		return "activating"
	default:
		return fmt.Sprintf("invalid(%d)", state)
	}
}

// generateId generates a random hex id
func generateId(prefix string) string {
	return fmt.Sprintf("%s_%x", prefix, rand.Uint32())
}

// shardJsonProxy is used for human-readable Shard JSON representation, with HEX instead of Base64 for []byte
// and a lowercase string for the state ("active", "inactive", "splitting", "activating"). The state is required.
type shardJsonProxy struct {
	Id         string      `json:"id,omitempty"`
	LowerBound string      `json:"lower_bound,omitempty"`
	UpperBound string      `json:"upper_bound,omitempty"`
	ParentId   string      `json:"parent_id,omitempty"`
	State      string      `json:"state,omitempty"`
	Replicas   []*Replica  `json:"replicas,omitempty"`
	Metadata   []*Metadata `json:"metadata,omitempty"`
}

// MarshalJSON implements json.Marshaler using the human-readable form (hex
// bounds shortened via ShortenBounds, lowercase state names).
func (s *Shard) MarshalJSON() ([]byte, error) {
	sl, su := ShortenBounds(s.LowerKey().Bytes(), s.UpperKey().Bytes())

	var state string
	switch s.State {
	case ShardState_SHARD_STATE_ACTIVE:
		state = "active"
	case ShardState_SHARD_STATE_INACTIVE:
		state = "inactive"
	case ShardState_SHARD_STATE_SPLITTING:
		state = "splitting"
	case ShardState_SHARD_STATE_ACTIVATING:
		state = "activating"
	default:
		return nil, fmt.Errorf("invalid state %d for shard %s", s.State, s.Id)
	}

	return json.Marshal(&shardJsonProxy{
		Id:         s.Id,
		LowerBound: hex.EncodeToString(sl),
		UpperBound: hex.EncodeToString(su),
		ParentId:   s.ParentId,
		State:      state,
		Replicas:   s.Replicas,
		Metadata:   s.Metadata,
	})
}

// UnmarshalJSON implements json.Unmarshaler for the human-readable form
// produced by MarshalJSON. Shortened hex bounds are re-padded: lower bounds
// with 0x00, upper bounds with 0xff.
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

	// Initialize with 0x00s: Decode can rewrite less than 4 bytes, leaving
	// 0x00s in the end (shortened lower bounds pad with zeros).
	lower := []byte{0x00, 0x00, 0x00, 0x00}
	if _, err := hex.Decode(lower, []byte(p.LowerBound)); err != nil {
		return err
	}
	s.LowerBound = binary.BigEndian.Uint32(lower)

	// Initialize with 0xffs: shortened upper bounds pad with 0xff.
	upper := []byte{0xff, 0xff, 0xff, 0xff}
	if _, err := hex.Decode(upper, []byte(p.UpperBound)); err != nil {
		return err
	}
	s.UpperBound = binary.BigEndian.Uint32(upper)

	switch p.State {
	case "active":
		s.State = ShardState_SHARD_STATE_ACTIVE
	case "inactive":
		s.State = ShardState_SHARD_STATE_INACTIVE
	case "splitting":
		s.State = ShardState_SHARD_STATE_SPLITTING
	case "activating":
		s.State = ShardState_SHARD_STATE_ACTIVATING
	case "":
		return fmt.Errorf("missing state for shard %s", p.Id)
	default:
		return fmt.Errorf("unknown state %q for shard %s", p.State, p.Id)
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
