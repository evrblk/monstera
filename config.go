package monstera

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
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

func LoadConfigFromProto(data []byte) (*ClusterConfig, error) {
	config := &ClusterConfig{}

	if err := proto.Unmarshal(data, config); err != nil {
		return nil, err
	}

	return LoadConfig(config.Applications, config.Nodes, config.UpdatedAt)
}

func LoadConfigFromJson(data []byte) (*ClusterConfig, error) {
	config := &ClusterConfig{}

	if err := protojson.Unmarshal(data, config); err != nil {
		return nil, err
	}

	return LoadConfig(config.Applications, config.Nodes, config.UpdatedAt)
}

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

// - updatedAt is not 0
// - there are at least 3 nodes
// - nodes have non-empty id
// - nodes have non-empty address
// - nodes have unique ids
// - applications have non-empty names
// - applications have globally unique names
// - applications have non-empty implementation
// - applications have replication factor of at least 3
// - shards have non-empty id
// - shards have globally unique ids
// - shards have globally unique global index prefixes
// - shards have no overlap in range
// - shards have 4 bytes ranges
// - all shards together cover the full range of keys
// - number of replicas is greater or equal to replication factor
// - replicas have non-empty id
// - replicas have globally unique ids
// - replicas are assigned to existing nodes
// - replicas are assigned to different nodes
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

// generateId generates a random hex id
func generateId(prefix string) string {
	return fmt.Sprintf("%s_%x", prefix, rand.Uint32())
}

// isWithinRange checks if a key is within boundaries, left and right included
func isWithinRange(key []byte, lowerBound []byte, upperBound []byte) bool {
	return bytes.Compare(key, upperBound) <= 0 &&
		bytes.Compare(key, lowerBound) >= 0
}
