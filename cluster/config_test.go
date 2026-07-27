package cluster

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestConfig_Builder(t *testing.T) {
	clusterConfig := CreateEmptyConfig()

	n1, err := clusterConfig.CreateNode("node_1", "localhost:9001")
	require.NoError(t, err)
	require.NotNil(t, n1)

	n2, err := clusterConfig.CreateNode("node_2", "localhost:9002")
	require.NoError(t, err)
	require.NotNil(t, n2)

	n3, err := clusterConfig.CreateNode("node_3", "localhost:9003")
	require.NoError(t, err)
	require.NotNil(t, n3)

	a, err := clusterConfig.CreateApplication("Core", "Core", 3)
	require.NoError(t, err)
	require.NotNil(t, a)

	s1, err := clusterConfig.CreateShard(a.Name, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, "")
	require.NoError(t, err)
	require.NotNil(t, s1)

	s2, err := clusterConfig.CreateShard(a.Name, []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, "")
	require.NoError(t, err)
	require.NotNil(t, s2)

	r1, err := clusterConfig.CreateReplica(a.Name, s1.Id, n1.Id)
	require.NoError(t, err)
	require.NotNil(t, r1)

	r2, err := clusterConfig.CreateReplica(a.Name, s1.Id, n2.Id)
	require.NoError(t, err)
	require.NotNil(t, r2)

	r3, err := clusterConfig.CreateReplica(a.Name, s1.Id, n3.Id)
	require.NoError(t, err)
	require.NotNil(t, r3)

	r4, err := clusterConfig.CreateReplica(a.Name, s2.Id, n1.Id)
	require.NoError(t, err)
	require.NotNil(t, r4)

	r5, err := clusterConfig.CreateReplica(a.Name, s2.Id, n2.Id)
	require.NoError(t, err)
	require.NotNil(t, r5)

	r6, err := clusterConfig.CreateReplica(a.Name, s2.Id, n3.Id)
	require.NoError(t, err)
	require.NotNil(t, r6)

	err = clusterConfig.Validate()
	require.NoError(t, err)
}

func TestConfig_Validate(t *testing.T) {
	// Test valid configuration
	t.Run("valid configuration", func(t *testing.T) {
		validConfig := &Config{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:         "shrd_01",
							LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound: []byte{0x7f, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "node_1"},
								{Id: "rpl_02", NodeId: "node_2"},
								{Id: "rpl_03", NodeId: "node_3"},
							},
						},
						{
							Id:         "shrd_02",
							LowerBound: []byte{0x80, 0x00, 0x00, 0x00},
							UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_04", NodeId: "node_1"},
								{Id: "rpl_05", NodeId: "node_2"},
								{Id: "rpl_06", NodeId: "node_3"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := validConfig.Validate()
		require.NoError(t, err)
	})

	// Test node validations
	t.Run("empty node id", func(t *testing.T) {
		config := &Config{
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty node id")
	})

	t.Run("duplicate node id", func(t *testing.T) {
		config := &Config{
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_1", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate node id")
	})

	t.Run("empty node address", func(t *testing.T) {
		config := &Config{
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: ""},
			},
			Version: 1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty node grpc address")
	})

	t.Run("duplicate node address", func(t *testing.T) {
		config := &Config{
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: "localhost:9001"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate node grpc address")
	})

	// Test application validations
	t.Run("empty application name", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{
				{Name: "", Implementation: "test.impl", ReplicationFactor: 3},
			},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty application name")
	})

	t.Run("empty application implementation", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{
				{Name: "test.app", Implementation: "", ReplicationFactor: 3},
			},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty application implementation")
	})

	t.Run("duplicate application name", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{
				{Name: "test.app", Implementation: "test.impl", ReplicationFactor: 3, Shards: []*Shard{
					{
						Id:         "shrd_01",
						LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
						UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
						State:      ShardState_SHARD_STATE_ACTIVE,
						Replicas: []*Replica{
							{Id: "rpl_01", NodeId: "node_1"},
							{Id: "rpl_02", NodeId: "node_2"},
							{Id: "rpl_03", NodeId: "node_3"},
						}},
				}},
				{Name: "test.app", Implementation: "test.impl2", ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:         "shrd_01",
							LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "node_1"},
								{Id: "rpl_02", NodeId: "node_2"},
								{Id: "rpl_03", NodeId: "node_3"},
							}},
					}},
			},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate application name")
	})

	t.Run("invalid replication factor", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{
				{Name: "test.app", Implementation: "test.impl", ReplicationFactor: 2},
			},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid replication factor")
	})

	// Test shard validations
	t.Run("empty shard id", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:         "",
							LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "node_1"},
								{Id: "rpl_02", NodeId: "node_2"},
								{Id: "rpl_03", NodeId: "node_3"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty shard id")
	})

	t.Run("duplicate shard id", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:         "shrd_01",
							LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound: []byte{0x7f, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "node_1"},
								{Id: "rpl_02", NodeId: "node_2"},
								{Id: "rpl_03", NodeId: "node_3"},
							},
						},
						{
							Id:         "shrd_01",
							LowerBound: []byte{0x80, 0x00, 0x00, 0x00},
							UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_04", NodeId: "node_1"},
								{Id: "rpl_05", NodeId: "node_2"},
								{Id: "rpl_06", NodeId: "node_3"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate shard id")
	})

	t.Run("not enough replicas", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:         "shrd_01",
							LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "node_1"},
								{Id: "rpl_02", NodeId: "node_2"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "not enough replicas")
	})

	t.Run("invalid bounds length", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:         "shrd_01",
							LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound: []byte{0x7f, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "node_1"},
								{Id: "rpl_02", NodeId: "node_2"},
								{Id: "rpl_03", NodeId: "node_3"},
							},
						},
						{
							Id:         "shrd_02",
							LowerBound: []byte{0x80, 0x00, 0x00},
							UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "node_1"},
								{Id: "rpl_02", NodeId: "node_2"},
								{Id: "rpl_03", NodeId: "node_3"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid lower bound/upper bounds")
	})

	t.Run("invalid bounds order", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:         "shrd_01",
							LowerBound: []byte{0xff, 0xff, 0xff, 0xff},
							UpperBound: []byte{0x00, 0x00, 0x00, 0x00},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "node_1"},
								{Id: "rpl_02", NodeId: "node_2"},
								{Id: "rpl_03", NodeId: "node_3"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid lower bound/upper bounds")
	})

	// Test replica validations
	t.Run("empty replica id", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:         "shrd_01",
							LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "", NodeId: "node_1"},
								{Id: "rpl_02", NodeId: "node_2"},
								{Id: "rpl_03", NodeId: "node_3"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty replica id")
	})

	t.Run("duplicate replica id", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:         "shrd_01",
							LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "node_1"},
								{Id: "rpl_01", NodeId: "node_2"},
								{Id: "rpl_03", NodeId: "node_3"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate replica id")
	})

	t.Run("replica assigned to nonexistent node", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:         "shrd_01",
							LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "node_1"},
								{Id: "rpl_02", NodeId: "node_2"},
								{Id: "rpl_03", NodeId: "node_4"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "node node_4 for replica rpl_03 not found")
	})

	t.Run("replicas assigned to same node", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:         "shrd_01",
							LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "node_1"},
								{Id: "rpl_02", NodeId: "node_1"},
								{Id: "rpl_03", NodeId: "node_3"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "replicas are not assigned to different nodes")
	})

	// Test multiple applications with shards
	t.Run("multiple applications with valid shards", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{
				{
					Name:              "app1",
					Implementation:    "impl1",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:         "shrd_01",
							LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "node_1"},
								{Id: "rpl_02", NodeId: "node_2"},
								{Id: "rpl_03", NodeId: "node_3"},
							},
						},
					},
				},
				{
					Name:              "app2",
					Implementation:    "impl2",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:         "shrd_02",
							LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_04", NodeId: "node_1"},
								{Id: "rpl_05", NodeId: "node_2"},
								{Id: "rpl_06", NodeId: "node_3"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.NoError(t, err)
	})

	// Test edge cases
	t.Run("minimum valid replication factor", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:         "shrd_01",
							LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "node_1"},
								{Id: "rpl_02", NodeId: "node_2"},
								{Id: "rpl_03", NodeId: "node_3"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.NoError(t, err)
	})

	t.Run("exact replica count", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:         "shrd_01",
							LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "node_1"},
								{Id: "rpl_02", NodeId: "node_2"},
								{Id: "rpl_03", NodeId: "node_3"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.NoError(t, err)
	})

	t.Run("empty applications and nodes", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{},
			Nodes:        []*Node{},
			Version:      1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least 3 nodes are required")
	})

	// Test minimum node requirement
	t.Run("insufficient nodes - 0 nodes", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{},
			Nodes:        []*Node{},
			Version:      1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least 3 nodes are required")
	})

	t.Run("insufficient nodes - 2 nodes", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least 3 nodes are required")
	})

	t.Run("sufficient nodes - 3 nodes", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.NoError(t, err)
	})

	t.Run("sufficient nodes - more than 3 nodes", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: "localhost:9003"},
				{Id: "node_4", GrpcAddress: "localhost:9004"},
				{Id: "node_5", GrpcAddress: "localhost:9005"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.NoError(t, err)
	})

	t.Run("application with no shards", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards:            []*Shard{},
				},
			},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_3", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "no shards for test.app")
	})

	t.Run("duplicate keys in metadata", func(t *testing.T) {
		config := &Config{
			Applications: []*Application{},
			Nodes: []*Node{
				{
					Id: "node_1", GrpcAddress: "localhost:9001",
					Metadata: []*Metadata{
						{Key: "key1", Value: "value1"},
						{Key: "key1", Value: "value2"},
					},
				},
				{
					Id: "node_2", GrpcAddress: "localhost:9002",
					Metadata: []*Metadata{
						{Key: "key1", Value: "value1"},
						{Key: "key2", Value: "value2"},
					},
				},
				{Id: "node_3", GrpcAddress: "localhost:9003"},
			},
			Version: 1,
		}

		err := config.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate metadata key key1 for node id node_1")
	})
}

func TestConfig_Validate_ShardCoverage(t *testing.T) {
	baseNodes := []*Node{
		{Id: "node_1", GrpcAddress: "localhost:9001"},
		{Id: "node_2", GrpcAddress: "localhost:9002"},
		{Id: "node_3", GrpcAddress: "localhost:9003"},
	}

	t.Run("valid contiguous full coverage", func(t *testing.T) {
		cfg := &Config{
			Applications: []*Application{
				{
					Name:              "app",
					Implementation:    "impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:         "shrd_01",
							LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound: []byte{0x7f, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "node_1"},
								{Id: "rpl_02", NodeId: "node_2"},
								{Id: "rpl_03", NodeId: "node_3"},
							},
						},
						{
							Id:         "shrd_02",
							LowerBound: []byte{0x80, 0x00, 0x00, 0x00},
							UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_04", NodeId: "node_1"},
								{Id: "rpl_05", NodeId: "node_2"},
								{Id: "rpl_06", NodeId: "node_3"},
							},
						},
					},
				},
			},
			Nodes:   baseNodes,
			Version: 1,
		}
		err := cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("gap between shards", func(t *testing.T) {
		cfg := &Config{
			Applications: []*Application{
				{
					Name:              "app",
					Implementation:    "impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:         "shrd_01",
							LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound: []byte{0x7f, 0xff, 0xff, 0xfe}, // gap after this
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "node_1"},
								{Id: "rpl_02", NodeId: "node_2"},
								{Id: "rpl_03", NodeId: "node_3"},
							},
						},
						{
							Id:         "shrd_02",
							LowerBound: []byte{0x80, 0x00, 0x00, 0x00},
							UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_04", NodeId: "node_1"},
								{Id: "rpl_05", NodeId: "node_2"},
								{Id: "rpl_06", NodeId: "node_3"},
							},
						},
					},
				},
			},
			Nodes:   baseNodes,
			Version: 1,
		}
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "shards are not contiguous")
	})

	t.Run("overlap between shards", func(t *testing.T) {
		cfg := &Config{
			Applications: []*Application{
				{
					Name:              "app",
					Implementation:    "impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:         "shrd_01",
							LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound: []byte{0x80, 0x00, 0x00, 0x00}, // overlaps next
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "node_1"},
								{Id: "rpl_02", NodeId: "node_2"},
								{Id: "rpl_03", NodeId: "node_3"},
							},
						},
						{
							Id:         "shrd_02",
							LowerBound: []byte{0x7f, 0xff, 0xff, 0xff},
							UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_04", NodeId: "node_1"},
								{Id: "rpl_05", NodeId: "node_2"},
								{Id: "rpl_06", NodeId: "node_3"},
							},
						},
					},
				},
			},
			Nodes:   baseNodes,
			Version: 1,
		}
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "shards are not contiguous")
	})

	t.Run("does not start at 0x00000000", func(t *testing.T) {
		cfg := &Config{
			Applications: []*Application{
				{
					Name:              "app",
					Implementation:    "impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:         "shrd_01",
							LowerBound: []byte{0x01, 0x00, 0x00, 0x00},
							UpperBound: []byte{0x7f, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "node_1"},
								{Id: "rpl_02", NodeId: "node_2"},
								{Id: "rpl_03", NodeId: "node_3"},
							},
						},
						{
							Id:         "shrd_02",
							LowerBound: []byte{0x80, 0x00, 0x00, 0x00},
							UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_04", NodeId: "node_1"},
								{Id: "rpl_05", NodeId: "node_2"},
								{Id: "rpl_06", NodeId: "node_3"},
							},
						},
					},
				},
			},
			Nodes:   baseNodes,
			Version: 1,
		}
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "shards do not start at 0x00000000")
	})

	t.Run("does not end at 0xFFFFFFFF", func(t *testing.T) {
		cfg := &Config{
			Applications: []*Application{
				{
					Name:              "app",
					Implementation:    "impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:         "shrd_01",
							LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound: []byte{0x7f, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "node_1"},
								{Id: "rpl_02", NodeId: "node_2"},
								{Id: "rpl_03", NodeId: "node_3"},
							},
						},
						{
							Id:         "shrd_02",
							LowerBound: []byte{0x80, 0x00, 0x00, 0x00},
							UpperBound: []byte{0xfe, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_04", NodeId: "node_1"},
								{Id: "rpl_05", NodeId: "node_2"},
								{Id: "rpl_06", NodeId: "node_3"},
							},
						},
					},
				},
			},
			Nodes:   baseNodes,
			Version: 1,
		}
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "shards do not end at 0xffffffff")
	})
}

// stateTestShard builds a shard with the given bounds, state and parent, fully
// replicated across node_1..node_3. Replica ids are derived from the shard id.
func stateTestShard(id string, lower, upper []byte, state ShardState, parentId string) *Shard {
	return &Shard{
		Id:         id,
		LowerBound: lower,
		UpperBound: upper,
		State:      state,
		ParentId:   parentId,
		Replicas: []*Replica{
			{Id: id + "_rpl_1", NodeId: "node_1"},
			{Id: id + "_rpl_2", NodeId: "node_2"},
			{Id: id + "_rpl_3", NodeId: "node_3"},
		},
	}
}

// stateTestConfig wraps shards into a valid 3-node single-application config.
func stateTestConfig(shards ...*Shard) *Config {
	return &Config{
		Applications: []*Application{
			{
				Name:              "app",
				Implementation:    "impl",
				ReplicationFactor: 3,
				Shards:            shards,
			},
		},
		Nodes: []*Node{
			{Id: "node_1", GrpcAddress: "localhost:9001"},
			{Id: "node_2", GrpcAddress: "localhost:9002"},
			{Id: "node_3", GrpcAddress: "localhost:9003"},
		},
		Version: 1,
	}
}

func TestConfig_Validate_ShardStates(t *testing.T) {
	t.Run("completed split: inactive parent overlapped by its active children", func(t *testing.T) {
		cfg := stateTestConfig(
			stateTestShard("shrd_p", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_INACTIVE, ""),
			stateTestShard("shrd_c1", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, "shrd_p"),
			stateTestShard("shrd_c2", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, "shrd_p"),
			stateTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
		)
		require.NoError(t, cfg.Validate())
	})

	t.Run("splitting shard with two activating children covering its range", func(t *testing.T) {
		cfg := stateTestConfig(
			stateTestShard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_SPLITTING, ""),
			stateTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_03", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
			stateTestShard("shrd_04", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
		)
		require.NoError(t, cfg.Validate())
	})

	t.Run("splitting shard with three activating children covering its range", func(t *testing.T) {
		cfg := stateTestConfig(
			stateTestShard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_SPLITTING, ""),
			stateTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_03", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x1f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
			stateTestShard("shrd_04", []byte{0x20, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
			stateTestShard("shrd_05", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
		)
		require.NoError(t, cfg.Validate())
	})

	t.Run("active shard with children", func(t *testing.T) {
		cfg := stateTestConfig(
			stateTestShard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_03", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
			stateTestShard("shrd_04", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
		)
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "active shard shrd_01 must not have children")
	})

	t.Run("inactive shard with no children", func(t *testing.T) {
		cfg := stateTestConfig(
			stateTestShard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_03", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0xbf, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_INACTIVE, ""),
		)
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "inactive shard shrd_03 must have children")
	})

	t.Run("activating shard with children", func(t *testing.T) {
		cfg := stateTestConfig(
			stateTestShard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_SPLITTING, ""),
			stateTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_03", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
			stateTestShard("shrd_04", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
			stateTestShard("shrd_05", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x1f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_03"),
		)
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "activating shard shrd_03 must not have children")
	})

	t.Run("splitting shard with inactive children", func(t *testing.T) {
		cfg := stateTestConfig(
			stateTestShard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_SPLITTING, ""),
			stateTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_03", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_INACTIVE, "shrd_01"),
			stateTestShard("shrd_04", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
		)
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "child shrd_03 of splitting shard shrd_01 must be activating")
	})

	t.Run("activating shard with a non-splitting parent", func(t *testing.T) {
		cfg := stateTestConfig(
			stateTestShard("shrd_p", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_INACTIVE, ""),
			stateTestShard("shrd_c1", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, "shrd_p"),
			stateTestShard("shrd_c2", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, "shrd_p"),
			stateTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_03", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_p"),
		)
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "activating shard shrd_03 must be a child of a splitting shard")
	})

	t.Run("activating shard without a parent", func(t *testing.T) {
		cfg := stateTestConfig(
			stateTestShard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_03", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, ""),
		)
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "activating shard shrd_03 must be a child of a splitting shard")
	})

	t.Run("parent does not exist", func(t *testing.T) {
		cfg := stateTestConfig(
			stateTestShard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_03", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "no_such_shard"),
		)
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "parent no_such_shard of shard shrd_03 not found in application app")
	})

	t.Run("children must be co-located with the splitting parent", func(t *testing.T) {
		cfg := stateTestConfig(
			stateTestShard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_SPLITTING, ""),
			stateTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_03", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
			stateTestShard("shrd_04", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
		)
		// Move one child replica to a node the parent has no replica on.
		cfg.Nodes = append(cfg.Nodes, &Node{Id: "node_4", GrpcAddress: "localhost:9004"})
		cfg.Applications[0].Shards[2].Replicas[0].NodeId = "node_4"

		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "activating child shrd_03 must have replicas on the same nodes as its splitting parent shrd_01")
	})

	t.Run("splitting shard with no children", func(t *testing.T) {
		cfg := stateTestConfig(
			stateTestShard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_SPLITTING, ""),
			stateTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
		)
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "splitting shard shrd_01 must have at least 2 activating children")
	})

	t.Run("splitting shard with a single child", func(t *testing.T) {
		cfg := stateTestConfig(
			stateTestShard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_SPLITTING, ""),
			stateTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_03", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
		)
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "splitting shard shrd_01 must have at least 2 activating children")
	})

	t.Run("children with a gap", func(t *testing.T) {
		cfg := stateTestConfig(
			stateTestShard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_SPLITTING, ""),
			stateTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_03", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xfe}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"), // gap after this
			stateTestShard("shrd_04", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
		)
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "children of splitting shard shrd_01 do not cover its range")
		require.Contains(t, err.Error(), "shards are not contiguous")
	})

	t.Run("children overlap each other", func(t *testing.T) {
		cfg := stateTestConfig(
			stateTestShard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_SPLITTING, ""),
			stateTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_03", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x40, 0x00, 0x00, 0x00}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"), // overlaps next
			stateTestShard("shrd_04", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
		)
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "children of splitting shard shrd_01 do not cover its range")
		require.Contains(t, err.Error(), "shards are not contiguous")
	})

	t.Run("children do not start at parent lower bound", func(t *testing.T) {
		cfg := stateTestConfig(
			stateTestShard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_SPLITTING, ""),
			stateTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_03", []byte{0x00, 0x00, 0x00, 0x01}, []byte{0x3f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
			stateTestShard("shrd_04", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
		)
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "children of splitting shard shrd_01 do not cover its range")
		require.Contains(t, err.Error(), "shards do not start at 0x00000000")
	})

	t.Run("children do not end at parent upper bound", func(t *testing.T) {
		cfg := stateTestConfig(
			stateTestShard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_SPLITTING, ""),
			stateTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_03", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
			stateTestShard("shrd_04", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xfe}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
		)
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "children of splitting shard shrd_01 do not cover its range")
		require.Contains(t, err.Error(), "shards do not end at 0x7fffffff")
	})

	t.Run("active child of a splitting shard breaks the routable partition", func(t *testing.T) {
		// shrd_03 is active, so it overlaps the active/splitting partition
		// (and would be rejected as a non-activating child anyway).
		cfg := stateTestConfig(
			stateTestShard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_SPLITTING, ""),
			stateTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_03", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, "shrd_01"),
			stateTestShard("shrd_04", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
		)
		err := cfg.Validate()
		require.Error(t, err)
	})

	t.Run("active and splitting shards must not overlap", func(t *testing.T) {
		cfg := stateTestConfig(
			stateTestShard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_SPLITTING, ""),
			stateTestShard("shrd_02", []byte{0x7f, 0xff, 0xff, 0xff}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""), // overlaps previous
			stateTestShard("shrd_03", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
			stateTestShard("shrd_04", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
		)
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "shards are not contiguous")
	})

	t.Run("active and splitting shards must leave no gaps", func(t *testing.T) {
		// The gap between the active shards is covered only by a non-routable
		// (activating) shard, which must not count.
		cfg := stateTestConfig(
			stateTestShard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
			stateTestShard("shrd_03", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, ""),
		)
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "shards are not contiguous")
	})

	t.Run("all shards inactive", func(t *testing.T) {
		cfg := stateTestConfig(
			stateTestShard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_INACTIVE, ""),
		)
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "no active shards for app")
	})

	t.Run("unknown shard state", func(t *testing.T) {
		cfg := stateTestConfig(
			stateTestShard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState(42), ""),
		)
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid state 42 for shard shrd_01")
	})
}

func TestConfig_ShardStateMarshaling(t *testing.T) {
	// A config with all four states: shrd_i was split into shrd_s and shrd_a;
	// shrd_s is now splitting again into activating shrd_v1 and shrd_v2.
	config := stateTestConfig(
		stateTestShard("shrd_i", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_INACTIVE, ""),
		stateTestShard("shrd_s", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_SPLITTING, "shrd_i"),
		stateTestShard("shrd_a", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, "shrd_i"),
		stateTestShard("shrd_v1", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x1f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_s"),
		stateTestShard("shrd_v2", []byte{0x20, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_s"),
		stateTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
	)
	require.NoError(t, config.Validate())
	// The loaders sort shards on load; sort the expected config the same way so
	// the roundtrip comparison is order-insensitive.
	config.sortShards()

	t.Run("JSON", func(t *testing.T) {
		data, err := WriteConfigToJson(config)
		require.NoError(t, err)

		// States are human-readable strings.
		require.Contains(t, string(data), `"state": "splitting"`)
		require.Contains(t, string(data), `"state": "inactive"`)
		require.Contains(t, string(data), `"state": "active"`)
		require.Contains(t, string(data), `"state": "activating"`)

		actual, err := LoadConfigFromJson(data)
		require.NoError(t, err)
		require.True(t, cmp.Equal(config, actual, protocmp.Transform()))
	})

	t.Run("JSON state is required", func(t *testing.T) {
		var s Shard
		require.NoError(t, s.UnmarshalJSON([]byte(`{"id":"x","lower_bound":"00","upper_bound":"ff","state":"active"}`)))
		require.Equal(t, ShardState_SHARD_STATE_ACTIVE, s.State)

		err := s.UnmarshalJSON([]byte(`{"id":"x","lower_bound":"00","upper_bound":"ff"}`))
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing state for shard x")

		err = s.UnmarshalJSON([]byte(`{"id":"x","lower_bound":"00","upper_bound":"ff","state":"bogus"}`))
		require.Error(t, err)
		require.Contains(t, err.Error(), `unknown state "bogus" for shard x`)
	})

	t.Run("JSON marshal rejects invalid state", func(t *testing.T) {
		s := &Shard{
			Id:         "x",
			LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
			UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
		}
		_, err := s.MarshalJSON()
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid state 0 for shard x")
	})

	t.Run("Protobuf", func(t *testing.T) {
		data, err := WriteConfigToProto(config)
		require.NoError(t, err)

		actual, err := LoadConfigFromProto(data)
		require.NoError(t, err)
		require.True(t, cmp.Equal(config, actual, protocmp.Transform()))
	})
}

func TestValidateTransition(t *testing.T) {
	baseConfig := &Config{
		Applications: []*Application{
			{
				Name:              "app1",
				Implementation:    "impl1",
				ReplicationFactor: 3,
				Shards: []*Shard{
					{
						Id:         "shrd_01",
						LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
						UpperBound: []byte{0x7f, 0xff, 0xff, 0xff},
						State:      ShardState_SHARD_STATE_ACTIVE,
						Replicas: []*Replica{
							{Id: "rpl_01", NodeId: "node_1"},
							{Id: "rpl_02", NodeId: "node_2"},
							{Id: "rpl_03", NodeId: "node_3"},
						},
					},
					{
						Id:         "shrd_02",
						LowerBound: []byte{0x80, 0x00, 0x00, 0x00},
						UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
						State:      ShardState_SHARD_STATE_ACTIVE,
						Replicas: []*Replica{
							{Id: "rpl_04", NodeId: "node_1"},
							{Id: "rpl_05", NodeId: "node_2"},
							{Id: "rpl_06", NodeId: "node_3"},
						},
					},
				},
			},
		},
		Nodes: []*Node{
			{Id: "node_1", GrpcAddress: "localhost:9001"},
			{Id: "node_2", GrpcAddress: "localhost:9002"},
			{Id: "node_3", GrpcAddress: "localhost:9003"},
		},
		Version: 1,
	}

	t.Run("validate base config", func(t *testing.T) {
		err := baseConfig.Validate()
		require.NoError(t, err)
	})

	t.Run("allow adding a new node", func(t *testing.T) {
		newConfig := cloneConfig(baseConfig)
		newConfig.Nodes = append(newConfig.Nodes, &Node{Id: "node_4", GrpcAddress: "localhost:9004"})

		err := newConfig.Validate()
		require.NoError(t, err)

		err = ValidateTransition(baseConfig, newConfig)
		require.NoError(t, err)
	})

	t.Run("forbid removing node with assigned replica", func(t *testing.T) {
		oldConfig := cloneConfig(baseConfig)
		oldConfig.Nodes = append(oldConfig.Nodes, &Node{Id: "node_4", GrpcAddress: "localhost:9004"})

		newConfig := &Config{
			Applications: []*Application{
				{
					Name:              "app1",
					Implementation:    "impl1",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:         "shrd_01",
							LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound: []byte{0x7f, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "node_1"},
								{Id: "rpl_02", NodeId: "node_2"},
								{Id: "rpl_03", NodeId: "node_4"},
							},
						},
						{
							Id:         "shrd_02",
							LowerBound: []byte{0x80, 0x00, 0x00, 0x00},
							UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
							State:      ShardState_SHARD_STATE_ACTIVE,
							Replicas: []*Replica{
								{Id: "rpl_04", NodeId: "node_1"},
								{Id: "rpl_05", NodeId: "node_2"},
								{Id: "rpl_06", NodeId: "node_4"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "node_1", GrpcAddress: "localhost:9001"},
				{Id: "node_2", GrpcAddress: "localhost:9002"},
				{Id: "node_4", GrpcAddress: "localhost:9004"},
			},
			Version: oldConfig.Version + 1,
		}
		err := newConfig.Validate()
		require.NoError(t, err)

		err = ValidateTransition(oldConfig, newConfig)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot remove node")
	})

	t.Run("allow removing node with no assigned replica", func(t *testing.T) {
		oldConfig := cloneConfig(baseConfig)
		oldConfig.Nodes = append(oldConfig.Nodes, &Node{Id: "node_4", GrpcAddress: "localhost:9004"})
		newConfig := cloneConfig(oldConfig)
		newConfig.Nodes = newConfig.Nodes[:3] // remove nd_04, which has no replica
		err := newConfig.Validate()
		require.NoError(t, err)

		err = ValidateTransition(oldConfig, newConfig)
		require.NoError(t, err)
	})

	t.Run("allow adding a new application", func(t *testing.T) {
		newConfig := cloneConfig(baseConfig)
		newConfig.Applications = append(newConfig.Applications, &Application{
			Name:              "app2",
			Implementation:    "impl2",
			ReplicationFactor: 3,
			Shards: []*Shard{
				{
					Id:         "shrd_03",
					LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
					UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
					State:      ShardState_SHARD_STATE_ACTIVE,
					Replicas: []*Replica{
						{Id: "rpl_07", NodeId: "node_1"},
						{Id: "rpl_08", NodeId: "node_2"},
						{Id: "rpl_09", NodeId: "node_3"},
					},
				},
			},
		})
		err := newConfig.Validate()
		require.NoError(t, err)

		err = ValidateTransition(baseConfig, newConfig)
		require.NoError(t, err)
	})

	t.Run("forbid removing an application", func(t *testing.T) {
		newConfig := cloneConfig(baseConfig)
		newConfig.Applications = []*Application{} // remove all
		err := newConfig.Validate()
		require.NoError(t, err)

		err = ValidateTransition(baseConfig, newConfig)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot remove application")
	})

	t.Run("forbid changing shard bounds", func(t *testing.T) {
		newConfig := cloneConfig(baseConfig)
		newConfig.Applications[0].Shards[0].UpperBound = []byte{0x01, 0xff, 0xff, 0xff}
		newConfig.Applications[0].Shards[1].LowerBound = []byte{0x02, 0x00, 0x00, 0x00}
		err := newConfig.Validate()
		require.NoError(t, err)

		err = ValidateTransition(baseConfig, newConfig)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot change bounds")
	})

	t.Run("allow adding a new replica only", func(t *testing.T) {
		newConfig := cloneConfig(baseConfig)
		newConfig.Nodes = append(newConfig.Nodes, &Node{Id: "node_4", GrpcAddress: "localhost:9004"})
		newConfig.Applications[0].Shards[1].Replicas = append(newConfig.Applications[0].Shards[1].Replicas, &Replica{Id: "rpl_07", NodeId: "node_4"})
		err := newConfig.Validate()
		require.NoError(t, err)

		err = ValidateTransition(baseConfig, newConfig)
		require.NoError(t, err)
	})

	t.Run("allow removing a replica only", func(t *testing.T) {
		oldConfig := cloneConfig(baseConfig)
		oldConfig.Nodes = append(oldConfig.Nodes, &Node{Id: "node_4", GrpcAddress: "localhost:9004"})
		oldConfig.Applications[0].Shards[0].Replicas = []*Replica{
			{Id: "rpl_01", NodeId: "node_1"},
			{Id: "rpl_02", NodeId: "node_2"},
			{Id: "rpl_03", NodeId: "node_3"},
			{Id: "rpl_09", NodeId: "node_4"},
		}

		newConfig := cloneConfig(oldConfig)
		newConfig.Applications[0].Shards[0].Replicas = []*Replica{
			{Id: "rpl_01", NodeId: "node_1"},
			{Id: "rpl_02", NodeId: "node_2"},
			{Id: "rpl_03", NodeId: "node_3"},
		}
		err := newConfig.Validate()
		require.NoError(t, err)

		err = ValidateTransition(oldConfig, newConfig)
		require.NoError(t, err)
	})

	t.Run("forbid adding and removing replicas in same transition", func(t *testing.T) {
		newConfig := cloneConfig(baseConfig)
		// Remove one, add one
		newConfig.Applications[0].Shards[0].Replicas = []*Replica{
			{Id: "rpl_01", NodeId: "node_1"},
			{Id: "rpl_02", NodeId: "node_2"},
			{Id: "rpl_07", NodeId: "node_3"}, // new
		}
		err := newConfig.Validate()
		require.NoError(t, err)

		err = ValidateTransition(baseConfig, newConfig)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot add and remove replicas")
	})

	t.Run("forbid reassigning existing replica to another node", func(t *testing.T) {
		newConfig := cloneConfig(baseConfig)
		newConfig.Applications[0].Shards[0].Replicas[0].NodeId = "node_2"

		err := ValidateTransition(baseConfig, newConfig)
		require.Error(t, err)
		require.Contains(t, err.Error(), "changed node assignment")
	})

	t.Run("forbid changing shard parent", func(t *testing.T) {
		newConfig := cloneConfig(baseConfig)
		newConfig.Applications[0].Shards[1].ParentId = "shrd_01"

		err := ValidateTransition(baseConfig, newConfig)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot change parent for shard shrd_02")
	})

	// splittingConfig is baseConfig with shrd_01 mid-split: splitting, with two
	// activating children.
	splittingConfig := cloneConfig(baseConfig)
	splittingConfig.Applications[0].Shards[0].State = ShardState_SHARD_STATE_SPLITTING
	splittingConfig.Applications[0].Shards = append(splittingConfig.Applications[0].Shards,
		stateTestShard("shrd_03", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
		stateTestShard("shrd_04", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
	)

	t.Run("allow starting a split", func(t *testing.T) {
		require.NoError(t, splittingConfig.Validate())
		require.NoError(t, ValidateTransition(baseConfig, splittingConfig))
	})

	t.Run("forbid starting a split without activating children", func(t *testing.T) {
		newConfig := cloneConfig(baseConfig)
		newConfig.Applications[0].Shards[0].State = ShardState_SHARD_STATE_SPLITTING

		err := ValidateTransition(baseConfig, newConfig)
		require.Error(t, err)
		require.Contains(t, err.Error(), "shard shrd_01 cannot start splitting without at least 2 activating children")
	})

	t.Run("forbid active shard becoming inactive", func(t *testing.T) {
		newConfig := cloneConfig(baseConfig)
		newConfig.Applications[0].Shards[0].State = ShardState_SHARD_STATE_INACTIVE

		err := ValidateTransition(baseConfig, newConfig)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid state transition for shard shrd_01: active -> inactive")
	})

	t.Run("forbid active shard becoming activating", func(t *testing.T) {
		newConfig := cloneConfig(baseConfig)
		newConfig.Applications[0].Shards[0].State = ShardState_SHARD_STATE_ACTIVATING

		err := ValidateTransition(baseConfig, newConfig)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid state transition for shard shrd_01: active -> activating")
	})

	t.Run("forbid new active shard out of nowhere", func(t *testing.T) {
		newConfig := cloneConfig(baseConfig)
		newConfig.Applications[0].Shards = append(newConfig.Applications[0].Shards,
			stateTestShard("shrd_05", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
		)

		err := ValidateTransition(baseConfig, newConfig)
		require.Error(t, err)
		require.Contains(t, err.Error(), "new shard shrd_05 in application app1 cannot be created active")
	})

	t.Run("allow completing a split", func(t *testing.T) {
		// The splitting parent retires to inactive and its activating children
		// become active in the same transition.
		newConfig := cloneConfig(splittingConfig)
		newConfig.Applications[0].Shards[0].State = ShardState_SHARD_STATE_INACTIVE
		newConfig.Applications[0].Shards[2].State = ShardState_SHARD_STATE_ACTIVE
		newConfig.Applications[0].Shards[3].State = ShardState_SHARD_STATE_ACTIVE

		require.NoError(t, newConfig.Validate())
		require.NoError(t, ValidateTransition(splittingConfig, newConfig))
	})

	t.Run("forbid splitting shard becoming active", func(t *testing.T) {
		newConfig := cloneConfig(splittingConfig)
		newConfig.Applications[0].Shards[0].State = ShardState_SHARD_STATE_ACTIVE

		err := ValidateTransition(splittingConfig, newConfig)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid state transition for shard shrd_01: splitting -> active")
	})

	t.Run("forbid activating shard becoming inactive", func(t *testing.T) {
		newConfig := cloneConfig(splittingConfig)
		newConfig.Applications[0].Shards[2].State = ShardState_SHARD_STATE_INACTIVE

		err := ValidateTransition(splittingConfig, newConfig)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid state transition for shard shrd_03: activating -> inactive")
	})

	t.Run("forbid inactive shard changing state", func(t *testing.T) {
		// Old config: the split of shrd_01 has completed.
		oldConfig := cloneConfig(splittingConfig)
		oldConfig.Applications[0].Shards[0].State = ShardState_SHARD_STATE_INACTIVE
		oldConfig.Applications[0].Shards[2].State = ShardState_SHARD_STATE_ACTIVE
		oldConfig.Applications[0].Shards[3].State = ShardState_SHARD_STATE_ACTIVE
		require.NoError(t, oldConfig.Validate())

		for _, state := range []ShardState{
			ShardState_SHARD_STATE_ACTIVE,
			ShardState_SHARD_STATE_SPLITTING,
			ShardState_SHARD_STATE_ACTIVATING,
		} {
			newConfig := cloneConfig(oldConfig)
			newConfig.Applications[0].Shards[0].State = state

			err := ValidateTransition(oldConfig, newConfig)
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid state transition for shard shrd_01: inactive -> "+shardStateName(state))
		}
	})
}

func TestConfigMarshaling(t *testing.T) {
	config := &Config{
		Applications: []*Application{
			{
				Name:              "test.app",
				Implementation:    "test.impl",
				ReplicationFactor: 3,
				Shards: []*Shard{
					{
						Id:         "shrd_01",
						LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
						UpperBound: []byte{0x7f, 0xff, 0xff, 0xff},
						State:      ShardState_SHARD_STATE_ACTIVE,
						Replicas: []*Replica{
							{Id: "rpl_01", NodeId: "node_1"},
							{Id: "rpl_02", NodeId: "node_2"},
							{Id: "rpl_03", NodeId: "node_3"},
						},
					},
					{
						Id:         "shrd_02",
						LowerBound: []byte{0x80, 0x00, 0x00, 0x00},
						UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
						State:      ShardState_SHARD_STATE_ACTIVE,
						Replicas: []*Replica{
							{Id: "rpl_04", NodeId: "node_1"},
							{Id: "rpl_05", NodeId: "node_2"},
							{Id: "rpl_06", NodeId: "node_3"},
						},
					},
				},
			},
		},
		Nodes: []*Node{
			{
				Id: "node_1", GrpcAddress: "localhost:9001",
				Metadata: []*Metadata{
					{Key: "key1", Value: "value1"},
				},
			},
			{Id: "node_2", GrpcAddress: "localhost:9002"},
			{Id: "node_3", GrpcAddress: "localhost:9003"},
		},
		Version: 1,
	}

	t.Run("JSON", func(t *testing.T) {
		data, err := WriteConfigToJson(config)
		require.NoError(t, err)

		actual, err := LoadConfigFromJson(data)
		require.NoError(t, err)

		require.True(t, cmp.Equal(config, actual, protocmp.Transform()))
	})

	t.Run("Protobuf", func(t *testing.T) {
		data, err := WriteConfigToProto(config)
		require.NoError(t, err)

		actual, err := LoadConfigFromProto(data)
		require.NoError(t, err)

		require.True(t, cmp.Equal(config, actual, protocmp.Transform()))
	})
}

func TestConfig_AddReplica(t *testing.T) {
	c := newValidConfig(t)
	shard, err := c.ListShards("Core")
	require.NoError(t, err)
	shardId := shard[0].Id

	// Happy path: explicit id is used verbatim.
	r, err := c.AddReplica("Core", shardId, "explicit_replica_id", "node_1")
	require.NoError(t, err)
	require.Equal(t, "explicit_replica_id", r.Id)
	require.Equal(t, "node_1", r.NodeId)

	got, err := c.GetReplica("explicit_replica_id")
	require.NoError(t, err)
	require.Equal(t, r, got)

	// Duplicate id anywhere in the config is rejected.
	_, err = c.AddReplica("Core", shardId, "explicit_replica_id", "node_2")
	require.ErrorIs(t, err, errReplicaAlreadyExists)

	// Unknown shard / application.
	_, err = c.AddReplica("Core", "no_such_shard", "another_id", "node_1")
	require.ErrorIs(t, err, errShardNotFound)
	_, err = c.AddReplica("NoSuchApp", shardId, "another_id", "node_1")
	require.ErrorIs(t, err, errApplicationNotFound)
}

func TestConfig_MetadataRoundTrip(t *testing.T) {
	// Config-level metadata must survive a marshal/unmarshal round-trip through
	// both encodings (it used to be silently dropped by the loaders).
	c := newValidConfig(t)
	c.Metadata = []*Metadata{
		{Key: "region", Value: "us-east"},
		{Key: "env", Value: "prod"},
	}
	require.NoError(t, c.Validate())

	t.Run("JSON", func(t *testing.T) {
		data, err := WriteConfigToJson(c)
		require.NoError(t, err)
		got, err := LoadConfigFromJson(data)
		require.NoError(t, err)
		require.True(t, cmp.Equal(c, got, protocmp.Transform()))
	})

	t.Run("Protobuf", func(t *testing.T) {
		data, err := WriteConfigToProto(c)
		require.NoError(t, err)
		got, err := LoadConfigFromProto(data)
		require.NoError(t, err)
		require.True(t, cmp.Equal(c, got, protocmp.Transform()))
	})

	t.Run("duplicate config metadata key rejected", func(t *testing.T) {
		bad := newValidConfig(t)
		bad.Metadata = []*Metadata{
			{Key: "dup", Value: "a"},
			{Key: "dup", Value: "b"},
		}
		err := bad.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate metadata key dup for config")
	})
}

func TestShard_UnmarshalJSON_RejectsOverlongBounds(t *testing.T) {
	// A bounds string longer than 8 hex chars would overflow the fixed 4-byte
	// destination in hex.Decode; it must be rejected rather than panic.
	var s Shard
	err := s.UnmarshalJSON([]byte(`{"id":"x","lower_bound":"000000000000","upper_bound":"ffffffff"}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "too long")
}

func TestConfig_ListReturnsCopies(t *testing.T) {
	// The slices returned by the list methods are copies, so appending to them
	// must not mutate the config.
	c := newValidConfig(t)

	nodes := c.ListNodes()
	nodes = append(nodes, &Node{Id: "extra", GrpcAddress: "localhost:1"})
	require.Len(t, c.ListNodes(), len(nodes)-1)

	apps := c.ListApplications()
	apps = append(apps, &Application{Name: "extra"})
	require.Len(t, c.ListApplications(), len(apps)-1)
}

func TestConfig_Hash(t *testing.T) {
	c := newValidConfig(t)

	h1, err := c.Hash()
	require.NoError(t, err)
	require.NotEmpty(t, h1)

	// Stable across a proto marshal/unmarshal round-trip.
	data, err := WriteConfigToProto(c)
	require.NoError(t, err)
	c2, err := LoadConfigFromProto(data)
	require.NoError(t, err)
	h2, err := c2.Hash()
	require.NoError(t, err)
	require.Equal(t, h1, h2)

	// Changes when the config changes (topology).
	shard, err := c2.ListShards("Core")
	require.NoError(t, err)
	_, err = c2.AddReplica("Core", shard[0].Id, "new_replica_id", "node_1")
	require.NoError(t, err)
	h3, err := c2.Hash()
	require.NoError(t, err)
	require.NotEqual(t, h1, h3)

	// Changes when only the version changes.
	c3, err := LoadConfigFromProto(data)
	require.NoError(t, err)
	c3.IncrementVersion()
	h4, err := c3.Hash()
	require.NoError(t, err)
	require.NotEqual(t, h1, h4)
}

// newValidConfig builds a valid 3-node, single-application, two-shard config
// (each shard fully replicated across the three nodes).
func newValidConfig(t *testing.T) *Config {
	t.Helper()

	c := CreateEmptyConfig()

	n1, err := c.CreateNode("node_1", "localhost:9001")
	require.NoError(t, err)
	n2, err := c.CreateNode("node_2", "localhost:9002")
	require.NoError(t, err)
	n3, err := c.CreateNode("node_3", "localhost:9003")
	require.NoError(t, err)

	a, err := c.CreateApplication("Core", "Core", 3)
	require.NoError(t, err)

	s1, err := c.CreateShard(a.Name, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, "")
	require.NoError(t, err)
	s2, err := c.CreateShard(a.Name, []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, "")
	require.NoError(t, err)

	for _, s := range []*Shard{s1, s2} {
		for _, n := range []*Node{n1, n2, n3} {
			_, err := c.CreateReplica(a.Name, s.Id, n.Id)
			require.NoError(t, err)
		}
	}

	require.NoError(t, c.Validate())
	return c
}

func TestConfig_Validate_RejectsSelfParentShard(t *testing.T) {
	// An inactive shard that is its own parent would otherwise satisfy both
	// "parent exists" and "inactive shard has children" (itself), corrupting the
	// split lineage. Validate must reject it.
	cfg := stateTestConfig(
		// An active shard covers the whole keyspace so the coverage check passes;
		// the inactive self-parent shard overlaps it (inactive shards may overlap).
		stateTestShard("shrd_full", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_ACTIVE, ""),
		stateTestShard("shrd_self", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, ShardState_SHARD_STATE_INACTIVE, "shrd_self"),
	)
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "its own parent")
}

func TestConfig_CreateShard_ValidatesBoundsAndDuplicates(t *testing.T) {
	c := CreateEmptyConfig()
	_, err := c.CreateApplication("Core", "Core", 3)
	require.NoError(t, err)

	// Non-4-byte bounds are rejected.
	_, err = c.CreateShard("Core", []byte{0x00}, []byte{0xff, 0xff, 0xff, 0xff}, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "4 bytes")

	// lower >= upper is rejected.
	_, err = c.CreateShard("Core", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0x80, 0x00, 0x00, 0x00}, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "lower bound must be less than upper bound")

	// A first valid shard succeeds.
	s, err := c.CreateShard("Core", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, "")
	require.NoError(t, err)

	// Re-creating a shard with the same bounds (hence the same derived id) is
	// rejected as a duplicate.
	_, err = c.CreateShard("Core", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "already exists")
	require.Equal(t, "Core__", s.Id)
}

func TestConfig_CreateReplica_RequiresExistingNode(t *testing.T) {
	c := CreateEmptyConfig()
	_, err := c.CreateNode("node_1", "localhost:9001")
	require.NoError(t, err)
	_, err = c.CreateApplication("Core", "Core", 3)
	require.NoError(t, err)
	s, err := c.CreateShard("Core", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, "")
	require.NoError(t, err)

	_, err = c.CreateReplica("Core", s.Id, "ghost_node")
	require.ErrorIs(t, err, errNodeNotFound)

	_, err = c.AddReplica("Core", s.Id, "rpl_1", "ghost_node")
	require.ErrorIs(t, err, errNodeNotFound)

	_, err = c.CreateReplica("Core", s.Id, "node_1")
	require.NoError(t, err)
}

// cloneConfig creates a deep copy of a ClusterConfig for test mutation
func cloneConfig(cfg *Config) *Config {
	newCfg := proto.Clone(cfg).(*Config)
	newCfg.Applications = make([]*Application, len(cfg.Applications))
	for i, a := range cfg.Applications {
		newCfg.Applications[i] = proto.Clone(a).(*Application)
		newCfg.Applications[i].Shards = make([]*Shard, len(a.Shards))
		for j, s := range a.Shards {
			shard := proto.Clone(s).(*Shard)
			shard.Replicas = make([]*Replica, len(s.Replicas))
			for k, r := range s.Replicas {
				shard.Replicas[k] = proto.Clone(r).(*Replica)
				shard.Replicas[k].Metadata = make([]*Metadata, len(r.Metadata))
				for l, m := range r.Metadata {
					shard.Replicas[k].Metadata[l] = proto.Clone(m).(*Metadata)
				}
			}
			shard.Metadata = make([]*Metadata, len(s.Metadata))
			for k, m := range s.Metadata {
				shard.Metadata[k] = proto.Clone(m).(*Metadata)
			}
			newCfg.Applications[i].Shards[j] = shard
		}
		newCfg.Applications[i].Metadata = make([]*Metadata, len(a.Metadata))
		for j, m := range a.Metadata {
			newCfg.Applications[i].Metadata[j] = proto.Clone(m).(*Metadata)
		}
	}
	newCfg.Nodes = make([]*Node, len(cfg.Nodes))
	for i, n := range cfg.Nodes {
		newCfg.Nodes[i] = proto.Clone(n).(*Node)
		newCfg.Nodes[i].Metadata = make([]*Metadata, len(n.Metadata))
		for j, m := range n.Metadata {
			newCfg.Nodes[i].Metadata[j] = proto.Clone(m).(*Metadata)
		}
	}
	newCfg.Version = cfg.Version + 1
	return newCfg
}

func TestConfig_WriteConfigToFile_Atomic(t *testing.T) {
	c := newValidConfig(t)
	dir := t.TempDir()

	for _, name := range []string{"cluster.json", "cluster.pb"} {
		path := filepath.Join(dir, name)

		require.NoError(t, WriteConfigToFile(c, path))

		got, err := LoadConfigFromFile(path)
		require.NoError(t, err)
		require.Equal(t, c.Version, got.Version)
		require.Len(t, got.Applications, len(c.Applications))
		require.Len(t, got.Nodes, len(c.Nodes))

		// Overwriting an existing file must succeed (atomic rename over the target).
		c.IncrementVersion()
		require.NoError(t, WriteConfigToFile(c, path))
		got, err = LoadConfigFromFile(path)
		require.NoError(t, err)
		require.Equal(t, c.Version, got.Version)
	}

	// No temporary files may be left behind after successful writes.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 2, "expected only the two config files, found leftovers: %v", entries)

	// Unsupported extension is still rejected.
	require.Error(t, WriteConfigToFile(c, filepath.Join(dir, "cluster.txt")))
}
