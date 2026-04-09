package cluster

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestClusterConfigBuilder(t *testing.T) {
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

func TestClusterConfigFindShard(t *testing.T) {
	applications := []*Application{
		{
			Name:              "test.app_01",
			Implementation:    "test.app",
			ReplicationFactor: 3,
			Shards: []*Shard{
				{
					Id:         "shrd_01",
					LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
					UpperBound: []byte{0x3f, 0xff, 0xff, 0xff},
					Replicas: []*Replica{
						{Id: "rplc_01", NodeId: "node_1"},
						{Id: "rplc_02", NodeId: "node_2"},
						{Id: "rplc_03", NodeId: "node_3"},
					},
				},
				{
					Id:         "shrd_05",
					LowerBound: []byte{0x40, 0x00, 0x00, 0x00},
					UpperBound: []byte{0x4f, 0xff, 0xff, 0xff},
					Replicas: []*Replica{
						{Id: "rplc_13", NodeId: "node_1"},
						{Id: "rplc_14", NodeId: "node_2"},
						{Id: "rplc_15", NodeId: "node_3"},
					},
				},
				{
					Id:         "shrd_06",
					LowerBound: []byte{0x50, 0x00, 0x00, 0x00},
					UpperBound: []byte{0x5f, 0xff, 0xff, 0xff},
					Replicas: []*Replica{
						{Id: "rplc_16", NodeId: "node_1"},
						{Id: "rplc_17", NodeId: "node_2"},
						{Id: "rplc_18", NodeId: "node_3"},
					},
				},
				{
					Id:         "shrd_07",
					LowerBound: []byte{0x60, 0x00, 0x00, 0x00},
					UpperBound: []byte{0x6f, 0xff, 0xff, 0xff},
					Replicas: []*Replica{
						{Id: "rplc_19", NodeId: "node_1"},
						{Id: "rplc_20", NodeId: "node_2"},
						{Id: "rplc_21", NodeId: "node_3"},
					},
				},
				{
					Id:         "shrd_08",
					LowerBound: []byte{0x70, 0x00, 0x00, 0x00},
					UpperBound: []byte{0x74, 0xff, 0xff, 0xff},
					Replicas: []*Replica{
						{Id: "rplc_22", NodeId: "node_1"},
						{Id: "rplc_23", NodeId: "node_2"},
						{Id: "rplc_24", NodeId: "node_3"},
					},
				},
				{
					Id:         "shrd_09",
					LowerBound: []byte{0x75, 0x00, 0x00, 0x00},
					UpperBound: []byte{0x75, 0x7f, 0xff, 0xff},
					Replicas: []*Replica{
						{Id: "rplc_25", NodeId: "node_1"},
						{Id: "rplc_26", NodeId: "node_2"},
						{Id: "rplc_27", NodeId: "node_3"},
					},
				},
				{
					Id:         "shrd_10",
					LowerBound: []byte{0x75, 0x80, 0x00, 0x00},
					UpperBound: []byte{0x7f, 0xff, 0xff, 0xff},
					Replicas: []*Replica{
						{Id: "rplc_28", NodeId: "node_1"},
						{Id: "rplc_29", NodeId: "node_2"},
						{Id: "rplc_30", NodeId: "node_3"},
					},
				},
				{
					Id:         "shrd_03",
					LowerBound: []byte{0x80, 0x00, 0x00, 0x00},
					UpperBound: []byte{0xbf, 0xff, 0xff, 0xff},
					Replicas: []*Replica{
						{Id: "rplc_07", NodeId: "node_1"},
						{Id: "rplc_08", NodeId: "node_2"},
						{Id: "rplc_09", NodeId: "node_3"},
					},
				},
				{
					Id:         "shrd_04",
					LowerBound: []byte{0xc0, 0x00, 0x00, 0x00},
					UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
					Replicas: []*Replica{
						{Id: "rplc_10", NodeId: "node_1"},
						{Id: "rplc_11", NodeId: "node_2"},
						{Id: "rplc_12", NodeId: "node_3"},
					},
				},
			},
		},
	}

	nodes := []*Node{
		{Id: "node_1", GrpcAddress: "localhost:9001"},
		{Id: "node_2", GrpcAddress: "localhost:9002"},
		{Id: "node_3", GrpcAddress: "localhost:9003"},
	}

	clusterConfig, err := LoadConfig(applications, nodes, time.Now().UnixMilli())
	require.NoError(t, err)

	p, err := clusterConfig.FindShardByShardKey("test.app_01", []byte{0x14, 0x90, 0x2f, 0x1e})
	require.NoError(t, err)
	require.Equal(t, "shrd_01", p.Id)

	p, err = clusterConfig.FindShardByShardKey("test.app_01", []byte{0x00, 0x90})
	require.NoError(t, err)
	require.Equal(t, "shrd_01", p.Id)

	_, err = clusterConfig.FindShardByShardKey("test.app_02", []byte{0x14, 0x90, 0x2f, 0x1e})
	require.Error(t, err)

	p, err = clusterConfig.FindShardByShardKey("test.app_01", []byte{0x80, 0x90, 0x2f, 0x1e})
	require.NoError(t, err)
	require.Equal(t, "shrd_03", p.Id)

	p, err = clusterConfig.FindShardByShardKey("test.app_01", []byte{0xff, 0x90, 0x2f, 0x1e})
	require.NoError(t, err)
	require.Equal(t, "shrd_04", p.Id)

	p, err = clusterConfig.FindShardByShardKey("test.app_01", []byte{0xff, 0xff, 0xff, 0xff})
	require.NoError(t, err)
	require.Equal(t, "shrd_04", p.Id)

	p, err = clusterConfig.FindShardByShardKey("test.app_01", []byte{0x45, 0x90, 0xff, 0xff})
	require.NoError(t, err)
	require.Equal(t, "shrd_05", p.Id)

	p, err = clusterConfig.FindShardByShardKey("test.app_01", []byte{0x75, 0x40, 0xff, 0xff})
	require.NoError(t, err)
	require.Equal(t, "shrd_09", p.Id)

	p, err = clusterConfig.FindShardByShardKey("test.app_01", []byte{0x75, 0x80, 0xff, 0xff})
	require.NoError(t, err)
	require.Equal(t, "shrd_10", p.Id)
}

func TestClusterConfigValidate(t *testing.T) {
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

func TestClusterConfigValidate_ShardCoverage(t *testing.T) {
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
}

func TestClusterConfig_Marshaling(t *testing.T) {
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

func BenchmarkClusterConfigFindShard(b *testing.B) {
	const (
		numNodes       = 1000
		numApps        = 50
		shardsPerApp   = 1024
		replication    = 3
		keyspacePerApp = 1 << 32 // 4 bytes
	)

	nodes := make([]*Node, numNodes)
	for i := range numNodes {
		nodes[i] = &Node{
			Id:          fmt.Sprintf("node_%04d", i),
			GrpcAddress: fmt.Sprintf("localhost:%d", 9000+i),
		}
	}

	applications := make([]*Application, numApps)
	for appIdx := range numApps {
		appName := fmt.Sprintf("app_%02d", appIdx)
		shards := make([]*Shard, shardsPerApp)
		shardSize := keyspacePerApp / shardsPerApp
		for shardIdx := range shardsPerApp {
			lower := uint32(shardIdx * shardSize)
			upper := uint32((shardIdx+1)*shardSize - 1)
			lowerBound := make([]byte, 4)
			upperBound := make([]byte, 4)
			binary.BigEndian.PutUint32(lowerBound, lower)
			binary.BigEndian.PutUint32(upperBound, upper)
			// Assign replicas to 3 different nodes in round-robin
			replicas := make([]*Replica, replication)
			for r := range replication {
				nodeIdx := (shardIdx*replication + r) % numNodes
				replicas[r] = &Replica{
					Id:     fmt.Sprintf("rpl_%02d_%04d_%d", appIdx, shardIdx, r),
					NodeId: nodes[nodeIdx].Id,
				}
			}
			shards[shardIdx] = &Shard{
				Id:         fmt.Sprintf("shrd_%02d_%04d", appIdx, shardIdx),
				LowerBound: lowerBound,
				UpperBound: upperBound,
				Replicas:   replicas,
			}
		}
		applications[appIdx] = &Application{
			Name:              appName,
			Implementation:    "impl",
			ReplicationFactor: replication,
			Shards:            shards,
		}
	}

	clusterConfig, err := LoadConfig(applications, nodes, time.Now().UnixMilli())
	if err != nil {
		b.Fatalf("failed to create config: %v", err)
	}

	rng := rand.New(rand.NewSource(42))
	lookupKeys := make([]struct {
		appIdx int
		key    []byte
	}, b.N)
	for i := 0; i < b.N; i++ {
		appIdx := rng.Intn(numApps)
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, rng.Uint32())
		lookupKeys[i] = struct {
			appIdx int
			key    []byte
		}{appIdx, key}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		app := applications[lookupKeys[i].appIdx]
		_, err := clusterConfig.FindShardByShardKey(app.Name, lookupKeys[i].key)
		if err != nil {
			b.Fatalf("FindShard failed: %v", err)
		}
	}
}
