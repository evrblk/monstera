package monstera

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestClusterConfigFindShard(t *testing.T) {
	require := require.New(t)

	applications := []*Application{
		{
			Name:              "test.app_01",
			Implementation:    "test.app",
			ReplicationFactor: 3,
			Shards: []*Shard{
				{
					Id:                "shrd_01",
					LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0x3f, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
					Replicas: []*Replica{
						{Id: "rplc_01", NodeAddress: "localhost:9001"},
						{Id: "rplc_02", NodeAddress: "localhost:9002"},
						{Id: "rplc_03", NodeAddress: "localhost:9003"},
					},
				},
				{
					Id:                "shrd_05",
					LowerBound:        []byte{0x40, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0x4f, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x05},
					Replicas: []*Replica{
						{Id: "rplc_13", NodeAddress: "localhost:9001"},
						{Id: "rplc_14", NodeAddress: "localhost:9002"},
						{Id: "rplc_15", NodeAddress: "localhost:9003"},
					},
				},
				{
					Id:                "shrd_06",
					LowerBound:        []byte{0x50, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0x5f, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x06},
					Replicas: []*Replica{
						{Id: "rplc_16", NodeAddress: "localhost:9001"},
						{Id: "rplc_17", NodeAddress: "localhost:9002"},
						{Id: "rplc_18", NodeAddress: "localhost:9003"},
					},
				},
				{
					Id:                "shrd_07",
					LowerBound:        []byte{0x60, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0x6f, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x07},
					Replicas: []*Replica{
						{Id: "rplc_19", NodeAddress: "localhost:9001"},
						{Id: "rplc_20", NodeAddress: "localhost:9002"},
						{Id: "rplc_21", NodeAddress: "localhost:9003"},
					},
				},
				{
					Id:                "shrd_08",
					LowerBound:        []byte{0x70, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0x74, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x08},
					Replicas: []*Replica{
						{Id: "rplc_22", NodeAddress: "localhost:9001"},
						{Id: "rplc_23", NodeAddress: "localhost:9002"},
						{Id: "rplc_24", NodeAddress: "localhost:9003"},
					},
				},
				{
					Id:                "shrd_09",
					LowerBound:        []byte{0x75, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0x75, 0x7f, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x09},
					Replicas: []*Replica{
						{Id: "rplc_25", NodeAddress: "localhost:9001"},
						{Id: "rplc_26", NodeAddress: "localhost:9002"},
						{Id: "rplc_27", NodeAddress: "localhost:9003"},
					},
				},
				{
					Id:                "shrd_10",
					LowerBound:        []byte{0x75, 0x80, 0x00, 0x00},
					UpperBound:        []byte{0x7f, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x0a},
					Replicas: []*Replica{
						{Id: "rplc_28", NodeAddress: "localhost:9001"},
						{Id: "rplc_29", NodeAddress: "localhost:9002"},
						{Id: "rplc_30", NodeAddress: "localhost:9003"},
					},
				},
				{
					Id:                "shrd_03",
					LowerBound:        []byte{0x80, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0xbf, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x03},
					Replicas: []*Replica{
						{Id: "rplc_07", NodeAddress: "localhost:9001"},
						{Id: "rplc_08", NodeAddress: "localhost:9002"},
						{Id: "rplc_09", NodeAddress: "localhost:9003"},
					},
				},
				{
					Id:                "shrd_04",
					LowerBound:        []byte{0xc0, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x04},
					Replicas: []*Replica{
						{Id: "rplc_10", NodeAddress: "localhost:9001"},
						{Id: "rplc_11", NodeAddress: "localhost:9002"},
						{Id: "rplc_12", NodeAddress: "localhost:9003"},
					},
				},
			},
		},
	}

	nodes := []*Node{
		{Address: "localhost:9001"},
		{Address: "localhost:9002"},
		{Address: "localhost:9003"},
	}

	clusterConfig, err := LoadConfig(applications, nodes, time.Now().UnixMilli())
	require.NoError(err)

	p, err := clusterConfig.FindShard("test.app_01", []byte{0x14, 0x90, 0x2f, 0x1e})
	require.NoError(err)
	require.Equal("shrd_01", p.Id)

	p, err = clusterConfig.FindShard("test.app_01", []byte{0x00, 0x90})
	require.NoError(err)
	require.Equal("shrd_01", p.Id)

	_, err = clusterConfig.FindShard("test.app_02", []byte{0x14, 0x90, 0x2f, 0x1e})
	require.Error(err)

	p, err = clusterConfig.FindShard("test.app_01", []byte{0x80, 0x90, 0x2f, 0x1e})
	require.NoError(err)
	require.Equal("shrd_03", p.Id)

	p, err = clusterConfig.FindShard("test.app_01", []byte{0xff, 0x90, 0x2f, 0x1e})
	require.NoError(err)
	require.Equal("shrd_04", p.Id)

	p, err = clusterConfig.FindShard("test.app_01", []byte{0xff, 0xff, 0xff, 0xff})
	require.NoError(err)
	require.Equal("shrd_04", p.Id)

	p, err = clusterConfig.FindShard("test.app_01", []byte{0x45, 0x90, 0xff, 0xff})
	require.NoError(err)
	require.Equal("shrd_05", p.Id)

	p, err = clusterConfig.FindShard("test.app_01", []byte{0x75, 0x40, 0xff, 0xff})
	require.NoError(err)
	require.Equal("shrd_09", p.Id)

	p, err = clusterConfig.FindShard("test.app_01", []byte{0x75, 0x80, 0xff, 0xff})
	require.NoError(err)
	require.Equal("shrd_10", p.Id)
}

func TestClusterConfigValidate(t *testing.T) {
	require := require.New(t)

	// Test valid configuration
	t.Run("valid configuration", func(t *testing.T) {
		validConfig := &ClusterConfig{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0x7f, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeAddress: "localhost:9001"},
								{Id: "rpl_02", NodeAddress: "localhost:9002"},
								{Id: "rpl_03", NodeAddress: "localhost:9003"},
							},
						},
						{
							Id:                "shrd_02",
							LowerBound:        []byte{0x80, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x02},
							Replicas: []*Replica{
								{Id: "rpl_04", NodeAddress: "localhost:9001"},
								{Id: "rpl_05", NodeAddress: "localhost:9002"},
								{Id: "rpl_06", NodeAddress: "localhost:9003"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9003"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := validConfig.Validate()
		require.NoError(err)
	})

	// Test node validations
	t.Run("empty node address", func(t *testing.T) {
		config := &ClusterConfig{
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: ""},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "empty node address")
	})

	t.Run("duplicate node address", func(t *testing.T) {
		config := &ClusterConfig{
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9001"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "duplicate node address")
	})

	// Test application validations
	t.Run("empty application name", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{
				{Name: "", Implementation: "test.impl", ReplicationFactor: 3},
			},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9003"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "empty application name")
	})

	t.Run("empty application implementation", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{
				{Name: "test.app", Implementation: "", ReplicationFactor: 3},
			},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9003"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "empty application implementation")
	})

	t.Run("duplicate application name", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{
				{Name: "test.app", Implementation: "test.impl", ReplicationFactor: 3, Shards: []*Shard{
					{
						Id:                "shrd_01",
						LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
						UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
						GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
						Replicas: []*Replica{
							{Id: "rpl_01", NodeAddress: "localhost:9001"},
							{Id: "rpl_02", NodeAddress: "localhost:9002"},
							{Id: "rpl_03", NodeAddress: "localhost:9003"},
						}},
				}},
				{Name: "test.app", Implementation: "test.impl2", ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeAddress: "localhost:9001"},
								{Id: "rpl_02", NodeAddress: "localhost:9002"},
								{Id: "rpl_03", NodeAddress: "localhost:9003"},
							}},
					}},
			},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9003"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "duplicate application name")
	})

	t.Run("invalid replication factor", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{
				{Name: "test.app", Implementation: "test.impl", ReplicationFactor: 2},
			},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9003"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "invalid replication factor")
	})

	// Test shard validations
	t.Run("empty shard id", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:                "",
							LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeAddress: "localhost:9001"},
								{Id: "rpl_02", NodeAddress: "localhost:9002"},
								{Id: "rpl_03", NodeAddress: "localhost:9003"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9003"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "empty shard id")
	})

	t.Run("duplicate shard id", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0x7f, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeAddress: "localhost:9001"},
								{Id: "rpl_02", NodeAddress: "localhost:9002"},
								{Id: "rpl_03", NodeAddress: "localhost:9003"},
							},
						},
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0x80, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x02},
							Replicas: []*Replica{
								{Id: "rpl_04", NodeAddress: "localhost:9001"},
								{Id: "rpl_05", NodeAddress: "localhost:9002"},
								{Id: "rpl_06", NodeAddress: "localhost:9003"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9003"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "duplicate shard id")
	})

	t.Run("duplicate global index prefix", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0x7f, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeAddress: "localhost:9001"},
								{Id: "rpl_02", NodeAddress: "localhost:9002"},
								{Id: "rpl_03", NodeAddress: "localhost:9003"},
							},
						},
						{
							Id:                "shrd_02",
							LowerBound:        []byte{0x80, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_04", NodeAddress: "localhost:9001"},
								{Id: "rpl_05", NodeAddress: "localhost:9002"},
								{Id: "rpl_06", NodeAddress: "localhost:9003"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9003"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "duplicate global index prefix")
	})

	t.Run("not enough replicas", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeAddress: "localhost:9001"},
								{Id: "rpl_02", NodeAddress: "localhost:9002"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9003"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "not enough replicas")
	})

	t.Run("invalid bounds length", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0x7f, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeAddress: "localhost:9001"},
								{Id: "rpl_02", NodeAddress: "localhost:9002"},
								{Id: "rpl_03", NodeAddress: "localhost:9003"},
							},
						},
						{
							Id:                "shrd_02",
							LowerBound:        []byte{0x80, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x02},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeAddress: "localhost:9001"},
								{Id: "rpl_02", NodeAddress: "localhost:9002"},
								{Id: "rpl_03", NodeAddress: "localhost:9003"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9003"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "invalid lower bound/upper bounds")
	})

	t.Run("invalid bounds order", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0xff, 0xff, 0xff, 0xff},
							UpperBound:        []byte{0x00, 0x00, 0x00, 0x00},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeAddress: "localhost:9001"},
								{Id: "rpl_02", NodeAddress: "localhost:9002"},
								{Id: "rpl_03", NodeAddress: "localhost:9003"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9003"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "invalid lower bound/upper bounds")
	})

	// Test replica validations
	t.Run("empty replica id", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "", NodeAddress: "localhost:9001"},
								{Id: "rpl_02", NodeAddress: "localhost:9002"},
								{Id: "rpl_03", NodeAddress: "localhost:9003"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9003"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "empty replica id")
	})

	t.Run("duplicate replica id", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeAddress: "localhost:9001"},
								{Id: "rpl_01", NodeAddress: "localhost:9002"},
								{Id: "rpl_03", NodeAddress: "localhost:9003"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9003"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "duplicate replica id")
	})

	t.Run("replica assigned to non-existent node", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeAddress: "localhost:9001"},
								{Id: "rpl_02", NodeAddress: "localhost:9002"},
								{Id: "rpl_03", NodeAddress: "localhost:9004"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9003"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "node localhost:9004 for replica rpl_03 not found")
	})

	t.Run("replicas assigned to same node", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeAddress: "localhost:9001"},
								{Id: "rpl_02", NodeAddress: "localhost:9001"},
								{Id: "rpl_03", NodeAddress: "localhost:9003"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9003"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "replicas are not assigned to different nodes")
	})

	// Test multiple applications with shards
	t.Run("multiple applications with valid shards", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{
				{
					Name:              "app1",
					Implementation:    "impl1",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeAddress: "localhost:9001"},
								{Id: "rpl_02", NodeAddress: "localhost:9002"},
								{Id: "rpl_03", NodeAddress: "localhost:9003"},
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
							Id:                "shrd_02",
							LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x02},
							Replicas: []*Replica{
								{Id: "rpl_04", NodeAddress: "localhost:9001"},
								{Id: "rpl_05", NodeAddress: "localhost:9002"},
								{Id: "rpl_06", NodeAddress: "localhost:9003"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9003"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.NoError(err)
	})

	// Test edge cases
	t.Run("minimum valid replication factor", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeAddress: "localhost:9001"},
								{Id: "rpl_02", NodeAddress: "localhost:9002"},
								{Id: "rpl_03", NodeAddress: "localhost:9003"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9003"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.NoError(err)
	})

	t.Run("exact replica count", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeAddress: "localhost:9001"},
								{Id: "rpl_02", NodeAddress: "localhost:9002"},
								{Id: "rpl_03", NodeAddress: "localhost:9003"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9003"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.NoError(err)
	})

	t.Run("empty applications and nodes", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{},
			Nodes:        []*Node{},
			UpdatedAt:    time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "at least 3 nodes are required")
	})

	// Test minimum node requirement
	t.Run("insufficient nodes - 0 nodes", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{},
			Nodes:        []*Node{},
			UpdatedAt:    time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "at least 3 nodes are required")
	})

	t.Run("insufficient nodes - 2 nodes", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "at least 3 nodes are required")
	})

	t.Run("sufficient nodes - 3 nodes", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9003"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.NoError(err)
	})

	t.Run("sufficient nodes - more than 3 nodes", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9003"},
				{Address: "localhost:9004"},
				{Address: "localhost:9005"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.NoError(err)
	})

	t.Run("application with no shards", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{
				{
					Name:              "test.app",
					Implementation:    "test.impl",
					ReplicationFactor: 3,
					Shards:            []*Shard{},
				},
			},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9003"},
			},
			UpdatedAt: time.Now().UnixMilli(),
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "no shards for test.app")
	})
}

func TestClusterConfigValidate_ShardCoverage(t *testing.T) {
	require := require.New(t)
	baseNodes := []*Node{
		{Address: "localhost:9001"},
		{Address: "localhost:9002"},
		{Address: "localhost:9003"},
	}

	t.Run("valid contiguous full coverage", func(t *testing.T) {
		cfg := &ClusterConfig{
			Applications: []*Application{
				{
					Name:              "app",
					Implementation:    "impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0x7f, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeAddress: "localhost:9001"},
								{Id: "rpl_02", NodeAddress: "localhost:9002"},
								{Id: "rpl_03", NodeAddress: "localhost:9003"},
							},
						},
						{
							Id:                "shrd_02",
							LowerBound:        []byte{0x80, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x02},
							Replicas: []*Replica{
								{Id: "rpl_04", NodeAddress: "localhost:9001"},
								{Id: "rpl_05", NodeAddress: "localhost:9002"},
								{Id: "rpl_06", NodeAddress: "localhost:9003"},
							},
						},
					},
				},
			},
			Nodes:     baseNodes,
			UpdatedAt: time.Now().UnixMilli(),
		}
		err := cfg.Validate()
		require.NoError(err)
	})

	t.Run("gap between shards", func(t *testing.T) {
		cfg := &ClusterConfig{
			Applications: []*Application{
				{
					Name:              "app",
					Implementation:    "impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0x7f, 0xff, 0xff, 0xfe}, // gap after this
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeAddress: "localhost:9001"},
								{Id: "rpl_02", NodeAddress: "localhost:9002"},
								{Id: "rpl_03", NodeAddress: "localhost:9003"},
							},
						},
						{
							Id:                "shrd_02",
							LowerBound:        []byte{0x80, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x02},
							Replicas: []*Replica{
								{Id: "rpl_04", NodeAddress: "localhost:9001"},
								{Id: "rpl_05", NodeAddress: "localhost:9002"},
								{Id: "rpl_06", NodeAddress: "localhost:9003"},
							},
						},
					},
				},
			},
			Nodes:     baseNodes,
			UpdatedAt: time.Now().UnixMilli(),
		}
		err := cfg.Validate()
		require.Error(err)
		require.Contains(err.Error(), "shards are not contiguous")
	})

	t.Run("overlap between shards", func(t *testing.T) {
		cfg := &ClusterConfig{
			Applications: []*Application{
				{
					Name:              "app",
					Implementation:    "impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0x80, 0x00, 0x00, 0x00}, // overlaps next
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeAddress: "localhost:9001"},
								{Id: "rpl_02", NodeAddress: "localhost:9002"},
								{Id: "rpl_03", NodeAddress: "localhost:9003"},
							},
						},
						{
							Id:                "shrd_02",
							LowerBound:        []byte{0x7f, 0xff, 0xff, 0xff},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x02},
							Replicas: []*Replica{
								{Id: "rpl_04", NodeAddress: "localhost:9001"},
								{Id: "rpl_05", NodeAddress: "localhost:9002"},
								{Id: "rpl_06", NodeAddress: "localhost:9003"},
							},
						},
					},
				},
			},
			Nodes:     baseNodes,
			UpdatedAt: time.Now().UnixMilli(),
		}
		err := cfg.Validate()
		require.Error(err)
		require.Contains(err.Error(), "shards are not contiguous")
	})

	t.Run("does not start at 0x00000000", func(t *testing.T) {
		cfg := &ClusterConfig{
			Applications: []*Application{
				{
					Name:              "app",
					Implementation:    "impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0x01, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0x7f, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeAddress: "localhost:9001"},
								{Id: "rpl_02", NodeAddress: "localhost:9002"},
								{Id: "rpl_03", NodeAddress: "localhost:9003"},
							},
						},
						{
							Id:                "shrd_02",
							LowerBound:        []byte{0x80, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x02},
							Replicas: []*Replica{
								{Id: "rpl_04", NodeAddress: "localhost:9001"},
								{Id: "rpl_05", NodeAddress: "localhost:9002"},
								{Id: "rpl_06", NodeAddress: "localhost:9003"},
							},
						},
					},
				},
			},
			Nodes:     baseNodes,
			UpdatedAt: time.Now().UnixMilli(),
		}
		err := cfg.Validate()
		require.Error(err)
		require.Contains(err.Error(), "shards do not start at 0x00000000")
	})

	t.Run("does not end at 0xFFFFFFFF", func(t *testing.T) {
		cfg := &ClusterConfig{
			Applications: []*Application{
				{
					Name:              "app",
					Implementation:    "impl",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0x7f, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeAddress: "localhost:9001"},
								{Id: "rpl_02", NodeAddress: "localhost:9002"},
								{Id: "rpl_03", NodeAddress: "localhost:9003"},
							},
						},
						{
							Id:                "shrd_02",
							LowerBound:        []byte{0x80, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xfe, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x02},
							Replicas: []*Replica{
								{Id: "rpl_04", NodeAddress: "localhost:9001"},
								{Id: "rpl_05", NodeAddress: "localhost:9002"},
								{Id: "rpl_06", NodeAddress: "localhost:9003"},
							},
						},
					},
				},
			},
			Nodes:     baseNodes,
			UpdatedAt: time.Now().UnixMilli(),
		}
		err := cfg.Validate()
		require.Error(err)
		require.Contains(err.Error(), "shards do not end at 0xffffffff")
	})
}

func TestValidateTransition(t *testing.T) {
	require := require.New(t)

	baseConfig := &ClusterConfig{
		Applications: []*Application{
			{
				Name:              "app1",
				Implementation:    "impl1",
				ReplicationFactor: 3,
				Shards: []*Shard{
					{
						Id:                "shrd_01",
						LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
						UpperBound:        []byte{0x7f, 0xff, 0xff, 0xff},
						GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
						Replicas: []*Replica{
							{Id: "rpl_01", NodeAddress: "localhost:9001"},
							{Id: "rpl_02", NodeAddress: "localhost:9002"},
							{Id: "rpl_03", NodeAddress: "localhost:9003"},
						},
					},
					{
						Id:                "shrd_02",
						LowerBound:        []byte{0x80, 0x00, 0x00, 0x00},
						UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
						GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x02},
						Replicas: []*Replica{
							{Id: "rpl_04", NodeAddress: "localhost:9001"},
							{Id: "rpl_05", NodeAddress: "localhost:9002"},
							{Id: "rpl_06", NodeAddress: "localhost:9003"},
						},
					},
				},
			},
		},
		Nodes: []*Node{
			{Address: "localhost:9001"},
			{Address: "localhost:9002"},
			{Address: "localhost:9003"},
		},
		UpdatedAt: time.Now().UnixMilli(),
	}

	t.Run("validate base config", func(t *testing.T) {
		err := baseConfig.Validate()
		require.NoError(err)
	})

	t.Run("allow adding a new node", func(t *testing.T) {
		newConfig := cloneConfig(baseConfig)
		newConfig.Nodes = append(newConfig.Nodes, &Node{Address: "localhost:9004"})

		err := newConfig.Validate()
		require.NoError(err)

		err = ValidateTransition(baseConfig, newConfig)
		require.NoError(err)
	})

	t.Run("forbid removing node with assigned replica", func(t *testing.T) {
		oldConfig := cloneConfig(baseConfig)
		oldConfig.Nodes = append(oldConfig.Nodes, &Node{Address: "localhost:9004"})

		newConfig := &ClusterConfig{
			Applications: []*Application{
				{
					Name:              "app1",
					Implementation:    "impl1",
					ReplicationFactor: 3,
					Shards: []*Shard{
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0x7f, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeAddress: "localhost:9001"},
								{Id: "rpl_02", NodeAddress: "localhost:9002"},
								{Id: "rpl_03", NodeAddress: "localhost:9004"},
							},
						},
						{
							Id:                "shrd_02",
							LowerBound:        []byte{0x80, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x02},
							Replicas: []*Replica{
								{Id: "rpl_04", NodeAddress: "localhost:9001"},
								{Id: "rpl_05", NodeAddress: "localhost:9002"},
								{Id: "rpl_06", NodeAddress: "localhost:9004"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Address: "localhost:9001"},
				{Address: "localhost:9002"},
				{Address: "localhost:9004"},
			},
			UpdatedAt: oldConfig.UpdatedAt + 1,
		}
		err := newConfig.Validate()
		require.NoError(err)

		err = ValidateTransition(oldConfig, newConfig)
		require.Error(err)
		require.Contains(err.Error(), "cannot remove node")
	})

	t.Run("allow removing node with no assigned replica", func(t *testing.T) {
		oldConfig := cloneConfig(baseConfig)
		oldConfig.Nodes = append(oldConfig.Nodes, &Node{Address: "localhost:9004"})
		newConfig := cloneConfig(oldConfig)
		newConfig.Nodes = newConfig.Nodes[:3] // remove nd_04, which has no replica
		err := newConfig.Validate()
		require.NoError(err)

		err = ValidateTransition(oldConfig, newConfig)
		require.NoError(err)
	})

	t.Run("allow adding a new application", func(t *testing.T) {
		newConfig := cloneConfig(baseConfig)
		newConfig.Applications = append(newConfig.Applications, &Application{
			Name:              "app2",
			Implementation:    "impl2",
			ReplicationFactor: 3,
			Shards: []*Shard{
				{
					Id:                "shrd_03",
					LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x03},
					Replicas: []*Replica{
						{Id: "rpl_07", NodeAddress: "localhost:9001"},
						{Id: "rpl_08", NodeAddress: "localhost:9002"},
						{Id: "rpl_09", NodeAddress: "localhost:9003"},
					},
				},
			},
		})
		err := newConfig.Validate()
		require.NoError(err)

		err = ValidateTransition(baseConfig, newConfig)
		require.NoError(err)
	})

	t.Run("forbid removing an application", func(t *testing.T) {
		newConfig := cloneConfig(baseConfig)
		newConfig.Applications = []*Application{} // remove all
		err := newConfig.Validate()
		require.NoError(err)

		err = ValidateTransition(baseConfig, newConfig)
		require.Error(err)
		require.Contains(err.Error(), "cannot remove application")
	})

	t.Run("forbid changing shard bounds", func(t *testing.T) {
		newConfig := cloneConfig(baseConfig)
		newConfig.Applications[0].Shards[0].UpperBound = []byte{0x01, 0xff, 0xff, 0xff}
		newConfig.Applications[0].Shards[1].LowerBound = []byte{0x02, 0x00, 0x00, 0x00}
		err := newConfig.Validate()
		require.NoError(err)

		err = ValidateTransition(baseConfig, newConfig)
		require.Error(err)
		require.Contains(err.Error(), "cannot change bounds")
	})

	t.Run("forbid changing shard global index prefix", func(t *testing.T) {
		newConfig := cloneConfig(baseConfig)
		newConfig.Applications[0].Shards[0].GlobalIndexPrefix = []byte{0x00, 0x00, 0x00, 0x08}
		err := newConfig.Validate()
		require.NoError(err)

		err = ValidateTransition(baseConfig, newConfig)
		require.Error(err)
		require.Contains(err.Error(), "cannot change bounds or global index prefix")
	})

	t.Run("allow adding a new replica only", func(t *testing.T) {
		newConfig := cloneConfig(baseConfig)
		newConfig.Nodes = append(newConfig.Nodes, &Node{Address: "localhost:9004"})
		newConfig.Applications[0].Shards[1].Replicas = append(newConfig.Applications[0].Shards[1].Replicas, &Replica{Id: "rpl_07", NodeAddress: "localhost:9004"})
		err := newConfig.Validate()
		require.NoError(err)

		err = ValidateTransition(baseConfig, newConfig)
		require.NoError(err)
	})

	t.Run("allow removing a replica only", func(t *testing.T) {
		oldConfig := cloneConfig(baseConfig)
		oldConfig.Nodes = append(oldConfig.Nodes, &Node{Address: "localhost:9004"})
		oldConfig.Applications[0].Shards[0].Replicas = []*Replica{
			{Id: "rpl_01", NodeAddress: "localhost:9001"},
			{Id: "rpl_02", NodeAddress: "localhost:9002"},
			{Id: "rpl_03", NodeAddress: "localhost:9003"},
			{Id: "rpl_09", NodeAddress: "localhost:9004"},
		}

		newConfig := cloneConfig(oldConfig)
		newConfig.Applications[0].Shards[0].Replicas = []*Replica{
			{Id: "rpl_01", NodeAddress: "localhost:9001"},
			{Id: "rpl_02", NodeAddress: "localhost:9002"},
			{Id: "rpl_03", NodeAddress: "localhost:9003"},
		}
		err := newConfig.Validate()
		require.NoError(err)

		err = ValidateTransition(oldConfig, newConfig)
		require.NoError(err)
	})

	t.Run("forbid adding and removing replicas in same transition", func(t *testing.T) {
		newConfig := cloneConfig(baseConfig)
		// Remove one, add one
		newConfig.Applications[0].Shards[0].Replicas = []*Replica{
			{Id: "rpl_01", NodeAddress: "localhost:9001"},
			{Id: "rpl_02", NodeAddress: "localhost:9002"},
			{Id: "rpl_07", NodeAddress: "localhost:9003"}, // new
		}
		err := newConfig.Validate()
		require.NoError(err)

		err = ValidateTransition(baseConfig, newConfig)
		require.Error(err)
		require.Contains(err.Error(), "cannot add and remove replicas")
	})

	t.Run("forbid reassigning existing replica to another node", func(t *testing.T) {
		newConfig := cloneConfig(baseConfig)
		newConfig.Applications[0].Shards[0].Replicas[0].NodeAddress = "localhost:9002"

		err := ValidateTransition(baseConfig, newConfig)
		require.Error(err)
		require.Contains(err.Error(), "changed node assignment")
	})
}

// cloneConfig creates a deep copy of a ClusterConfig for test mutation
func cloneConfig(cfg *ClusterConfig) *ClusterConfig {
	newCfg := proto.Clone(cfg).(*ClusterConfig)
	newCfg.Applications = make([]*Application, len(cfg.Applications))
	for i, a := range cfg.Applications {
		newCfg.Applications[i] = proto.Clone(a).(*Application)
		newCfg.Applications[i].Shards = make([]*Shard, len(a.Shards))
		for j, s := range a.Shards {
			shard := proto.Clone(s).(*Shard)
			shard.Replicas = make([]*Replica, len(s.Replicas))
			for k, r := range s.Replicas {
				shard.Replicas[k] = proto.Clone(r).(*Replica)
			}
			newCfg.Applications[i].Shards[j] = shard
		}
	}
	newCfg.Nodes = make([]*Node, len(cfg.Nodes))
	for i, n := range cfg.Nodes {
		newCfg.Nodes[i] = proto.Clone(n).(*Node)
	}
	newCfg.UpdatedAt = cfg.UpdatedAt + 1
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
	for i := 0; i < numNodes; i++ {
		nodes[i] = &Node{
			Address: fmt.Sprintf("localhost:%d", 9000+i),
		}
	}

	applications := make([]*Application, numApps)
	for appIdx := 0; appIdx < numApps; appIdx++ {
		appName := fmt.Sprintf("app_%02d", appIdx)
		shards := make([]*Shard, shardsPerApp)
		shardSize := keyspacePerApp / shardsPerApp
		for shardIdx := 0; shardIdx < shardsPerApp; shardIdx++ {
			lower := uint32(shardIdx * shardSize)
			upper := uint32((shardIdx+1)*shardSize - 1)
			lowerBound := make([]byte, 4)
			upperBound := make([]byte, 4)
			binary.BigEndian.PutUint32(lowerBound, lower)
			binary.BigEndian.PutUint32(upperBound, upper)
			// Assign replicas to 3 different nodes in round-robin
			replicas := make([]*Replica, replication)
			for r := 0; r < replication; r++ {
				nodeIdx := (shardIdx*replication + r) % numNodes
				replicas[r] = &Replica{
					Id:          fmt.Sprintf("rpl_%02d_%04d_%d", appIdx, shardIdx, r),
					NodeAddress: nodes[nodeIdx].Address,
				}
			}
			shards[shardIdx] = &Shard{
				Id:                fmt.Sprintf("shrd_%02d_%04d", appIdx, shardIdx),
				LowerBound:        lowerBound,
				UpperBound:        upperBound,
				GlobalIndexPrefix: []byte{byte(appIdx), byte(shardIdx >> 8), byte(shardIdx)},
				Replicas:          replicas,
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
		_, err := clusterConfig.FindShard(app.Name, lookupKeys[i].key)
		if err != nil {
			b.Fatalf("FindShard failed: %v", err)
		}
	}
}
