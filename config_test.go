package monstera

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
						{
							Id:     "rplc_01",
							NodeId: "nd_01",
						},
						{
							Id:     "rplc_02",
							NodeId: "nd_02",
						},
						{
							Id:     "rplc_03",
							NodeId: "nd_03",
						},
					},
				},
				{
					Id:                "shrd_05",
					LowerBound:        []byte{0x40, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0x4f, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x05},
					Replicas: []*Replica{
						{
							Id:     "rplc_13",
							NodeId: "nd_01",
						},
						{
							Id:     "rplc_14",
							NodeId: "nd_02",
						},
						{
							Id:     "rplc_15",
							NodeId: "nd_03",
						},
					},
				},
				{
					Id:                "shrd_06",
					LowerBound:        []byte{0x50, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0x5f, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x06},
					Replicas: []*Replica{
						{
							Id:     "rplc_16",
							NodeId: "nd_01",
						},
						{
							Id:     "rplc_17",
							NodeId: "nd_02",
						},
						{
							Id:     "rplc_18",
							NodeId: "nd_03",
						},
					},
				},
				{
					Id:                "shrd_07",
					LowerBound:        []byte{0x60, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0x6f, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x07},
					Replicas: []*Replica{
						{
							Id:     "rplc_19",
							NodeId: "nd_01",
						},
						{
							Id:     "rplc_20",
							NodeId: "nd_02",
						},
						{
							Id:     "rplc_21",
							NodeId: "nd_03",
						},
					},
				},
				{
					Id:                "shrd_08",
					LowerBound:        []byte{0x70, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0x74, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x08},
					Replicas: []*Replica{
						{
							Id:     "rplc_22",
							NodeId: "nd_01",
						},
						{
							Id:     "rplc_23",
							NodeId: "nd_02",
						},
						{
							Id:     "rplc_24",
							NodeId: "nd_03",
						},
					},
				},
				{
					Id:                "shrd_09",
					LowerBound:        []byte{0x75, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0x75, 0x7f, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x09},
					Replicas: []*Replica{
						{
							Id:     "rplc_25",
							NodeId: "nd_01",
						},
						{
							Id:     "rplc_26",
							NodeId: "nd_02",
						},
						{
							Id:     "rplc_27",
							NodeId: "nd_03",
						},
					},
				},
				{
					Id:                "shrd_10",
					LowerBound:        []byte{0x75, 0x80, 0x00, 0x00},
					UpperBound:        []byte{0x7f, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x0a},
					Replicas: []*Replica{
						{
							Id:     "rplc_28",
							NodeId: "nd_01",
						},
						{
							Id:     "rplc_29",
							NodeId: "nd_02",
						},
						{
							Id:     "rplc_30",
							NodeId: "nd_03",
						},
					},
				},
				{
					Id:                "shrd_03",
					LowerBound:        []byte{0x80, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0xbf, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x03},
					Replicas: []*Replica{
						{
							Id:     "rplc_07",
							NodeId: "nd_01",
						},
						{
							Id:     "rplc_08",
							NodeId: "nd_02",
						},
						{
							Id:     "rplc_09",
							NodeId: "nd_03",
						},
					},
				},
				{
					Id:                "shrd_04",
					LowerBound:        []byte{0xc0, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x04},
					Replicas: []*Replica{
						{
							Id:     "rplc_10",
							NodeId: "nd_01",
						},
						{
							Id:     "rplc_11",
							NodeId: "nd_02",
						},
						{
							Id:     "rplc_12",
							NodeId: "nd_03",
						},
					},
				},
			},
		},
	}

	nodes := []*Node{
		{
			Id:      "nd_01",
			Address: "localhost:9001",
		},
		{
			Id:      "nd_02",
			Address: "localhost:9002",
		},
		{
			Id:      "nd_03",
			Address: "localhost:9003",
		},
	}

	clusterConfig, err := LoadConfig(applications, nodes, time.Now().UnixMilli())
	require.NoError(err)

	p, err := clusterConfig.FindShard("test.app_01", []byte{0x14, 0x90, 0x2f, 0x1e})
	require.NoError(err)
	require.Equal("shrd_01", p.Id)

	p, err = clusterConfig.FindShard("test.app_01", []byte{0x00, 0x90})
	require.NoError(err)
	require.Equal("shrd_01", p.Id)

	p, err = clusterConfig.FindShard("test.app_02", []byte{0x14, 0x90, 0x2f, 0x1e})
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
								{Id: "rpl_01", NodeId: "nd_01"},
								{Id: "rpl_02", NodeId: "nd_02"},
								{Id: "rpl_03", NodeId: "nd_03"},
							},
						},
						{
							Id:                "shrd_02",
							LowerBound:        []byte{0x80, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x02},
							Replicas: []*Replica{
								{Id: "rpl_04", NodeId: "nd_01"},
								{Id: "rpl_05", NodeId: "nd_02"},
								{Id: "rpl_06", NodeId: "nd_03"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
			},
		}

		err := validConfig.Validate()
		require.NoError(err)
	})

	// Test node validations
	t.Run("empty node address", func(t *testing.T) {
		config := &ClusterConfig{
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: ""},
			},
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "empty node address")
	})

	t.Run("empty node id", func(t *testing.T) {
		config := &ClusterConfig{
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "", Address: "localhost:9003"},
			},
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "empty node id")
	})

	t.Run("duplicate node id", func(t *testing.T) {
		config := &ClusterConfig{
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_01", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
			},
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "duplicate node id")
	})

	// Test application validations
	t.Run("empty application name", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{
				{Name: "", Implementation: "test.impl", ReplicationFactor: 3},
			},
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
			},
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
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
			},
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "empty application implementation")
	})

	t.Run("duplicate application name", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{
				{Name: "test.app", Implementation: "test.impl", ReplicationFactor: 3},
				{Name: "test.app", Implementation: "test.impl2", ReplicationFactor: 3},
			},
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
			},
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
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
			},
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
								{Id: "rpl_01", NodeId: "nd_01"},
								{Id: "rpl_02", NodeId: "nd_02"},
								{Id: "rpl_03", NodeId: "nd_03"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
			},
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
								{Id: "rpl_01", NodeId: "nd_01"},
								{Id: "rpl_02", NodeId: "nd_02"},
								{Id: "rpl_03", NodeId: "nd_03"},
							},
						},
						{
							Id:                "shrd_01",
							LowerBound:        []byte{0x80, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x02},
							Replicas: []*Replica{
								{Id: "rpl_04", NodeId: "nd_01"},
								{Id: "rpl_05", NodeId: "nd_02"},
								{Id: "rpl_06", NodeId: "nd_03"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
			},
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
								{Id: "rpl_01", NodeId: "nd_01"},
								{Id: "rpl_02", NodeId: "nd_02"},
								{Id: "rpl_03", NodeId: "nd_03"},
							},
						},
						{
							Id:                "shrd_02",
							LowerBound:        []byte{0x80, 0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_04", NodeId: "nd_01"},
								{Id: "rpl_05", NodeId: "nd_02"},
								{Id: "rpl_06", NodeId: "nd_03"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
			},
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
								{Id: "rpl_01", NodeId: "nd_01"},
								{Id: "rpl_02", NodeId: "nd_02"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
			},
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
							LowerBound:        []byte{0x00, 0x00, 0x00},
							UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "nd_01"},
								{Id: "rpl_02", NodeId: "nd_02"},
								{Id: "rpl_03", NodeId: "nd_03"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
			},
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
								{Id: "rpl_01", NodeId: "nd_01"},
								{Id: "rpl_02", NodeId: "nd_02"},
								{Id: "rpl_03", NodeId: "nd_03"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
			},
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
								{Id: "", NodeId: "nd_01"},
								{Id: "rpl_02", NodeId: "nd_02"},
								{Id: "rpl_03", NodeId: "nd_03"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
			},
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
								{Id: "rpl_01", NodeId: "nd_01"},
								{Id: "rpl_01", NodeId: "nd_02"},
								{Id: "rpl_03", NodeId: "nd_03"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
			},
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
								{Id: "rpl_01", NodeId: "nd_01"},
								{Id: "rpl_02", NodeId: "nd_02"},
								{Id: "rpl_03", NodeId: "nd_04"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
			},
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "node nd_04 for replica rpl_03 not found")
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
								{Id: "rpl_01", NodeId: "nd_01"},
								{Id: "rpl_02", NodeId: "nd_01"},
								{Id: "rpl_03", NodeId: "nd_03"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
			},
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
							UpperBound:        []byte{0x7f, 0xff, 0xff, 0xff},
							GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
							Replicas: []*Replica{
								{Id: "rpl_01", NodeId: "nd_01"},
								{Id: "rpl_02", NodeId: "nd_02"},
								{Id: "rpl_03", NodeId: "nd_03"},
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
								{Id: "rpl_04", NodeId: "nd_01"},
								{Id: "rpl_05", NodeId: "nd_02"},
								{Id: "rpl_06", NodeId: "nd_03"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
			},
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
								{Id: "rpl_01", NodeId: "nd_01"},
								{Id: "rpl_02", NodeId: "nd_02"},
								{Id: "rpl_03", NodeId: "nd_03"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
			},
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
								{Id: "rpl_01", NodeId: "nd_01"},
								{Id: "rpl_02", NodeId: "nd_02"},
								{Id: "rpl_03", NodeId: "nd_03"},
							},
						},
					},
				},
			},
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
			},
		}

		err := config.Validate()
		require.NoError(err)
	})

	t.Run("empty applications and nodes", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{},
			Nodes:        []*Node{},
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
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "at least 3 nodes are required")
	})

	t.Run("insufficient nodes - 2 nodes", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{},
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
			},
		}

		err := config.Validate()
		require.Error(err)
		require.Contains(err.Error(), "at least 3 nodes are required")
	})

	t.Run("sufficient nodes - 3 nodes", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{},
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
			},
		}

		err := config.Validate()
		require.NoError(err)
	})

	t.Run("sufficient nodes - more than 3 nodes", func(t *testing.T) {
		config := &ClusterConfig{
			Applications: []*Application{},
			Nodes: []*Node{
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
				{Id: "nd_04", Address: "localhost:9004"},
				{Id: "nd_05", Address: "localhost:9005"},
			},
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
				{Id: "nd_01", Address: "localhost:9001"},
				{Id: "nd_02", Address: "localhost:9002"},
				{Id: "nd_03", Address: "localhost:9003"},
			},
		}

		err := config.Validate()
		require.NoError(err)
	})
}
