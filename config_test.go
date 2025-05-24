package monstera

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClusterConfig(t *testing.T) {
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
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x025},
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
