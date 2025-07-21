package playground

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/evrblk/monstera"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestPlaygroundApiMonsteraStub_ReadAndUpdate(t *testing.T) {
	require := require.New(t)

	clusterConfig := NewTestClusterConfig()
	stub := NewMonsteraStub(clusterConfig)
	nodes := NewCluster(clusterConfig)
	defer func() {
		for _, n := range nodes {
			n.monsteraNode.Stop()
			n.grpcServer.Stop()
		}
	}()

	t1 := time.Now()
	allReady := true
	for time.Now().Before(t1.Add(2 * time.Second)) {
		allReady = true
		for _, n := range nodes {
			if n.monsteraNode.NodeState() != monstera.READY {
				allReady = false
				break
			}
		}
		if allReady {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !allReady {
		t.Fatalf("Nodes not ready after 2 seconds")
	}

	for i := 0; i < 100; i++ {
		// Test reading non-existent key
		key := rand.Uint64()

		resp1, err := stub.Read(context.Background(), key)
		require.NoError(err)
		require.Empty(resp1, "Expected empty result for non-existent key")

		// Update key
		value := "test value"
		resp2, err := stub.Update(context.Background(), key, value)
		require.NoError(err)
		require.Equal(value, resp2)

		// Test reading existing key
		resp3, err := stub.Read(context.Background(), key)
		require.NoError(err)
		require.Equal(value, resp3)
	}

	nodes[0].monsteraNode.Stop()
	nodes[0].grpcServer.Stop()

	for i := 0; i < 100; i++ {
		key := rand.Uint64()

		// Update key
		value := "test value"
		resp2, err := stub.Update(context.Background(), key, value)
		require.NoError(err)
		require.Equal(value, resp2)

		// Test reading existing key
		resp3, err := stub.Read(context.Background(), key)
		require.NoError(err)
		require.Equal(value, resp3)
	}
}

type localNode struct {
	monsteraNode *monstera.MonsteraNode
	lis          net.Listener
	grpcServer   *grpc.Server
}

func NewCluster(clusterConfig *monstera.ClusterConfig) []localNode {
	nodes := make([]localNode, 0)

	for _, n := range clusterConfig.Nodes {
		badgerStore := monstera.NewBadgerInMemoryStore()

		baseDir := fmt.Sprintf("/tmp/monstera/%d/%s", rand.Uint64(), n.Id)
		monsteraNode := monstera.NewNode(baseDir, n.Id, clusterConfig, badgerStore, monstera.DefaultMonsteraNodeConfig)

		monsteraNode.RegisterApplicationCore(&monstera.ApplicationCoreDescriptor{
			Name: "Core",
			CoreFactoryFunc: func(application *monstera.Application, shard *monstera.Shard, replica *monstera.Replica) monstera.ApplicationCore {
				return NewPlaygroundCore()
			},
			RestoreSnapshotOnStart: false,
		})

		lis, err := net.Listen("tcp", n.Address)
		if err != nil {
			panic(err)
		}

		monsteraServer := monstera.NewMonsteraServer(monsteraNode)

		grpcServer := grpc.NewServer()
		monstera.RegisterMonsteraApiServer(grpcServer, monsteraServer)

		nodes = append(nodes, localNode{
			monsteraNode: monsteraNode,
			lis:          lis,
			grpcServer:   grpcServer,
		})

		monsteraNode.Start()

		go func() {
			grpcServer.Serve(lis)
		}()
	}

	return nodes
}

func NewMonsteraStub(clusterConfig *monstera.ClusterConfig) *PlaygroundApiMonsteraStub {
	monsteraClient := monstera.NewMonsteraClient(clusterConfig)
	return NewPlaygroundApiMonsteraStub(monsteraClient)
}

func NewTestClusterConfig() *monstera.ClusterConfig {
	applications := []*monstera.Application{
		{
			Name:              "Core",
			Implementation:    "Core",
			ReplicationFactor: 3,
			Shards: []*monstera.Shard{
				{
					Id:                "shrd_01",
					LowerBound:        []byte{0x00, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0x3f, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x01},
					Replicas: []*monstera.Replica{
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
					Id:                "shrd_02",
					LowerBound:        []byte{0x40, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0x7f, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x02},
					Replicas: []*monstera.Replica{
						{
							Id:     "rplc_4",
							NodeId: "nd_01",
						},
						{
							Id:     "rplc_5",
							NodeId: "nd_02",
						},
						{
							Id:     "rplc_6",
							NodeId: "nd_03",
						},
					},
				},
				{
					Id:                "shrd_03",
					LowerBound:        []byte{0x80, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0xbf, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x03},
					Replicas: []*monstera.Replica{
						{
							Id:     "rplc_7",
							NodeId: "nd_01",
						},
						{
							Id:     "rplc_8",
							NodeId: "nd_02",
						},
						{
							Id:     "rplc_9",
							NodeId: "nd_03",
						},
					},
				},
				{
					Id:                "shrd_04",
					LowerBound:        []byte{0xc0, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x04},
					Replicas: []*monstera.Replica{
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

	nodes := []*monstera.Node{
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

	clusterConfig, err := monstera.LoadConfig(applications, nodes, time.Now().UnixMilli())
	if err != nil {
		panic(err)
	}

	return clusterConfig
}
