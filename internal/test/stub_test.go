package test

import (
	"context"
	"fmt"
	"log"
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
			n.grpcServer.Stop()
			n.monsteraNode.Stop()
		}
	}()

	t1 := time.Now()
	allReady := true
	for time.Now().Before(t1.Add(5 * time.Second)) {
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
		t.Fatalf("Nodes not ready after 5 seconds")
	}
	log.Println("Nodes are ready")

	time.Sleep(1 * time.Second)

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

	nodes[0].grpcServer.Stop()
	nodes[0].monsteraNode.Stop()

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

	log.Println("Test completed")
}

type localNode struct {
	monsteraNode *monstera.MonsteraNode
	lis          net.Listener
	grpcServer   *grpc.Server
}

func NewCluster(clusterConfig *monstera.ClusterConfig) []localNode {
	nodes := make([]localNode, 0)

	for _, n := range clusterConfig.Nodes {
		baseDir := fmt.Sprintf("/tmp/monstera/%d/%s", rand.Uint64(), n.Address)

		coreDescriptors := monstera.ApplicationCoreDescriptors{
			"Core": {
				CoreFactoryFunc: func(shard *monstera.Shard, replica *monstera.Replica) monstera.ApplicationCore {
					return NewPlaygroundCore()
				},
				RestoreSnapshotOnStart: false,
			},
		}

		nodeConfig := monstera.DefaultMonsteraNodeConfig
		nodeConfig.UseInMemoryRaftStore = true

		monsteraNode, err := monstera.NewNode(baseDir, n.Address, clusterConfig, coreDescriptors, nodeConfig)
		if err != nil {
			panic(err)
		}

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
			err := grpcServer.Serve(lis)
			if err != nil {
				panic(err)
			}
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
						{Id: "rplc_01", NodeAddress: "localhost:9001"},
						{Id: "rplc_02", NodeAddress: "localhost:9002"},
						{Id: "rplc_03", NodeAddress: "localhost:9003"},
					},
				},
				{
					Id:                "shrd_02",
					LowerBound:        []byte{0x40, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0x7f, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x02},
					Replicas: []*monstera.Replica{
						{Id: "rplc_4", NodeAddress: "localhost:9001"},
						{Id: "rplc_5", NodeAddress: "localhost:9002"},
						{Id: "rplc_6", NodeAddress: "localhost:9003"},
					},
				},
				{
					Id:                "shrd_03",
					LowerBound:        []byte{0x80, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0xbf, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x03},
					Replicas: []*monstera.Replica{
						{Id: "rplc_7", NodeAddress: "localhost:9001"},
						{Id: "rplc_8", NodeAddress: "localhost:9002"},
						{Id: "rplc_9", NodeAddress: "localhost:9003"},
					},
				},
				{
					Id:                "shrd_04",
					LowerBound:        []byte{0xc0, 0x00, 0x00, 0x00},
					UpperBound:        []byte{0xff, 0xff, 0xff, 0xff},
					GlobalIndexPrefix: []byte{0x00, 0x00, 0x00, 0x04},
					Replicas: []*monstera.Replica{
						{Id: "rplc_10", NodeAddress: "localhost:9001"},
						{Id: "rplc_11", NodeAddress: "localhost:9002"},
						{Id: "rplc_12", NodeAddress: "localhost:9003"},
					},
				},
			},
		},
	}

	nodes := []*monstera.Node{
		{Address: "localhost:9001"},
		{Address: "localhost:9002"},
		{Address: "localhost:9003"},
	}

	clusterConfig, err := monstera.LoadConfig(applications, nodes, time.Now().UnixMilli())
	if err != nil {
		panic(err)
	}

	return clusterConfig
}
