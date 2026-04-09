package test

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/transport/grpc"
)

func TestPlaygroundApiMonsteraStub_ReadAndUpdate(t *testing.T) {
	clusterConfig := NewTestClusterConfig()
	stub := NewMonsteraStub(clusterConfig)
	nodes := NewCluster(clusterConfig)
	defer func() {
		for _, n := range nodes {
			n.monsteraNode.Stop()
			n.monsteraServer.Stop()
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

	log.Println("Sending requests")
	for i := 0; i < 5000; i++ {
		// Test reading nonexistent key
		key := rand.Uint64()

		resp1, err := stub.Read(context.Background(), key)
		require.NoError(t, err)
		require.Empty(t, resp1, "Expected empty result for nonexistent key")

		// Update key
		value := fmt.Sprintf("test value %d", key)
		resp2, err := stub.Update(context.Background(), key, value)
		require.NoError(t, err)
		require.Equal(t, value, resp2)

		// Test reading existing key
		resp3, err := stub.Read(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, value, resp3)
	}

	log.Println("Killing a node")

	nodes[0].monsteraNode.Stop()
	nodes[0].monsteraServer.Stop()

	log.Println("Sending requests")
	for i := 0; i < 5000; i++ {
		key := rand.Uint64()

		// Update key
		value := fmt.Sprintf("test value %d", key)
		resp2, err := stub.Update(context.Background(), key, value)
		require.NoError(t, err)
		require.Equal(t, value, resp2)

		// Test reading existing key
		resp3, err := stub.Read(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, value, resp3)
	}

	log.Println("Test completed")
}

type localNode struct {
	monsteraNode   *monstera.Node
	monsteraServer *grpc.GrpcServer
}

func NewCluster(clusterConfig *cluster.Config) []localNode {
	nodes := make([]localNode, 0)

	for _, n := range clusterConfig.Nodes {
		baseDir := fmt.Sprintf("/tmp/monstera/%d/%s", rand.Uint64(), n.Id)

		coreDescriptors := monstera.ApplicationCoreDescriptors{
			"Core": {
				CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
					return NewPlaygroundCore()
				},
				RestoreSnapshotOnStart: false,
			},
		}

		trans := grpc.NewGrpcTransport(clusterConfig)

		nodeConfig := monstera.DefaultMonsteraNodeConfig
		nodeConfig.UseInMemoryRaftStore = true

		monsteraNode, err := monstera.NewNode(baseDir, n.Id, clusterConfig, coreDescriptors, nodeConfig, trans)
		if err != nil {
			panic(err)
		}

		monsteraServer := grpc.NewGrpcServer(monsteraNode)

		nodes = append(nodes, localNode{
			monsteraNode:   monsteraNode,
			monsteraServer: monsteraServer,
		})

		monsteraNode.Start()

		go func() {
			err := monsteraServer.Serve(n.GrpcAddress)
			if err != nil {
				panic(err)
			}
		}()
	}

	return nodes
}

func NewMonsteraStub(clusterConfig *cluster.Config) *PlaygroundApiMonsteraStub {
	trans := grpc.NewGrpcTransport(clusterConfig)
	monsteraClient := monstera.NewMonsteraClient(clusterConfig, trans, monstera.DefaultClientConfig())
	return NewPlaygroundApiMonsteraStub(monsteraClient)
}

func NewTestClusterConfig() *cluster.Config {
	applications := []*cluster.Application{
		{
			Name:              "Core",
			Implementation:    "Core",
			ReplicationFactor: 3,
			Shards: []*cluster.Shard{
				{
					Id:         "shrd_01",
					LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
					UpperBound: []byte{0x3f, 0xff, 0xff, 0xff},
					Replicas: []*cluster.Replica{
						{Id: "rplc_01", NodeId: "node_01"},
						{Id: "rplc_02", NodeId: "node_02"},
						{Id: "rplc_03", NodeId: "node_03"},
					},
				},
				{
					Id:         "shrd_02",
					LowerBound: []byte{0x40, 0x00, 0x00, 0x00},
					UpperBound: []byte{0x7f, 0xff, 0xff, 0xff},
					Replicas: []*cluster.Replica{
						{Id: "rplc_04", NodeId: "node_01"},
						{Id: "rplc_05", NodeId: "node_02"},
						{Id: "rplc_06", NodeId: "node_03"},
					},
				},
				{
					Id:         "shrd_03",
					LowerBound: []byte{0x80, 0x00, 0x00, 0x00},
					UpperBound: []byte{0xbf, 0xff, 0xff, 0xff},
					Replicas: []*cluster.Replica{
						{Id: "rplc_07", NodeId: "node_01"},
						{Id: "rplc_08", NodeId: "node_02"},
						{Id: "rplc_09", NodeId: "node_03"},
					},
				},
				{
					Id:         "shrd_04",
					LowerBound: []byte{0xc0, 0x00, 0x00, 0x00},
					UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
					Replicas: []*cluster.Replica{
						{Id: "rplc_10", NodeId: "node_01"},
						{Id: "rplc_11", NodeId: "node_02"},
						{Id: "rplc_12", NodeId: "node_03"},
					},
				},
			},
		},
	}

	nodes := []*cluster.Node{
		{Id: "node_01", GrpcAddress: "localhost:9001"},
		{Id: "node_02", GrpcAddress: "localhost:9002"},
		{Id: "node_03", GrpcAddress: "localhost:9003"},
	}

	clusterConfig, err := cluster.LoadConfig(applications, nodes, 1)
	if err != nil {
		panic(err)
	}

	return clusterConfig
}
