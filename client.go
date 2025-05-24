package monstera

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/samber/lo"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type MonsteraClient struct {
	mu            sync.RWMutex
	clusterConfig *ClusterConfig
	grpcClients   map[string]monsteraGrpcClient

	ReplicaStates map[string]*ReplicaState

	refresherCancel context.CancelFunc
}

type monsteraGrpcClient struct {
	client MonsteraApiClient
	conn   *grpc.ClientConn
}

func (c *MonsteraClient) Stop() {
	log.Printf("Stopping Monstera Client")

	if c.refresherCancel != nil {
		c.refresherCancel()
	}

	for _, grpcClient := range c.grpcClients {
		err := grpcClient.conn.Close()
		if err != nil {
			log.Printf("Error closing grpc client: %v", err)
		}
	}
}

func (c *MonsteraClient) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	c.refresherCancel = cancel

	go func(monstera *MonsteraClient, ctx context.Context) {
		for {
			for _, n := range c.clusterConfig.ListNodes() {
				grpcClient := c.getClient(n.Id)

				tctx, tcancel := context.WithTimeout(ctx, time.Millisecond*500)
				defer tcancel()

				resp, err := grpcClient.client.HealthCheck(tctx, &HealthCheckRequest{})
				if err != nil {
					continue
				}

				c.mu.Lock()
				for _, r := range resp.Replicas {
					c.ReplicaStates[r.ReplicaId] = r
				}
				c.mu.Unlock()
			}

			duration := time.Duration(int32(rand.Int32N(10000))+55000) * time.Millisecond

			select {
			case <-ctx.Done():
				return
			case <-time.After(duration):
				// just wait
			}
		}
	}(c, ctx)
}

func (c *MonsteraClient) Read(ctx context.Context, applicationName string, shardKey []byte, allowReadFromFollowers bool, request proto.Message, response proto.Message) error {
	shard, err := c.clusterConfig.FindShard(applicationName, shardKey)
	if err != nil {
		return err
	}

	return c.readShard(ctx, applicationName, shard, shardKey, allowReadFromFollowers, request, response)
}

func (c *MonsteraClient) ReadShard(ctx context.Context, applicationName string, shardId string, allowReadFromFollowers bool, request proto.Message, response proto.Message) error {
	shard, err := c.clusterConfig.GetShard(shardId)
	if err != nil {
		return err
	}

	return c.readShard(ctx, applicationName, shard, []byte{}, allowReadFromFollowers, request, response)
}

func (c *MonsteraClient) readShard(ctx context.Context, applicationName string, shard *Shard, shardKey []byte, allowReadFromFollowers bool, request proto.Message, response proto.Message) error {
	payload, err := proto.Marshal(request)
	if err != nil {
		return fmt.Errorf("proto.Marshal: %v", err)
	}

	var r *Replica
	if allowReadFromFollowers {
		r = c.getAny(shard.Replicas)
	} else {
		r = c.getLeader(shard.Replicas)
	}

	readRequest := ReadRequest{
		Payload:                payload,
		ShardKey:               shardKey,
		ApplicationName:        applicationName,
		ShardId:                shard.Id,
		ReplicaId:              r.Id,
		AllowReadFromFollowers: allowReadFromFollowers,
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	api := c.getClient(r.NodeId)
	resp, err := api.client.Read(ctx, &readRequest)
	if err != nil {
		return fmt.Errorf("monsteraClient.Read: %v", err)
	}

	return proto.Unmarshal(resp.Payload, response)
}

func (c *MonsteraClient) Update(ctx context.Context, applicationName string, shardKey []byte, request proto.Message, response proto.Message) error {
	shard, err := c.clusterConfig.FindShard(applicationName, shardKey)
	if err != nil {
		return err
	}

	return c.updateShard(ctx, applicationName, shard, shardKey, request, response)
}

func (c *MonsteraClient) UpdateShard(ctx context.Context, applicationName string, shardId string, request proto.Message, response proto.Message) error {
	shard, err := c.clusterConfig.GetShard(shardId)
	if err != nil {
		return err
	}

	return c.updateShard(ctx, applicationName, shard, []byte{}, request, response)
}

func (c *MonsteraClient) updateShard(ctx context.Context, applicationName string, shard *Shard, shardKey []byte, request proto.Message, response proto.Message) error {
	payload, err := proto.Marshal(request)
	if err != nil {
		return fmt.Errorf("proto.Marshal: %v", err)
	}

	replicas := shard.Replicas

	r := c.getLeader(replicas)

	updateRequest := UpdateRequest{
		Payload:         payload,
		ShardKey:        shardKey,
		ApplicationName: applicationName,
		ShardId:         shard.Id,
		ReplicaId:       r.Id,
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	api := c.getClient(r.NodeId)
	resp, err := api.client.Update(ctx, &updateRequest)
	if err != nil {
		return fmt.Errorf("monsteraClient.Update: %v", err)
	}

	return proto.Unmarshal(resp.Payload, response)
}

func (c *MonsteraClient) getLeader(replicas []*Replica) *Replica {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, r := range replicas {
		s, ok := c.ReplicaStates[r.Id]
		if ok && s.RaftState == RaftState_RAFT_STATE_LEADER {
			return r
		}
	}
	return lo.Sample(replicas) // this is a fallback
}

func (c *MonsteraClient) getAny(replicas []*Replica) *Replica {
	return lo.Sample(replicas)
}

func (c *MonsteraClient) getClient(nodeId string) monsteraGrpcClient {
	c.mu.Lock()
	defer c.mu.Unlock()

	client, ok := c.grpcClients[nodeId]
	if ok {
		return client
	}

	node, err := c.clusterConfig.GetNode(nodeId)
	if err != nil {
		panic("Node not found")
	}

	conn, err := grpc.Dial(node.Address, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	client = monsteraGrpcClient{client: NewMonsteraApiClient(conn), conn: conn}
	c.grpcClients[node.Id] = client
	return client
}

func (c *MonsteraClient) ListApplications() []*Application {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.clusterConfig.ListApplications()
}

func (c *MonsteraClient) ListShards(applicationName string) ([]*Shard, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	shards, err := c.clusterConfig.ListShards(applicationName)
	if err != nil {
		return nil, err
	}

	return shards, nil
}

func NewMonsteraClient(clusterConfig *ClusterConfig) *MonsteraClient {
	return &MonsteraClient{
		clusterConfig: clusterConfig,
		grpcClients:   make(map[string]monsteraGrpcClient),
		ReplicaStates: make(map[string]*ReplicaState),
	}
}

func GetShardKey(key []byte, size int) []byte {
	h := sha256.New()
	h.Write(key)
	return h.Sum(nil)[0:size]
}
