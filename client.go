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
)

type MonsteraClient struct {
	mu            sync.RWMutex
	clusterConfig *ClusterConfig
	pool          *MonsteraConnectionPool

	ReplicaStates map[string]*ReplicaState

	refresherCancel context.CancelFunc
}

func (c *MonsteraClient) Stop() {
	log.Printf("Stopping Monstera Client")

	if c.refresherCancel != nil {
		c.refresherCancel()
	}

	c.pool.Close()
}

func (c *MonsteraClient) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	c.refresherCancel = cancel

	go func(monstera *MonsteraClient, ctx context.Context) {
		for {
			for _, n := range c.clusterConfig.ListNodes() {
				conn, err := c.pool.GetConnection(n.Address)
				if err != nil {
					//log.Println(err)
					continue
				}

				tctx, tcancel := context.WithTimeout(ctx, time.Millisecond*500)
				defer tcancel()

				resp, err := conn.HealthCheck(tctx, &HealthCheckRequest{})
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

func (c *MonsteraClient) Read(ctx context.Context, applicationName string, shardKey []byte, allowReadFromFollowers bool, payload []byte) ([]byte, error) {
	shard, err := c.clusterConfig.FindShard(applicationName, shardKey)
	if err != nil {
		return nil, err
	}

	return c.readShard(ctx, applicationName, shard, shardKey, allowReadFromFollowers, payload)
}

func (c *MonsteraClient) ReadShard(ctx context.Context, applicationName string, shardId string, allowReadFromFollowers bool, payload []byte) ([]byte, error) {
	shard, err := c.clusterConfig.GetShard(shardId)
	if err != nil {
		return nil, err
	}

	return c.readShard(ctx, applicationName, shard, []byte{}, allowReadFromFollowers, payload)
}

func (c *MonsteraClient) readShard(ctx context.Context, applicationName string, shard *Shard, shardKey []byte, allowReadFromFollowers bool, payload []byte) ([]byte, error) {
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

	n, err := c.clusterConfig.GetNode(r.NodeId)
	if err != nil {
		return nil, fmt.Errorf("clusterConfig.GetNode: %v", err)
	}

	conn, err := c.pool.GetConnection(n.Address)
	if err != nil {
		return nil, fmt.Errorf("pool.GetConnection: %v", err)
	}

	resp, err := conn.Read(ctx, &readRequest)
	if err != nil {
		return nil, fmt.Errorf("monsteraClient.Read: %v", err)
	}

	return resp.Payload, nil
}

func (c *MonsteraClient) Update(ctx context.Context, applicationName string, shardKey []byte, payload []byte) ([]byte, error) {
	shard, err := c.clusterConfig.FindShard(applicationName, shardKey)
	if err != nil {
		return nil, err
	}

	return c.updateShard(ctx, applicationName, shard, shardKey, payload)
}

func (c *MonsteraClient) UpdateShard(ctx context.Context, applicationName string, shardId string, payload []byte) ([]byte, error) {
	shard, err := c.clusterConfig.GetShard(shardId)
	if err != nil {
		return nil, err
	}

	return c.updateShard(ctx, applicationName, shard, []byte{}, payload)
}

func (c *MonsteraClient) updateShard(ctx context.Context, applicationName string, shard *Shard, shardKey []byte, payload []byte) ([]byte, error) {
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

	n, err := c.clusterConfig.GetNode(r.NodeId)
	if err != nil {
		return nil, fmt.Errorf("clusterConfig.GetNode: %v", err)
	}

	conn, err := c.pool.GetConnection(n.Address)
	if err != nil {
		return nil, fmt.Errorf("pool.GetConnection: %v", err)
	}

	resp, err := conn.Update(ctx, &updateRequest)
	if err != nil {
		return nil, fmt.Errorf("monsteraClient.Update: %v", err)
	}

	return resp.Payload, nil
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

func NewMonsteraClient(clusterConfig *ClusterConfig) *MonsteraClient {
	return &MonsteraClient{
		clusterConfig: clusterConfig,
		pool:          NewMonsteraConnectionPool(),
		ReplicaStates: make(map[string]*ReplicaState),
	}
}

func GetShardKey(key []byte, size int) []byte {
	h := sha256.New()
	h.Write(key)
	return h.Sum(nil)[0:size]
}
