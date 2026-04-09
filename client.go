package monstera

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxRetriesOnSingleReplica = 10
)

type Client struct {
	mu            sync.RWMutex
	clusterConfig *cluster.Config
	ReplicaStates map[string]*transport.ReplicaState

	trans transport.Transport

	refresherCancel context.CancelFunc
}

func (c *Client) Stop() {
	log.Printf("Stopping Monstera Client")

	if c.refresherCancel != nil {
		c.refresherCancel()
	}

	c.trans.Close()
}

func (c *Client) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	c.refresherCancel = cancel

	go func(monstera *Client, ctx context.Context) {
		for {
			for _, n := range c.clusterConfig.ListNodes() {
				tctx, tcancel := context.WithTimeout(ctx, time.Millisecond*500)
				states, err := c.trans.HealthCheck(tctx, n.Id)
				tcancel()
				if err != nil {
					continue
				}

				c.mu.Lock()
				for _, s := range states {
					c.ReplicaStates[s.ReplicaId] = s
				}
				c.mu.Unlock()
			}

			duration := time.Duration(int32(rand.Int32N(1000))+5000) * time.Millisecond

			select {
			case <-ctx.Done():
				return
			case <-time.After(duration):
				// just wait
			}
		}
	}(c, ctx)
}

func (c *Client) Read(ctx context.Context, applicationName string, shardKey []byte, allowReadFromFollowers bool, payload []byte) ([]byte, error) {
	shard, err := c.clusterConfig.FindShardByShardKey(applicationName, shardKey)
	if err != nil {
		return nil, err
	}

	return c.readShard(ctx, applicationName, shard, shardKey, allowReadFromFollowers, payload)
}

func (c *Client) ReadShard(ctx context.Context, applicationName string, shardId string, allowReadFromFollowers bool, payload []byte) ([]byte, error) {
	shard, err := c.clusterConfig.GetShard(shardId)
	if err != nil {
		return nil, err
	}

	return c.readShard(ctx, applicationName, shard, []byte{}, allowReadFromFollowers, payload)
}

func (c *Client) readShard(ctx context.Context, applicationName string, shard *cluster.Shard, shardKey []byte, allowReadFromFollowers bool, payload []byte) ([]byte, error) {
	var replicas []*cluster.Replica
	if allowReadFromFollowers {
		replicas = c.shuffleReplicas(shard.Replicas)
	} else {
		replicas = c.shuffleReplicasAndLeaderFirst(shard.Replicas)
	}

	for _, r := range replicas {
		for range maxRetriesOnSingleReplica {
			result, err := c.trans.Read(ctx, r.NodeId, &transport.ReadRequest{
				ApplicationName:        applicationName,
				ShardId:                shard.Id,
				ReplicaId:              r.Id,
				ShardKey:               shardKey,
				Payload:                payload,
				AllowReadFromFollowers: allowReadFromFollowers,
				Hops:                   0,
			})
			if err != nil {
				if isErrorRetryableOnTheSameReplica(err) {
					time.Sleep(100 * time.Millisecond) // TODO: make this configurable
					continue
				}

				if isErrorForDeadReplica(err) {
					break
				}

				// Some other error, not retryable
				return nil, fmt.Errorf("monsteraClient.Read: %v", err)
			}

			return result.Payload, nil
		}

		// All retries failed, or a replica is dead, try next replica
		continue
	}

	return nil, fmt.Errorf("all replicas failed")
}

func (c *Client) Update(ctx context.Context, applicationName string, shardKey []byte, payload []byte) ([]byte, error) {
	shard, err := c.clusterConfig.FindShardByShardKey(applicationName, shardKey)
	if err != nil {
		return nil, err
	}

	return c.updateShard(ctx, applicationName, shard, shardKey, payload)
}

func (c *Client) UpdateShard(ctx context.Context, applicationName string, shardId string, payload []byte) ([]byte, error) {
	shard, err := c.clusterConfig.GetShard(shardId)
	if err != nil {
		return nil, err
	}

	return c.updateShard(ctx, applicationName, shard, []byte{}, payload)
}

func (c *Client) updateShard(ctx context.Context, applicationName string, shard *cluster.Shard, shardKey []byte, payload []byte) ([]byte, error) {
	replicas := c.shuffleReplicasAndLeaderFirst(shard.Replicas)

	for _, r := range replicas {
		for range maxRetriesOnSingleReplica {
			result, err := c.trans.Update(ctx, r.NodeId, &transport.UpdateRequest{
				ApplicationName: applicationName,
				ShardId:         shard.Id,
				ReplicaId:       r.Id,
				ShardKey:        shardKey,
				Payload:         payload,
				Hops:            0,
			})
			if err != nil {
				if isErrorRetryableOnTheSameReplica(err) {
					time.Sleep(500 * time.Millisecond) // TODO: make this configurable
					continue
				}

				if isErrorForDeadReplica(err) {
					break
				}

				// Some other error, not retryable
				return nil, fmt.Errorf("monsteraClient.Update: %v", err)
			}

			return result.Payload, nil
		}

		// All retries failed, or a replica is dead, try next replica
		continue
	}

	return nil, fmt.Errorf("all replicas failed")
}

func (c *Client) TriggerSnapshot(applicationName string, shardId string, replicaId string) error {
	replica, err := c.clusterConfig.GetReplica(replicaId)
	if err != nil {
		return err
	}

	return c.trans.TriggerSnapshot(context.Background(), replica.NodeId, &transport.TriggerSnapshotRequest{
		ReplicaId: replicaId,
	})
}

func (c *Client) LeadershipTransfer(applicationName string, shardId string, replicaId string) error {
	replica, err := c.clusterConfig.GetReplica(replicaId)
	if err != nil {
		return err
	}

	return c.trans.LeadershipTransfer(context.Background(), replica.NodeId, &transport.LeadershipTransferRequest{
		ReplicaId: replicaId,
	})
}

func (c *Client) ListShards(applicationName string) ([]*cluster.Shard, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	shards, err := c.clusterConfig.ListShards(applicationName)
	if err != nil {
		return nil, err
	}

	sortedShards := make([]*cluster.Shard, len(shards))
	copy(sortedShards, shards)

	sort.Slice(sortedShards, func(i, j int) bool {
		return bytes.Compare(sortedShards[i].LowerBound, sortedShards[j].LowerBound) < 0
	})

	return sortedShards, nil
}

func (c *Client) shuffleReplicas(replicas []*cluster.Replica) []*cluster.Replica {
	result := make([]*cluster.Replica, len(replicas))
	copy(result, replicas)
	rand.Shuffle(len(result), func(i, j int) {
		result[i], result[j] = result[j], result[i]
	})
	return result
}

func (c *Client) shuffleReplicasAndLeaderFirst(replicas []*cluster.Replica) []*cluster.Replica {
	result := make([]*cluster.Replica, len(replicas))
	result[0] = c.getLeader(replicas)
	otherReplicas := make([]*cluster.Replica, 0, len(replicas))
	for _, r := range replicas {
		if r.Id != result[0].Id {
			otherReplicas = append(otherReplicas, r)
		}
	}
	rand.Shuffle(len(otherReplicas), func(i, j int) {
		otherReplicas[i], otherReplicas[j] = otherReplicas[j], otherReplicas[i]
	})
	copy(result[1:], otherReplicas)
	return result
}

func (c *Client) getLeader(replicas []*cluster.Replica) *cluster.Replica {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, r := range replicas {
		s, ok := c.ReplicaStates[r.Id]
		if ok && s.IsLeader {
			return r
		}
	}
	return replicas[rand.IntN(len(replicas))] // this is a fallback
}

func NewMonsteraClient(clusterConfig *cluster.Config, trans transport.Transport) *Client {
	return &Client{
		clusterConfig: clusterConfig,
		trans:         trans,
		ReplicaStates: make(map[string]*transport.ReplicaState),
	}
}

func isErrorRetryableOnTheSameReplica(err error) bool {
	if st, ok := status.FromError(err); ok {
		if st.Message() == "leader is unknown" {
			return true
		}
	}

	return false
}

func isErrorForDeadReplica(err error) bool {
	if st, ok := status.FromError(err); ok {
		if st.Code() == codes.DeadlineExceeded || st.Code() == codes.Canceled || st.Code() == codes.Unavailable {
			return true
		}
	}

	return false
}
