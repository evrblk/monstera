package monstera

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/transport"
)

// ClientConfig holds tunable parameters for Client behavior.
type ClientConfig struct {
	// MaxRetriesOnSingleReplica is the number of times to retry a request on the
	// same replica before moving on to the next one.
	MaxRetriesOnSingleReplica int
	// HealthCheckTimeout is the per-node timeout for each health check RPC.
	HealthCheckTimeout time.Duration
	// RefreshIntervalBase is the minimum wait between health check rounds.
	RefreshIntervalBase time.Duration
	// RefreshIntervalJitter is the upper bound of random jitter added to
	// RefreshIntervalBase to spread health check load across clients.
	RefreshIntervalJitter time.Duration
	// ReadRetryDelay is how long to wait before retrying a read on the same replica.
	ReadRetryDelay time.Duration
	// UpdateRetryDelay is how long to wait before retrying an update on the same replica.
	UpdateRetryDelay time.Duration
}

// DefaultClientConfig returns a ClientConfig with sensible defaults.
func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		MaxRetriesOnSingleReplica: 10,
		HealthCheckTimeout:        500 * time.Millisecond,
		RefreshIntervalBase:       5000 * time.Millisecond,
		RefreshIntervalJitter:     1000 * time.Millisecond,
		ReadRetryDelay:            100 * time.Millisecond,
		UpdateRetryDelay:          500 * time.Millisecond,
	}
}

// Client is a Monstera cluster client that routes reads and updates to the
// correct shard replicas and keeps replica leadership state up to date via
// periodic health checks. It is ok for leadership state to be stale here,
// because Monstera nodes can forward requests to the current leader.
type Client struct {
	mu            sync.RWMutex
	clusterConfig *cluster.Config
	replicaStates map[string]*transport.ReplicaState

	trans  transport.Transport
	config ClientConfig

	refresherCancel context.CancelFunc
}

// Stop cancels the background health-check goroutine and closes the transport.
func (c *Client) Stop() {
	log.Printf("Stopping Monstera Client")

	if c.refresherCancel != nil {
		c.refresherCancel()
	}

	c.trans.Close()
}

// Start launches the background goroutine that periodically polls all nodes
// for replica health state, used to identify the current leader of each shard.
func (c *Client) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	c.refresherCancel = cancel

	go func(monstera *Client, ctx context.Context) {
		for {
			for _, n := range c.clusterConfig.ListNodes() {
				tctx, tcancel := context.WithTimeout(ctx, c.config.HealthCheckTimeout)
				states, err := c.trans.HealthCheck(tctx, n.Id)
				tcancel()
				if err != nil {
					continue
				}

				c.mu.Lock()
				for _, s := range states {
					c.replicaStates[s.ReplicaId] = s
				}
				c.mu.Unlock()
			}

			duration := c.config.RefreshIntervalBase + time.Duration(rand.Int64N(int64(c.config.RefreshIntervalJitter)))

			select {
			case <-ctx.Done():
				return
			case <-time.After(duration):
				// just wait
			}
		}
	}(c, ctx)
}

// Read routes a read request to the shard responsible for shardKey.
func (c *Client) Read(ctx context.Context, applicationName string, shardKey []byte, allowReadFromFollowers bool, payload []byte) ([]byte, error) {
	shard, err := c.clusterConfig.FindShardByShardKey(applicationName, shardKey)
	if err != nil {
		return nil, err
	}

	return c.readShard(ctx, applicationName, shard, shardKey, allowReadFromFollowers, payload)
}

// ReadShard sends a read request directly to the specified shard by ID,
// bypassing shard-key routing.
func (c *Client) ReadShard(ctx context.Context, applicationName string, shardId string, allowReadFromFollowers bool, payload []byte) ([]byte, error) {
	shard, err := c.clusterConfig.GetShard(shardId)
	if err != nil {
		return nil, err
	}

	return c.readShard(ctx, applicationName, shard, []byte{}, allowReadFromFollowers, payload)
}

// readShard tries each replica in turn, retrying transient errors on the same
// replica up to MaxRetriesOnSingleReplica times before moving to the next.
func (c *Client) readShard(ctx context.Context, applicationName string, shard *cluster.Shard, shardKey []byte, allowReadFromFollowers bool, payload []byte) ([]byte, error) {
	var replicas []*cluster.Replica
	if allowReadFromFollowers {
		replicas = c.shuffleReplicas(shard.Replicas)
	} else {
		replicas = c.shuffleReplicasAndLeaderFirst(shard.Replicas)
	}

	for _, r := range replicas {
		for range c.config.MaxRetriesOnSingleReplica {
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
					time.Sleep(c.config.ReadRetryDelay)
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

// Update routes a write request to the shard responsible for shardKey.
func (c *Client) Update(ctx context.Context, applicationName string, shardKey []byte, payload []byte) ([]byte, error) {
	shard, err := c.clusterConfig.FindShardByShardKey(applicationName, shardKey)
	if err != nil {
		return nil, err
	}

	return c.updateShard(ctx, applicationName, shard, shardKey, payload)
}

// UpdateShard sends a write request directly to the specified shard by ID,
// bypassing shard-key routing.
func (c *Client) UpdateShard(ctx context.Context, applicationName string, shardId string, payload []byte) ([]byte, error) {
	shard, err := c.clusterConfig.GetShard(shardId)
	if err != nil {
		return nil, err
	}

	return c.updateShard(ctx, applicationName, shard, []byte{}, payload)
}

// updateShard tries replicas leader-first, retrying transient errors on the
// same replica up to MaxRetriesOnSingleReplica times before moving to the next.
func (c *Client) updateShard(ctx context.Context, applicationName string, shard *cluster.Shard, shardKey []byte, payload []byte) ([]byte, error) {
	replicas := c.shuffleReplicasAndLeaderFirst(shard.Replicas)

	for _, r := range replicas {
		for range c.config.MaxRetriesOnSingleReplica {
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
					time.Sleep(c.config.UpdateRetryDelay)
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

func (c *Client) ListShards(applicationName string) ([]*cluster.Shard, error) {
	return c.clusterConfig.ListShards(applicationName)
}

// shuffleReplicas returns a randomly ordered copy of replicas.
func (c *Client) shuffleReplicas(replicas []*cluster.Replica) []*cluster.Replica {
	result := make([]*cluster.Replica, len(replicas))
	copy(result, replicas)
	rand.Shuffle(len(result), func(i, j int) {
		result[i], result[j] = result[j], result[i]
	})
	return result
}

// shuffleReplicasAndLeaderFirst returns a copy of replicas with the known
// leader placed first and the remaining replicas in random order.
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

// getLeader returns the replica currently known to be the leader, or a random
// replica if no leader is cached yet.
func (c *Client) getLeader(replicas []*cluster.Replica) *cluster.Replica {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, r := range replicas {
		s, ok := c.replicaStates[r.Id]
		if ok && s.RaftState == transport.RaftStateLeader {
			return r
		}
	}
	return replicas[rand.IntN(len(replicas))] // this is a fallback
}

// NewMonsteraClient creates a Client. Call Start to begin background health checks.
func NewMonsteraClient(clusterConfig *cluster.Config, trans transport.Transport, config ClientConfig) *Client {
	return &Client{
		clusterConfig: clusterConfig,
		trans:         trans,
		config:        config,
		replicaStates: make(map[string]*transport.ReplicaState),
	}
}

// isErrorRetryableOnTheSameReplica reports whether the error indicates a
// transient condition (e.g. leader election in progress) that may resolve on
// the same replica without switching to another.
func isErrorRetryableOnTheSameReplica(err error) bool {
	if st, ok := status.FromError(err); ok {
		if st.Message() == "leader is unknown" {
			return true
		}
	}

	return false
}

// isErrorForDeadReplica reports whether the error indicates the replica is
// unreachable, so the caller should move on to the next replica immediately.
func isErrorForDeadReplica(err error) bool {
	if st, ok := status.FromError(err); ok {
		if st.Code() == codes.DeadlineExceeded || st.Code() == codes.Canceled || st.Code() == codes.Unavailable {
			return true
		}
	}

	return false
}
