package monstera

import (
	"context"
	"errors"
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

var (
	ErrNoClusterConfig   = errors.New("monstera client has no cluster config yet")
	ErrAllReplicasFailed = errors.New("all replicas failed")
	// ErrPayloadTooLarge is returned by Read/Update (and their *Shard variants)
	// when the payload exceeds the client's configured limit. It is checked on the
	// client before any node is contacted, so an oversized request never reaches a
	// Monstera node.
	ErrPayloadTooLarge = errors.New("payload exceeds maximum size")
)

const (
	defaultMaxRetriesOnSingleReplica = 10
	defaultListReplicaStatesTimeout  = 500 * time.Millisecond
	defaultRefreshIntervalBase       = 5000 * time.Millisecond
	defaultRefreshIntervalJitter     = 1000 * time.Millisecond
	defaultReadRetryDelay            = 100 * time.Millisecond
	defaultUpdateRetryDelay          = 500 * time.Millisecond
	defaultMaxReadPayloadBytes       = 1 << 20 // 1 MiB
	defaultMaxUpdatePayloadBytes     = 1 << 20 // 1 MiB
)

// ClientConfig holds tunable parameters for Client behavior.
type ClientConfig struct {
	// MaxRetriesOnSingleReplica is the number of times to retry a request on the
	// same replica before moving on to the next one.
	MaxRetriesOnSingleReplica int
	// ListReplicaStatesTimeout is the per-node timeout for each replica-state
	// refresh RPC.
	ListReplicaStatesTimeout time.Duration
	// RefreshIntervalBase is the minimum wait between replica-state refresh
	// rounds.
	RefreshIntervalBase time.Duration
	// RefreshIntervalJitter is the upper bound of random jitter added to
	// RefreshIntervalBase to spread refresh load across clients. Non-positive
	// means default.
	RefreshIntervalJitter time.Duration
	// ReadRetryDelay is how long to wait before retrying a read on the same
	// replica.
	ReadRetryDelay time.Duration
	// UpdateRetryDelay is how long to wait before retrying an update on the same
	// replica.
	UpdateRetryDelay time.Duration

	// MaxReadPayloadBytes is the maximum size in bytes of a Read payload. A Read
	// (or ReadShard) with a larger payload fails locally with ErrPayloadTooLarge
	// before any node is contacted. Zero or negative means the default (1 MiB).
	MaxReadPayloadBytes int
	// MaxUpdatePayloadBytes is the maximum size in bytes of an Update payload. An
	// Update (or UpdateShard) with a larger payload fails locally with
	// ErrPayloadTooLarge before any node is contacted. Zero or negative means the
	// default (1 MiB).
	MaxUpdatePayloadBytes int
}

// DefaultClientConfig returns a ClientConfig with sensible defaults.
func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		MaxRetriesOnSingleReplica: defaultMaxRetriesOnSingleReplica,
		ListReplicaStatesTimeout:  defaultListReplicaStatesTimeout,
		RefreshIntervalBase:       defaultRefreshIntervalBase,
		RefreshIntervalJitter:     defaultRefreshIntervalJitter,
		ReadRetryDelay:            defaultReadRetryDelay,
		UpdateRetryDelay:          defaultUpdateRetryDelay,
		MaxReadPayloadBytes:       defaultMaxReadPayloadBytes,
		MaxUpdatePayloadBytes:     defaultMaxUpdatePayloadBytes,
	}
}

// withDefaults returns the config with every non-positive field replaced by its
// default.
func (c ClientConfig) withDefaults() ClientConfig {
	if c.MaxRetriesOnSingleReplica <= 0 {
		c.MaxRetriesOnSingleReplica = defaultMaxRetriesOnSingleReplica
	}
	if c.ListReplicaStatesTimeout <= 0 {
		c.ListReplicaStatesTimeout = defaultListReplicaStatesTimeout
	}
	if c.RefreshIntervalBase <= 0 {
		c.RefreshIntervalBase = defaultRefreshIntervalBase
	}
	if c.RefreshIntervalJitter <= 0 {
		c.RefreshIntervalJitter = defaultRefreshIntervalJitter
	}
	if c.ReadRetryDelay <= 0 {
		c.ReadRetryDelay = defaultReadRetryDelay
	}
	if c.UpdateRetryDelay <= 0 {
		c.UpdateRetryDelay = defaultUpdateRetryDelay
	}
	if c.MaxReadPayloadBytes <= 0 {
		c.MaxReadPayloadBytes = defaultMaxReadPayloadBytes
	}
	if c.MaxUpdatePayloadBytes <= 0 {
		c.MaxUpdatePayloadBytes = defaultMaxUpdatePayloadBytes
	}
	return c
}

// Client is a Monstera cluster client that routes reads and updates to the
// correct shard replicas and keeps replica leadership state up to date via
// periodic health checks. It is ok for leadership state to be stale here,
// because Monstera nodes can forward requests to the current leader.
type Client struct {
	mu            sync.RWMutex
	clusterConfig *cluster.Config
	// router is the routing index built from clusterConfig; the two are swapped
	// together by onConfig.
	router        *Router
	replicaStates map[string]*transport.ReplicaState

	provider ClusterConfigProvider
	trans    transport.DataPlane
	config   ClientConfig

	unwatch         func()
	refresherCancel context.CancelFunc
	// refresherDone is closed by refreshLoop when it returns. Stop waits on it
	// before closing the transport so a refresh sweep in flight can never call
	// into a closed transport.
	refresherDone chan struct{}
}

// Stop unsubscribes from the config provider, stops it and the background
// health-check goroutine, and closes the transport. It blocks until the
// refresh loop has fully exited so the transport is not closed out from under
// an in-flight ListReplicaStates call.
func (c *Client) Stop() {
	log.Printf("Stopping Monstera Client")

	if c.refresherCancel != nil {
		c.refresherCancel()
		<-c.refresherDone
	}
	if c.unwatch != nil {
		c.unwatch()
	}
	c.provider.Stop()

	c.trans.Close()
}

// Start subscribes to the config provider (so topology changes flow into routing
// and the data plane), starts it, and launches the background goroutine that
// periodically polls nodes for replica states to identify shard leaders.
//
// Start does not block on the initial config: with a PollingClusterConfigProvider the
// client comes up immediately and begins routing as soon as a config is adopted;
// requests made before then return an error. It returns an error only if the
// provider itself fails to start.
func (c *Client) Start(ctx context.Context) error {
	// Subscribe before starting the provider so we observe the first config and
	// every change. Watch fires synchronously with the current config if one is
	// already available.
	c.unwatch = c.provider.Watch(c.onConfig)

	if err := c.provider.Start(ctx); err != nil {
		c.unwatch()
		c.unwatch = nil
		return err
	}

	refCtx, cancel := context.WithCancel(ctx)
	c.refresherCancel = cancel
	c.refresherDone = make(chan struct{})
	go c.refreshLoop(refCtx)

	return nil
}

// onConfig adopts a new cluster config: it swaps the routing config and pushes it
// into the data plane (if the data plane resolves addresses from a config).
func (c *Client) onConfig(cfg *cluster.Config) {
	c.mu.Lock()
	c.clusterConfig = cfg
	c.router = NewRouter(cfg)
	c.mu.Unlock()

	if cc, ok := c.trans.(transport.ClusterConfigConsumer); ok {
		cc.SetClusterConfig(cfg)
	}
}

// currentRouter returns the client's current routing index, snapshotting the
// pointer under the read lock (it is swapped wholesale by onConfig). It is nil
// until the first config is adopted.
func (c *Client) currentRouter() *Router {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.router
}

// currentConfig returns the client's current cluster config, snapshotting the
// pointer under the read lock (it is swapped wholesale by onConfig).
func (c *Client) currentConfig() *cluster.Config {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.clusterConfig
}

func (c *Client) refreshLoop(ctx context.Context) {
	defer close(c.refresherDone)
	for {
		if cfg := c.currentConfig(); cfg != nil {
			for _, n := range cfg.ListNodes() {
				tctx, tcancel := context.WithTimeout(ctx, c.config.ListReplicaStatesTimeout)
				states, err := c.trans.ListReplicaStates(tctx, n.Id)
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

			// Prune leadership state for replicas that no longer exist in the
			// current config: splits and shard moves retire replica ids
			// continuously, and without this replicaStates would grow without
			// bound. Prune against the latest router so a replica added by a
			// very recent config (not yet polled) is never dropped.
			c.pruneReplicaStates()
		}

		duration := c.config.RefreshIntervalBase + time.Duration(rand.Int64N(int64(c.config.RefreshIntervalJitter)))

		select {
		case <-ctx.Done():
			return
		case <-time.After(duration):
			// just wait
		}
	}
}

// pruneReplicaStates drops cached leadership state for any replica id that is no
// longer present in the current cluster config (retired by a split or a shard
// move). Called on every refresh sweep so the map tracks the live replica set.
func (c *Client) pruneReplicaStates() {
	router := c.currentRouter()
	if router == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for id := range c.replicaStates {
		if _, err := router.GetReplica(id); err != nil {
			delete(c.replicaStates, id)
		}
	}
}

// checkReadPayload enforces MaxReadPayloadBytes, falling back to the default
// limit when it is non-positive (so the limit holds even for a directly
// constructed Client whose config was not normalized via withDefaults).
func (c *Client) checkReadPayload(payload []byte) error {
	limit := c.config.MaxReadPayloadBytes
	if limit <= 0 {
		limit = defaultMaxReadPayloadBytes
	}
	if len(payload) > limit {
		return fmt.Errorf("%w: read payload is %d bytes, limit is %d", ErrPayloadTooLarge, len(payload), limit)
	}
	return nil
}

// checkUpdatePayload enforces MaxUpdatePayloadBytes, falling back to the default
// limit when it is non-positive (see checkReadPayload).
func (c *Client) checkUpdatePayload(payload []byte) error {
	limit := c.config.MaxUpdatePayloadBytes
	if limit <= 0 {
		limit = defaultMaxUpdatePayloadBytes
	}
	if len(payload) > limit {
		return fmt.Errorf("%w: update payload is %d bytes, limit is %d", ErrPayloadTooLarge, len(payload), limit)
	}
	return nil
}

// Read routes a read request to the shard responsible for shardKey.
func (c *Client) Read(ctx context.Context, applicationName string, shardKey cluster.ShardKey, allowReadFromFollowers bool, payload []byte) ([]byte, error) {
	if err := c.checkReadPayload(payload); err != nil {
		return nil, err
	}

	router := c.currentRouter()
	if router == nil {
		return nil, ErrNoClusterConfig
	}
	shard, err := router.FindShardByShardKey(applicationName, shardKey)
	if err != nil {
		return nil, err
	}

	return c.readShard(ctx, applicationName, shard, shardKey, true, allowReadFromFollowers, payload)
}

// ReadShard sends a read request directly to the specified shard by ID,
// bypassing shard-key routing.
func (c *Client) ReadShard(ctx context.Context, applicationName string, shardId string, allowReadFromFollowers bool, payload []byte) ([]byte, error) {
	if err := c.checkReadPayload(payload); err != nil {
		return nil, err
	}

	router := c.currentRouter()
	if router == nil {
		return nil, ErrNoClusterConfig
	}
	shard, err := router.GetShard(shardId)
	if err != nil {
		return nil, err
	}

	return c.readShard(ctx, applicationName, shard, 0, false, allowReadFromFollowers, payload)
}

// readShard tries each replica in turn, retrying transient errors on the same
// replica up to MaxRetriesOnSingleReplica times before moving to the next.
func (c *Client) readShard(ctx context.Context, applicationName string, shard *cluster.Shard, shardKey cluster.ShardKey, hasShardKey bool, allowReadFromFollowers bool, payload []byte) ([]byte, error) {
	var replicas []*cluster.Replica
	if allowReadFromFollowers {
		replicas = c.shuffleReplicas(shard.Replicas)
	} else {
		replicas = c.shuffleReplicasAndLeaderFirst(shard.Replicas)
	}

	req := &transport.ReadRequest{
		ApplicationName:        applicationName,
		ShardId:                shard.Id,
		ShardKey:               shardKey,
		HasShardKey:            hasShardKey,
		Payload:                payload,
		AllowReadFromFollowers: allowReadFromFollowers,
		Hops:                   0,
	}

	for _, r := range replicas {
		for range c.config.MaxRetriesOnSingleReplica {
			resp, err := c.trans.Read(ctx, r.NodeId, req)
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

			return resp.Payload, nil
		}

		// All retries failed, or a replica is dead, try next replica
		continue
	}

	return nil, ErrAllReplicasFailed
}

// Update routes a write request to the shard responsible for shardKey.
func (c *Client) Update(ctx context.Context, applicationName string, shardKey cluster.ShardKey, payload []byte) ([]byte, error) {
	if err := c.checkUpdatePayload(payload); err != nil {
		return nil, err
	}

	router := c.currentRouter()
	if router == nil {
		return nil, ErrNoClusterConfig
	}
	shard, err := router.FindShardByShardKey(applicationName, shardKey)
	if err != nil {
		return nil, err
	}

	return c.updateShard(ctx, applicationName, shard, shardKey, true, payload)
}

// UpdateShard sends a write request directly to the specified shard by ID,
// bypassing shard-key routing.
func (c *Client) UpdateShard(ctx context.Context, applicationName string, shardId string, payload []byte) ([]byte, error) {
	if err := c.checkUpdatePayload(payload); err != nil {
		return nil, err
	}

	router := c.currentRouter()
	if router == nil {
		return nil, ErrNoClusterConfig
	}
	shard, err := router.GetShard(shardId)
	if err != nil {
		return nil, err
	}

	return c.updateShard(ctx, applicationName, shard, 0, false, payload)
}

// updateShard tries replicas leader-first, retrying transient errors on the
// same replica up to MaxRetriesOnSingleReplica times before moving to the next.
func (c *Client) updateShard(ctx context.Context, applicationName string, shard *cluster.Shard, shardKey cluster.ShardKey, hasShardKey bool, payload []byte) ([]byte, error) {
	replicas := c.shuffleReplicasAndLeaderFirst(shard.Replicas)

	req := &transport.UpdateRequest{
		ApplicationName: applicationName,
		ShardId:         shard.Id,
		ShardKey:        shardKey,
		HasShardKey:     hasShardKey,
		Payload:         payload,
		Hops:            0,
	}

	for _, r := range replicas {
		for range c.config.MaxRetriesOnSingleReplica {
			resp, err := c.trans.Update(ctx, r.NodeId, req)
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

			return resp.Payload, nil
		}

		// All retries failed, or a replica is dead, try next replica
		continue
	}

	return nil, ErrAllReplicasFailed
}

// ListShards returns the application's currently routable shards (active or
// splitting), sorted by lower bound. These are exactly the shards that serve the
// keyspace, so it is the set to fan a request out over (e.g. running GC on every
// shard); retired (inactive) and not-yet-serving (activating) shards are
// excluded. See Router.ListRoutableShards.
func (c *Client) ListShards(applicationName string) ([]*cluster.Shard, error) {
	router := c.currentRouter()
	if router == nil {
		return nil, ErrNoClusterConfig
	}
	return router.ListRoutableShards(applicationName)
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

// NewMonsteraClient creates a Client fed by the given config provider. Call Start
// to subscribe to config changes and begin background leader-state health checks.
//
// If the provider already has a config (e.g. a StaticClusterConfigProvider), it is
// adopted eagerly here so the client is usable without Start; a
// PollingClusterConfigProvider has no config until Start, so gateways must call Start.
//
// Non-positive config fields are replaced by their defaults, so a hand-built
// ClientConfig never has to set every knob.
func NewMonsteraClient(provider ClusterConfigProvider, trans transport.DataPlane, config ClientConfig) *Client {
	c := &Client{
		provider:      provider,
		trans:         trans,
		config:        config.withDefaults(),
		replicaStates: make(map[string]*transport.ReplicaState),
	}

	if cfg := provider.Latest(); cfg != nil {
		c.onConfig(cfg)
	}

	return c
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
