package monstera

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/transport"
)

// ClusterConfigProvider supplies the current cluster config to a non-node process (a
// gateway, worker, or admin tool) and notifies it when a newer version is
// adopted. It is the single owner that a Client uses to keep both its routing
// table and its data plane in sync with the cluster topology.
type ClusterConfigProvider interface {
	// Latest returns the most recently adopted config, or nil before the first one
	// is available. Cheap and safe for concurrent use.
	Latest() *cluster.Config

	// Watch registers fn. If a config is already available, fn is called with it
	// synchronously before Watch returns; thereafter fn is called on every strictly
	// newer version adopted (monotonic by Config.Version). fn must not block. The
	// returned func unregisters the callback.
	Watch(fn func(*cluster.Config)) (unwatch func())

	// Start begins background discovery/polling. It does not block on, or fail for,
	// unreachable nodes: a PollingClusterConfigProvider returns immediately and adopts a
	// config as soon as one becomes available (Latest stays nil until then, and
	// callers should treat that as "not ready yet" rather than an error). Static
	// providers no-op.
	Start(ctx context.Context) error

	// Stop halts background work started by Start.
	Stop()
}

// StaticClusterConfigProvider always yields the same config. This is the pre-provider
// behavior: use it when the config is fixed for the process lifetime (tests, or a
// gateway that is restarted on config change).
type StaticClusterConfigProvider struct {
	cfg *cluster.Config
}

var _ ClusterConfigProvider = (*StaticClusterConfigProvider)(nil)

func NewStaticClusterConfigProvider(cfg *cluster.Config) *StaticClusterConfigProvider {
	return &StaticClusterConfigProvider{cfg: cfg}
}

func (p *StaticClusterConfigProvider) Latest() *cluster.Config { return p.cfg }

func (p *StaticClusterConfigProvider) Watch(fn func(*cluster.Config)) func() {
	if p.cfg != nil {
		fn(p.cfg)
	}
	return func() {}
}

func (p *StaticClusterConfigProvider) Start(ctx context.Context) error { return nil }

func (p *StaticClusterConfigProvider) Stop() {}

// PollingOptions tunes a PollingClusterConfigProvider.
type PollingOptions struct {
	// Interval is the wait between poll rounds. Defaults to 5s.
	Interval time.Duration
	// Timeout bounds each per-endpoint GetClusterConfig call. Defaults to 1s.
	Timeout time.Duration
}

// PollingClusterConfigProvider learns the config from the cluster itself: each round it
// asks a set of candidate nodes (from a NodeDiscovery, unioned with the nodes in
// the config it already holds) for their config over the AdminPlane and adopts the
// highest Version seen. Config versions are monotonic and transitions are
// validated cluster-side, so "highest version wins" is correct even mid-rollout
// when nodes briefly disagree. This is what makes a gateway self-healing — the
// cluster is the single source of truth, with no config files to keep in sync.
type PollingClusterConfigProvider struct {
	discovery NodeDiscovery
	admin     transport.AdminPlane
	interval  time.Duration
	timeout   time.Duration

	mu          sync.RWMutex
	current     *cluster.Config
	watchers    map[int]func(*cluster.Config)
	nextWatcher int

	cancel context.CancelFunc
	done   chan struct{}
}

var _ ClusterConfigProvider = (*PollingClusterConfigProvider)(nil)

func NewPollingClusterConfigProvider(discovery NodeDiscovery, admin transport.AdminPlane, opts PollingOptions) *PollingClusterConfigProvider {
	if opts.Interval <= 0 {
		opts.Interval = 5 * time.Second
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 1 * time.Second
	}
	return &PollingClusterConfigProvider{
		discovery: discovery,
		admin:     admin,
		interval:  opts.Interval,
		timeout:   opts.Timeout,
		watchers:  make(map[int]func(*cluster.Config)),
	}
}

func (p *PollingClusterConfigProvider) Latest() *cluster.Config {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.current
}

func (p *PollingClusterConfigProvider) Watch(fn func(*cluster.Config)) func() {
	p.mu.Lock()
	id := p.nextWatcher
	p.nextWatcher++
	p.watchers[id] = fn
	current := p.current
	p.mu.Unlock()

	if current != nil {
		fn(current)
	}

	return func() {
		p.mu.Lock()
		delete(p.watchers, id)
		p.mu.Unlock()
	}
}

func (p *PollingClusterConfigProvider) Start(ctx context.Context) error {
	loopCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.done = make(chan struct{})

	go func() {
		defer close(p.done)
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()
		for {
			// Poll immediately on entry, then every interval. Failures are non-fatal:
			// keep the last good config (or none yet) and retry. This makes Start
			// non-blocking — a gateway comes up and serves even if no node is
			// reachable, adopting a config as soon as one answers.
			_ = p.pollOnce(loopCtx)

			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	return nil
}

func (p *PollingClusterConfigProvider) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	if p.done != nil {
		<-p.done
	}
}

// pollOnce asks every candidate node for its config and adopts the highest
// version among the responses. It returns an error only when no node could be
// reached (so the first poll in Start can fail fast); once at least one node
// answers it succeeds.
func (p *PollingClusterConfigProvider) pollOnce(ctx context.Context) error {
	addrs, err := p.candidateAddresses(ctx)
	if err != nil {
		return err
	}
	if len(addrs) == 0 {
		return fmt.Errorf("no candidate node addresses from discovery")
	}

	var best *cluster.Config
	var lastErr error
	reached := false
	for _, addr := range addrs {
		cctx, cancel := context.WithTimeout(ctx, p.timeout)
		cfg, err := p.admin.GetClusterConfig(cctx, addr)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		reached = true
		if best == nil || cfg.Version > best.Version {
			best = cfg
		}
	}
	if !reached {
		return fmt.Errorf("no node reachable for cluster config: %v", lastErr)
	}

	p.adopt(best)
	return nil
}

// candidateAddresses is the union of the discovery endpoints and the addresses of
// the nodes in the config already held. If discovery fails but a config is already
// held, its nodes are used so a transient discovery outage doesn't stall polling.
func (p *PollingClusterConfigProvider) candidateAddresses(ctx context.Context) ([]string, error) {
	discovered, err := p.discovery.Endpoints(ctx)
	if err != nil {
		if p.Latest() == nil {
			return nil, fmt.Errorf("node discovery: %w", err)
		}
		discovered = nil
	}

	seen := make(map[string]bool)
	var out []string
	add := func(a string) {
		if a != "" && !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}
	for _, a := range discovered {
		add(a)
	}
	if cur := p.Latest(); cur != nil {
		for _, n := range cur.Nodes {
			add(n.GrpcAddress)
		}
	}
	return out, nil
}

// adopt swaps in cfg and notifies watchers, but only if cfg is strictly newer than
// the config currently held (monotonic by version). Watchers are invoked outside
// the lock.
func (p *PollingClusterConfigProvider) adopt(cfg *cluster.Config) {
	p.mu.Lock()
	if p.current != nil && cfg.Version <= p.current.Version {
		p.mu.Unlock()
		return
	}
	p.current = cfg
	watchers := make([]func(*cluster.Config), 0, len(p.watchers))
	for _, w := range p.watchers {
		watchers = append(watchers, w)
	}
	p.mu.Unlock()

	for _, w := range watchers {
		w(cfg)
	}
}
