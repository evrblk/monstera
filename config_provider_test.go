package monstera

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/transport"
)

// fakeAdmin is a transport.AdminPlane test double. Only GetClusterConfig is
// exercised; the rest satisfy the interface.
type fakeAdmin struct {
	mu      sync.Mutex
	configs map[string]*cluster.Config // address -> config; missing address == unreachable
}

var _ transport.AdminPlane = (*fakeAdmin)(nil)

func (f *fakeAdmin) set(addr string, cfg *cluster.Config) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.configs == nil {
		f.configs = make(map[string]*cluster.Config)
	}
	f.configs[addr] = cfg
}

func (f *fakeAdmin) GetClusterConfig(ctx context.Context, address string) (*cluster.Config, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cfg, ok := f.configs[address]
	if !ok {
		return nil, fmt.Errorf("unreachable: %s", address)
	}
	return cfg, nil
}

func (f *fakeAdmin) Bootstrap(ctx context.Context, address, nodeId string, config *cluster.Config) error {
	return nil
}
func (f *fakeAdmin) UpdateClusterConfig(ctx context.Context, address string, config *cluster.Config) error {
	return nil
}
func (f *fakeAdmin) ListReplicaStates(ctx context.Context, address string) ([]*transport.ReplicaState, error) {
	return nil, nil
}
func (f *fakeAdmin) ListReplicaSnapshots(ctx context.Context, address, replicaId string) ([]*transport.RaftSnapshot, error) {
	return nil, nil
}
func (f *fakeAdmin) TriggerSnapshot(ctx context.Context, address, replicaId string) error { return nil }
func (f *fakeAdmin) LeadershipTransfer(ctx context.Context, address, replicaId string) error {
	return nil
}
func (f *fakeAdmin) Close() error { return nil }

func TestStaticClusterConfigProvider(t *testing.T) {
	cfg := &cluster.Config{Version: 7}
	p := NewStaticClusterConfigProvider(cfg)

	require.Same(t, cfg, p.Latest())
	require.NoError(t, p.Start(context.Background()))

	var got *cluster.Config
	unwatch := p.Watch(func(c *cluster.Config) { got = c })
	require.Same(t, cfg, got, "Watch must fire immediately with the current config")
	unwatch()
	p.Stop()
}

func TestPollingClusterConfigProviderAdoptsHighestVersion(t *testing.T) {
	admin := &fakeAdmin{}
	admin.set("a", &cluster.Config{Version: 1})
	admin.set("b", &cluster.Config{Version: 3})
	admin.set("c", &cluster.Config{Version: 2})

	disc := NewStaticNodeDiscovery([]string{"a", "b", "c"})
	p := NewPollingClusterConfigProvider(disc, admin, PollingOptions{Interval: 20 * time.Millisecond, Timeout: time.Second})

	require.NoError(t, p.Start(context.Background()))
	defer p.Stop()

	require.Eventually(t, func() bool {
		c := p.Latest()
		return c != nil && c.Version == 3
	}, 2*time.Second, 10*time.Millisecond, "highest version among endpoints must win")
}

func TestPollingClusterConfigProviderNonBlockingWhenUnreachable(t *testing.T) {
	admin := &fakeAdmin{} // nothing reachable yet
	disc := NewStaticNodeDiscovery([]string{"a"})
	p := NewPollingClusterConfigProvider(disc, admin, PollingOptions{Interval: 20 * time.Millisecond, Timeout: 200 * time.Millisecond})

	// Start must not block or fail even though no node is reachable.
	require.NoError(t, p.Start(context.Background()))
	defer p.Stop()
	require.Nil(t, p.Latest(), "no config should be adopted while unreachable")

	// Once a node becomes reachable, the provider adopts its config.
	admin.set("a", &cluster.Config{Version: 5})
	require.Eventually(t, func() bool {
		c := p.Latest()
		return c != nil && c.Version == 5
	}, 2*time.Second, 10*time.Millisecond)
}

func TestPollingClusterConfigProviderAdoptsNewerOnPoll(t *testing.T) {
	admin := &fakeAdmin{}
	admin.set("a", &cluster.Config{Version: 1})

	disc := NewStaticNodeDiscovery([]string{"a"})
	p := NewPollingClusterConfigProvider(disc, admin, PollingOptions{Interval: 20 * time.Millisecond, Timeout: time.Second})

	var mu sync.Mutex
	var versions []int64
	p.Watch(func(c *cluster.Config) {
		mu.Lock()
		versions = append(versions, c.Version)
		mu.Unlock()
	})

	require.NoError(t, p.Start(context.Background()))
	defer p.Stop()

	// Wait for v1 to be adopted before publishing v2, so the sequence is deterministic.
	require.Eventually(t, func() bool {
		return p.Latest() != nil && p.Latest().Version == 1
	}, 2*time.Second, 10*time.Millisecond)

	admin.set("a", &cluster.Config{Version: 2})
	require.Eventually(t, func() bool {
		return p.Latest() != nil && p.Latest().Version == 2
	}, 2*time.Second, 10*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []int64{1, 2}, versions, "watcher must see v1 then v2, and no re-fire for the same version")
}
