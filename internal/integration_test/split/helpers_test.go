package split

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/internal/integration_test/testcore"
	"github.com/evrblk/monstera/internal/integration_test/testutils"
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/transport"
	"github.com/evrblk/monstera/transport/grpc"
)

const (
	parentShard = "Core_p"
	child1Shard = "Core_c1" // [0x00000000, 0x7fffffff]
	child2Shard = "Core_c2" // [0x80000000, 0xffffffff]
)

// coreCase parameterizes a split test by the application core's storage
// model. newDescriptors builds one node's descriptors; it receives that
// node's core store (nil unless usesStore).
type coreCase struct {
	name           string
	usesStore      bool
	newDescriptors func(s *store.BadgerStore) monstera.ApplicationCoreDescriptors
}

func coreCases() []coreCase {
	return []coreCase{
		{
			name: "InMemory",
			newDescriptors: func(*store.BadgerStore) monstera.ApplicationCoreDescriptors {
				return testcore.InMemoryPlaygroundDescriptors()
			},
		},
		{
			name:           "PersistedShared",
			usesStore:      true,
			newDescriptors: testcore.SharedPlaygroundDescriptors,
		},
		{
			name:           "PersistedExclusive",
			usesStore:      true,
			newDescriptors: testcore.ExclusivePlaygroundDescriptors,
		},
	}
}

// coreStores manages the per-node Badger stores backing the persisted
// playground cores, so a test can close one across a node kill and reopen it
// for the restart. For core types without a store every method no-ops and
// get/reopen return nil.
type coreStores struct {
	t      *testing.T
	used   bool
	dirs   []string
	stores []*store.BadgerStore
}

func newCoreStores(t *testing.T, cc coreCase, n int) *coreStores {
	t.Helper()
	cs := &coreStores{t: t, used: cc.usesStore}
	if !cs.used {
		return cs
	}
	cs.dirs = make([]string, n)
	cs.stores = make([]*store.BadgerStore, n)
	for i := range cs.stores {
		cs.dirs[i] = t.TempDir()
		cs.stores[i] = openCoreStore(t, cs.dirs[i])
	}
	t.Cleanup(func() {
		for i := range cs.stores {
			cs.close(i)
		}
	})
	return cs
}

func openCoreStore(t *testing.T, dir string) *store.BadgerStore {
	t.Helper()
	s, err := store.NewBadgerStore(store.DefaultOptions(dir).WithSyncWrites(false))
	require.NoError(t, err)
	return s
}

func (cs *coreStores) get(i int) *store.BadgerStore {
	if !cs.used {
		return nil
	}
	return cs.stores[i]
}

func (cs *coreStores) close(i int) {
	if !cs.used || cs.stores[i] == nil {
		return
	}
	cs.stores[i].Close()
	cs.stores[i] = nil
}

func (cs *coreStores) reopen(i int) *store.BadgerStore {
	if !cs.used {
		return nil
	}
	cs.stores[i] = openCoreStore(cs.t, cs.dirs[i])
	return cs.stores[i]
}

// splitTestConfig builds the test topology at a given phase, encoded by
// version: 1 = single active parent; 2, 3 = parent splitting with two
// co-located activating children; 4 = parent inactive, children active.
func splitTestConfig(addrs [3]string, version int64) *cluster.Config {
	parentState := cluster.ShardState_SHARD_STATE_ACTIVE
	childState := cluster.ShardState_SHARD_STATE_ACTIVATING
	withChildren := version >= 2
	if withChildren {
		parentState = cluster.ShardState_SHARD_STATE_SPLITTING
	}
	if version >= 4 {
		parentState = cluster.ShardState_SHARD_STATE_INACTIVE
		childState = cluster.ShardState_SHARD_STATE_ACTIVE
	}

	shards := []*cluster.Shard{
		{
			Id:         parentShard,
			LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
			UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
			State:      parentState,
			Replicas:   replicasFor(parentShard),
		},
	}
	if withChildren {
		shards = append(shards,
			&cluster.Shard{
				Id:         child1Shard,
				LowerBound: []byte{0x00, 0x00, 0x00, 0x00},
				UpperBound: []byte{0x7f, 0xff, 0xff, 0xff},
				State:      childState,
				ParentId:   parentShard,
				Replicas:   replicasFor(child1Shard),
			},
			&cluster.Shard{
				Id:         child2Shard,
				LowerBound: []byte{0x80, 0x00, 0x00, 0x00},
				UpperBound: []byte{0xff, 0xff, 0xff, 0xff},
				State:      childState,
				ParentId:   parentShard,
				Replicas:   replicasFor(child2Shard),
			},
		)
	}

	return &cluster.Config{
		Version: version,
		Applications: []*cluster.Application{
			{
				Name:              "Core",
				Implementation:    "Core",
				ReplicationFactor: 3,
				Shards:            shards,
			},
		},
		Nodes: []*cluster.Node{
			{Id: "node_1", GrpcAddress: addrs[0]},
			{Id: "node_2", GrpcAddress: addrs[1]},
			{Id: "node_3", GrpcAddress: addrs[2]},
		},
	}
}

func replicasFor(shardId string) []*cluster.Replica {
	replicas := make([]*cluster.Replica, 3)
	for i := 0; i < 3; i++ {
		replicas[i] = &cluster.Replica{
			Id:     fmt.Sprintf("%s_r%d", shardId, i+1),
			NodeId: fmt.Sprintf("node_%d", i+1),
		}
	}
	return replicas
}

func parentReplicaIds() map[string]bool {
	ids := make(map[string]bool)
	for _, r := range replicasFor(parentShard) {
		ids[r.Id] = true
	}
	return ids
}

func childReplicaIds() map[string]bool {
	ids := make(map[string]bool)
	for _, shardId := range []string{child1Shard, child2Shard} {
		for _, r := range replicasFor(shardId) {
			ids[r.Id] = true
		}
	}
	return ids
}

func childReplicaIdsByNode() map[string][]string {
	byNode := make(map[string][]string)
	for _, shardId := range []string{child1Shard, child2Shard} {
		for _, r := range replicasFor(shardId) {
			byNode[r.NodeId] = append(byNode[r.NodeId], r.Id)
		}
	}
	return byNode
}

// requireSeedingVisible waits until every node reports its two dormant
// children with Seeding set.
func requireSeedingVisible(t *testing.T, admin *grpc.AdminClient, addrs []string) {
	t.Helper()
	byNode := childReplicaIdsByNode()
	require.Eventually(t, func() bool {
		for i, addr := range addrs {
			nodeId := fmt.Sprintf("node_%d", i+1)
			states, err := testutils.ListReplicaStates(admin, addr)
			if err != nil {
				return false
			}
			for _, id := range byNode[nodeId] {
				s, ok := states[id]
				if !ok || !s.Seeding || s.RaftState != transport.RaftStateSeeding {
					return false
				}
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond, "children never reported as seeding")
}

// parentCommitIndex returns the parent leader's commit index.
func parentCommitIndex(t *testing.T, admin *grpc.AdminClient, addrs []string) uint64 {
	t.Helper()
	parents := parentReplicaIds()
	var commit uint64
	require.Eventually(t, func() bool {
		for _, addr := range addrs {
			states, err := testutils.ListReplicaStates(admin, addr)
			if err != nil {
				continue
			}
			for id, s := range states {
				if parents[id] && s.RaftState == transport.RaftStateLeader {
					commit = s.Stats.CommitIndex
					return true
				}
			}
		}
		return false
	}, 15*time.Second, 100*time.Millisecond, "no parent leader found")
	return commit
}

// requireSeededTo waits until every child replica on every node has seeded at
// least up to target.
func requireSeededTo(t *testing.T, admin *grpc.AdminClient, addrs []string, target uint64) {
	t.Helper()
	byNode := childReplicaIdsByNode()
	require.Eventually(t, func() bool {
		for i, addr := range addrs {
			nodeId := fmt.Sprintf("node_%d", i+1)
			states, err := testutils.ListReplicaStates(admin, addr)
			if err != nil {
				return false
			}
			for _, id := range byNode[nodeId] {
				s, ok := states[id]
				if !ok || !s.Seeding || s.SeededIndex < target {
					return false
				}
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond, "children never caught up to parent commit index %d", target)
}

// sendCutoff proposes the split CUTOFF through the parent's current leader,
// retrying through elections and leader changes.
func sendCutoff(t *testing.T, admin *grpc.AdminClient, addrs []string, shardId string) {
	t.Helper()
	parents := parentReplicaIds()
	require.Eventually(t, func() bool {
		for _, addr := range addrs {
			states, err := testutils.ListReplicaStates(admin, addr)
			if err != nil {
				continue
			}
			for id, s := range states {
				if parents[id] && s.RaftState == transport.RaftStateLeader {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					err := admin.SplitCutoff(ctx, addr, shardId)
					cancel()
					if err != nil {
						t.Logf("cutoff attempt via %s: %v", addr, err)
						return false
					}
					return true
				}
			}
		}
		return false
	}, 30*time.Second, 200*time.Millisecond, "cutoff was never accepted by a parent leader")
}

// requireParentsFrozen waits until every parent replica on every node reports
// itself frozen by the cutoff.
func requireParentsFrozen(t *testing.T, admin *grpc.AdminClient, addrs []string) {
	t.Helper()
	parents := parentReplicaIds()
	require.Eventually(t, func() bool {
		frozen := 0
		for _, addr := range addrs {
			states, err := testutils.ListReplicaStates(admin, addr)
			if err != nil {
				return false
			}
			for id, s := range states {
				if parents[id] {
					if !s.Frozen {
						return false
					}
					frozen++
				}
			}
		}
		return frozen == len(parents)
	}, 30*time.Second, 200*time.Millisecond, "parent replicas never froze")
}

// verifyAllKeys asserts that every acknowledged write is served with its
// value, routed by key over the given (post-split) config.
func verifyAllKeys(t *testing.T, cfg *cluster.Config, written *writtenSet) {
	t.Helper()
	stub := testutils.NewPlaygroundStub(cfg)
	keys, values := written.snapshot()
	require.NotEmpty(t, keys)

	// The first read waits out the children's leader elections.
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		v, err := stub.Read(ctx, keys[0])
		return err == nil && v == values[0]
	}, 30*time.Second, 200*time.Millisecond, "children never started serving")

	for i, k := range keys {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		v, err := stub.Read(ctx, k)
		cancel()
		require.NoErrorf(t, err, "reading key %d after the split", k)
		require.Equalf(t, values[i], v, "key %d has wrong value after the split", k)
	}
	t.Logf("verified %d keys after the split", len(keys))
}

// writtenSet tracks every acknowledged write for post-split verification.
type writtenSet struct {
	mu     sync.Mutex
	keys   []uint64
	values []string
}

func newWrittenSet() *writtenSet { return &writtenSet{} }

func (w *writtenSet) add(key uint64, value string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.keys = append(w.keys, key)
	w.values = append(w.values, value)
}

func (w *writtenSet) snapshot() ([]uint64, []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]uint64(nil), w.keys...), append([]string(nil), w.values...)
}

func writeKey(t *testing.T, stub *testcore.PlaygroundApiMonsteraStub, written *writtenSet, key uint64) {
	t.Helper()
	v := fmt.Sprintf("value-%d", key)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := stub.Update(ctx, key, v)
	require.NoErrorf(t, err, "writing key %d", key)
	written.add(key, v)
}

// startWriter launches a background writer issuing sequential updates from
// startKey on. If failures is non-nil, every failed write is counted there
// and described in lastFailure (strict mode — the zero-downtime assertion);
// otherwise failures are tolerated and only acknowledged writes are recorded.
// The returned stop func terminates the writer and waits for it to exit.
func startWriter(stub *testcore.PlaygroundApiMonsteraStub, written *writtenSet, startKey uint64,
	failures *atomic.Int64, lastFailure *atomic.Value) (stop func()) {
	stopCh := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := startKey; ; i++ {
			select {
			case <-stopCh:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			v := fmt.Sprintf("value-%d", i)
			_, err := stub.Update(ctx, i, v)
			cancel()
			if err != nil {
				if failures != nil {
					failures.Add(1)
					lastFailure.Store(fmt.Sprintf("key %d: %v", i, err))
				}
				continue
			}
			written.add(i, v)
			time.Sleep(2 * time.Millisecond)
		}
	}()
	return func() {
		close(stopCh)
		<-done
	}
}
