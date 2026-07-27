package monstera

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera/cluster"
)

// routerTestShard builds a shard with three replicas on node_1/2/3.
func routerTestShard(id string, lower, upper []byte, state cluster.ShardState, parentId string) *cluster.Shard {
	return &cluster.Shard{
		Id:         id,
		LowerBound: lower,
		UpperBound: upper,
		State:      state,
		ParentId:   parentId,
		Replicas: []*cluster.Replica{
			{Id: id + "_rpl_1", NodeId: "node_1"},
			{Id: id + "_rpl_2", NodeId: "node_2"},
			{Id: id + "_rpl_3", NodeId: "node_3"},
		},
	}
}

// routerTestConfig wraps shards into a valid 3-node single-application config.
func routerTestConfig(shards ...*cluster.Shard) *cluster.Config {
	return &cluster.Config{
		Applications: []*cluster.Application{
			{
				Name:              "app",
				Implementation:    "impl",
				ReplicationFactor: 3,
				Shards:            shards,
			},
		},
		Nodes: []*cluster.Node{
			{Id: "node_1", GrpcAddress: "localhost:9001"},
			{Id: "node_2", GrpcAddress: "localhost:9002"},
			{Id: "node_3", GrpcAddress: "localhost:9003"},
		},
		Version: 1,
	}
}

// TestRouter_RoutesOverUnsortedConfig is the C1 regression: a config whose shard
// slice is NOT sorted by lower bound (e.g. one applied over RPC, where split
// children are appended at the end rather than being normalized by the config
// package's Load* functions) must still route every key to the correct shard.
// The Router builds its own sorted index, so routing does not depend on the
// config's slice order.
func TestRouter_RoutesOverUnsortedConfig(t *testing.T) {
	cfg := routerTestConfig(
		routerTestShard("shrd_c", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xbf, 0xff, 0xff, 0xff}, cluster.ShardState_SHARD_STATE_ACTIVE, ""),
		routerTestShard("shrd_a", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}, cluster.ShardState_SHARD_STATE_ACTIVE, ""),
		routerTestShard("shrd_d", []byte{0xc0, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, cluster.ShardState_SHARD_STATE_ACTIVE, ""),
		routerTestShard("shrd_b", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, cluster.ShardState_SHARD_STATE_ACTIVE, ""),
	)
	// The config is valid but intentionally left unsorted.
	require.NoError(t, cfg.Validate())

	router := NewRouter(cfg)

	cases := []struct {
		key  []byte
		want string
	}{
		{[]byte{0x00, 0x00, 0x00, 0x00}, "shrd_a"},
		{[]byte{0x3f, 0xff, 0xff, 0xff}, "shrd_a"},
		{[]byte{0x40, 0x00, 0x00, 0x00}, "shrd_b"},
		{[]byte{0x7f, 0xff, 0xff, 0xff}, "shrd_b"},
		{[]byte{0x80, 0x00, 0x00, 0x00}, "shrd_c"},
		{[]byte{0xbf, 0xff, 0xff, 0xff}, "shrd_c"},
		{[]byte{0xc0, 0x00, 0x00, 0x00}, "shrd_d"},
		{[]byte{0xff, 0xff, 0xff, 0xff}, "shrd_d"},
	}
	for _, c := range cases {
		s, err := router.FindShardByShardKey("app", c.key)
		require.NoError(t, err, "key %x", c.key)
		require.Equal(t, c.want, s.Id, "key %x", c.key)
	}
}

// TestRouter_SkipsNonRoutableShards: a splitting parent keeps serving its whole
// range (children excluded), and after the split the inactive parent is skipped
// in favour of its active children.
func TestRouter_SkipsNonRoutableShards(t *testing.T) {
	cfg := routerTestConfig(
		routerTestShard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, cluster.ShardState_SHARD_STATE_SPLITTING, ""),
		routerTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, cluster.ShardState_SHARD_STATE_ACTIVE, ""),
		// activating children appended out of order, sharing bounds with the parent
		routerTestShard("shrd_04", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, cluster.ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
		routerTestShard("shrd_03", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}, cluster.ShardState_SHARD_STATE_ACTIVATING, "shrd_01"),
	)
	require.NoError(t, cfg.Validate())
	router := NewRouter(cfg)

	for _, key := range [][]byte{
		{0x00, 0x00, 0x00, 0x00},
		{0x14, 0x90, 0x2f, 0x1e},
		{0x40, 0x00, 0x00, 0x00},
		{0x7f, 0xff, 0xff, 0xff},
	} {
		s, err := router.FindShardByShardKey("app", key)
		require.NoError(t, err, "key %x", key)
		require.Equal(t, "shrd_01", s.Id, "key %x", key)
	}
}

func TestRouter_FindShardByShardKey_InvalidKeyAndUnknownApp(t *testing.T) {
	router := NewRouter(routerTestConfig(
		routerTestShard("shrd_a", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, cluster.ShardState_SHARD_STATE_ACTIVE, ""),
	))

	_, err := router.FindShardByShardKey("app", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid shard key length")

	_, err = router.FindShardByShardKey("app", []byte{0x00, 0x00, 0x00, 0x00, 0x00})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid shard key length")

	_, err = router.FindShardByShardKey("does-not-exist", []byte{0x00})
	require.ErrorIs(t, err, errRouteApplicationNotFound)

	// A shorter-than-4-byte key is still accepted and resolves.
	s, err := router.FindShardByShardKey("app", []byte{0x00, 0x90})
	require.NoError(t, err)
	require.NotNil(t, s)
}

func BenchmarkRouterFindShard(b *testing.B) {
	const (
		numNodes       = 1000
		numApps        = 50
		shardsPerApp   = 1024
		replication    = 3
		keyspacePerApp = 1 << 32 // 4 bytes
	)

	nodes := make([]*cluster.Node, numNodes)
	for i := range numNodes {
		nodes[i] = &cluster.Node{
			Id:          fmt.Sprintf("node_%04d", i),
			GrpcAddress: fmt.Sprintf("localhost:%d", 9000+i),
		}
	}

	applications := make([]*cluster.Application, numApps)
	for appIdx := range numApps {
		appName := fmt.Sprintf("app_%02d", appIdx)
		shards := make([]*cluster.Shard, shardsPerApp)
		shardSize := keyspacePerApp / shardsPerApp
		for shardIdx := range shardsPerApp {
			lower := uint32(shardIdx * shardSize)
			upper := uint32((shardIdx+1)*shardSize - 1)
			lowerBound := make([]byte, 4)
			upperBound := make([]byte, 4)
			binary.BigEndian.PutUint32(lowerBound, lower)
			binary.BigEndian.PutUint32(upperBound, upper)
			replicas := make([]*cluster.Replica, replication)
			for r := range replication {
				nodeIdx := (shardIdx*replication + r) % numNodes
				replicas[r] = &cluster.Replica{
					Id:     fmt.Sprintf("rpl_%02d_%04d_%d", appIdx, shardIdx, r),
					NodeId: nodes[nodeIdx].Id,
				}
			}
			shards[shardIdx] = &cluster.Shard{
				Id:         fmt.Sprintf("shrd_%02d_%04d", appIdx, shardIdx),
				LowerBound: lowerBound,
				UpperBound: upperBound,
				State:      cluster.ShardState_SHARD_STATE_ACTIVE,
				Replicas:   replicas,
			}
		}
		applications[appIdx] = &cluster.Application{
			Name:              appName,
			Implementation:    "impl",
			ReplicationFactor: replication,
			Shards:            shards,
		}
	}

	clusterConfig, err := cluster.LoadConfig(applications, nodes, nil, 1)
	if err != nil {
		b.Fatalf("failed to create config: %v", err)
	}
	router := NewRouter(clusterConfig)

	rng := rand.New(rand.NewSource(42))
	lookupKeys := make([]struct {
		appIdx int
		key    []byte
	}, b.N)
	for i := 0; i < b.N; i++ {
		appIdx := rng.Intn(numApps)
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, rng.Uint32())
		lookupKeys[i] = struct {
			appIdx int
			key    []byte
		}{appIdx, key}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		app := applications[lookupKeys[i].appIdx]
		_, err := router.FindShardByShardKey(app.Name, lookupKeys[i].key)
		if err != nil {
			b.Fatalf("FindShard failed: %v", err)
		}
	}
}

func TestRouter_IdLookups(t *testing.T) {
	cfg := routerTestConfig(
		routerTestShard("shrd_a", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, cluster.ShardState_SHARD_STATE_ACTIVE, ""),
		routerTestShard("shrd_b", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, cluster.ShardState_SHARD_STATE_ACTIVE, ""),
	)
	require.NoError(t, cfg.Validate())
	router := NewRouter(cfg)

	s, err := router.GetShard("shrd_b")
	require.NoError(t, err)
	require.Equal(t, "shrd_b", s.Id)
	_, err = router.GetShard("nope")
	require.ErrorIs(t, err, errRouteShardNotFound)

	rep, err := router.GetReplica("shrd_a_rpl_2")
	require.NoError(t, err)
	require.Equal(t, "node_2", rep.NodeId)
	_, err = router.GetReplica("nope")
	require.ErrorIs(t, err, errRouteReplicaNotFound)

	n, err := router.GetNode("node_3")
	require.NoError(t, err)
	require.Equal(t, "localhost:9003", n.GrpcAddress)
	_, err = router.GetNode("nope")
	require.ErrorIs(t, err, errRouteNodeNotFound)

	shards, err := router.ListRoutableShards("app")
	require.NoError(t, err)
	require.Len(t, shards, 2)
	require.Equal(t, "shrd_a", shards[0].Id) // sorted by lower bound
	require.Equal(t, "shrd_b", shards[1].Id)
	_, err = router.ListRoutableShards("nope")
	require.ErrorIs(t, err, errRouteApplicationNotFound)
}

// TestRouter_ListRoutableShards: fanout must target only the shards that
// currently serve the keyspace (active + splitting), never inactive (retired)
// or activating (not yet serving) shards.
func TestRouter_ListRoutableShards(t *testing.T) {
	// A completed split: inactive parent overlapped by two active children,
	// plus an unrelated active shard.
	cfg := routerTestConfig(
		routerTestShard("shrd_p", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, cluster.ShardState_SHARD_STATE_INACTIVE, ""),
		routerTestShard("shrd_c1", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}, cluster.ShardState_SHARD_STATE_ACTIVE, "shrd_p"),
		routerTestShard("shrd_c2", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}, cluster.ShardState_SHARD_STATE_ACTIVE, "shrd_p"),
		routerTestShard("shrd_02", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, cluster.ShardState_SHARD_STATE_ACTIVE, ""),
	)
	require.NoError(t, cfg.Validate())
	router := NewRouter(cfg)

	shards, err := router.ListRoutableShards("app")
	require.NoError(t, err)
	ids := make([]string, len(shards))
	for i, s := range shards {
		ids[i] = s.Id
	}
	// The inactive parent shrd_p is excluded; the three active shards remain,
	// sorted by lower bound.
	require.Equal(t, []string{"shrd_c1", "shrd_c2", "shrd_02"}, ids)
}

// TestRouter_FindShard_MultiShard exercises routing over a many-shard, single
// application config with fine-grained boundaries, cross-application isolation,
// and sub-4-byte keys.
func TestRouter_FindShard_MultiShard(t *testing.T) {
	shard := func(id string, lower, upper []byte) *cluster.Shard {
		return routerTestShard(id, lower, upper, cluster.ShardState_SHARD_STATE_ACTIVE, "")
	}
	applications := []*cluster.Application{
		{
			Name:              "test.app_01",
			Implementation:    "test.app",
			ReplicationFactor: 3,
			Shards: []*cluster.Shard{
				shard("shrd_01", []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x3f, 0xff, 0xff, 0xff}),
				shard("shrd_05", []byte{0x40, 0x00, 0x00, 0x00}, []byte{0x4f, 0xff, 0xff, 0xff}),
				shard("shrd_06", []byte{0x50, 0x00, 0x00, 0x00}, []byte{0x5f, 0xff, 0xff, 0xff}),
				shard("shrd_07", []byte{0x60, 0x00, 0x00, 0x00}, []byte{0x6f, 0xff, 0xff, 0xff}),
				shard("shrd_08", []byte{0x70, 0x00, 0x00, 0x00}, []byte{0x74, 0xff, 0xff, 0xff}),
				shard("shrd_09", []byte{0x75, 0x00, 0x00, 0x00}, []byte{0x75, 0x7f, 0xff, 0xff}),
				shard("shrd_10", []byte{0x75, 0x80, 0x00, 0x00}, []byte{0x7f, 0xff, 0xff, 0xff}),
				shard("shrd_03", []byte{0x80, 0x00, 0x00, 0x00}, []byte{0xbf, 0xff, 0xff, 0xff}),
				shard("shrd_04", []byte{0xc0, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}),
			},
		},
	}
	nodes := []*cluster.Node{
		{Id: "node_1", GrpcAddress: "localhost:9001"},
		{Id: "node_2", GrpcAddress: "localhost:9002"},
		{Id: "node_3", GrpcAddress: "localhost:9003"},
	}

	clusterConfig, err := cluster.LoadConfig(applications, nodes, nil, 1)
	require.NoError(t, err)

	router := NewRouter(clusterConfig)

	cases := []struct {
		app  string
		key  []byte
		want string
	}{
		{"test.app_01", []byte{0x14, 0x90, 0x2f, 0x1e}, "shrd_01"},
		{"test.app_01", []byte{0x00, 0x90}, "shrd_01"}, // sub-4-byte key
		{"test.app_01", []byte{0x80, 0x90, 0x2f, 0x1e}, "shrd_03"},
		{"test.app_01", []byte{0xff, 0x90, 0x2f, 0x1e}, "shrd_04"},
		{"test.app_01", []byte{0xff, 0xff, 0xff, 0xff}, "shrd_04"},
		{"test.app_01", []byte{0x45, 0x90, 0xff, 0xff}, "shrd_05"},
		{"test.app_01", []byte{0x75, 0x40, 0xff, 0xff}, "shrd_09"},
		{"test.app_01", []byte{0x75, 0x80, 0xff, 0xff}, "shrd_10"},
	}
	for _, c := range cases {
		p, err := router.FindShardByShardKey(c.app, c.key)
		require.NoError(t, err, "key %x", c.key)
		require.Equal(t, c.want, p.Id, "key %x", c.key)
	}

	// An unknown application is not routable.
	_, err = router.FindShardByShardKey("test.app_02", []byte{0x14, 0x90, 0x2f, 0x1e})
	require.ErrorIs(t, err, errRouteApplicationNotFound)
}

// TestRouter_Nil ensures a Router built from a nil config is usable and reports
// everything as not found rather than panicking.
func TestRouter_Nil(t *testing.T) {
	router := NewRouter(nil)
	_, err := router.FindShardByShardKey("app", []byte{0x00})
	require.ErrorIs(t, err, errRouteApplicationNotFound)
	_, err = router.GetShard("x")
	require.ErrorIs(t, err, errRouteShardNotFound)
	_, err = router.ListRoutableShards("app")
	require.ErrorIs(t, err, errRouteApplicationNotFound)
}
