package testutils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera/cluster"
)

// SingleShardConfig builds the standard test topology: nodes "node_1" ..
// "node_N" at the given addresses, one application "Core" with replication
// factor `replicas`, and one full-range ACTIVE shard with a replica on each
// of the first `replicas` nodes (any remaining nodes host nothing — the
// add-node/move-shard target).
func SingleShardConfig(t *testing.T, addrs []string, replicas int) *cluster.Config {
	t.Helper()
	require.LessOrEqual(t, replicas, len(addrs))

	c := cluster.CreateEmptyConfig()
	ids := make([]string, len(addrs))
	for i, addr := range addrs {
		ids[i] = fmt.Sprintf("node_%d", i+1)
		_, err := c.CreateNode(ids[i], addr)
		require.NoError(t, err)
	}
	a, err := c.CreateApplication("Core", "Core", int32(replicas))
	require.NoError(t, err)
	s, err := c.CreateShard(a.Name, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, "")
	require.NoError(t, err)
	for _, id := range ids[:replicas] {
		_, err := c.CreateReplica(a.Name, s.Id, id)
		require.NoError(t, err)
	}
	require.NoError(t, c.Validate())
	return c
}

// SingleShardLocalConfig is SingleShardConfig for the local (in-memory)
// transport, where a node's address is its id.
func SingleShardLocalConfig(t *testing.T, nodes int, replicas int) *cluster.Config {
	t.Helper()
	addrs := make([]string, nodes)
	for i := range addrs {
		addrs[i] = fmt.Sprintf("node_%d", i+1)
	}
	return SingleShardConfig(t, addrs, replicas)
}
