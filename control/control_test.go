package control

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/transport"
)

// baseConfig builds a valid 3-node config with one application, one shard, and a
// replica of that shard on each node. Node addresses are the node ids.
func baseConfig(t *testing.T) *cluster.Config {
	t.Helper()
	c := cluster.CreateEmptyConfig()
	for _, id := range []string{"node_1", "node_2", "node_3"} {
		_, err := c.CreateNode(id, id)
		require.NoError(t, err)
	}
	a, err := c.CreateApplication("Core", "Core", 3)
	require.NoError(t, err)
	s, err := c.CreateShard(a.Name, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, "")
	require.NoError(t, err)
	for _, id := range []string{"node_1", "node_2", "node_3"} {
		_, err := c.CreateReplica(a.Name, s.Id, id)
		require.NoError(t, err)
	}
	require.NoError(t, c.Validate())
	return c
}

func testOpts() Options {
	return Options{PollInterval: time.Millisecond, RPCTimeout: time.Second, Logf: func(string, ...any) {}}
}

// --- fake AdminPlane -------------------------------------------------------

type fakeNode struct {
	provisioned bool
	cfg         *cluster.Config
	states      []*transport.ReplicaState
}

type fakeAdmin struct {
	mu    sync.Mutex
	nodes map[string]*fakeNode // keyed by address
}

var _ transport.AdminPlane = (*fakeAdmin)(nil)

func newFakeAdmin() *fakeAdmin { return &fakeAdmin{nodes: map[string]*fakeNode{}} }

func (f *fakeAdmin) addProvisioned(addr string, cfg *cluster.Config) {
	f.nodes[addr] = &fakeNode{provisioned: true, cfg: cfg}
}
func (f *fakeAdmin) addUnprovisioned(addr string) { f.nodes[addr] = &fakeNode{} }

func (f *fakeAdmin) GetClusterConfig(ctx context.Context, address string) (*cluster.Config, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, ok := f.nodes[address]
	if !ok {
		return nil, fmt.Errorf("unreachable: %s", address)
	}
	if !n.provisioned {
		return nil, nil // mirrors a node that has no applied config yet
	}
	return n.cfg, nil
}

func (f *fakeAdmin) UpdateClusterConfig(ctx context.Context, address string, config *cluster.Config) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, ok := f.nodes[address]
	if !ok {
		return fmt.Errorf("unreachable: %s", address)
	}
	if !n.provisioned {
		return fmt.Errorf("node is not in READY state")
	}
	if config.Version <= n.cfg.Version {
		return fmt.Errorf("config version must increase (have %d, got %d)", n.cfg.Version, config.Version)
	}
	n.cfg = config
	return nil
}

func (f *fakeAdmin) Bootstrap(ctx context.Context, address, nodeId string, config *cluster.Config) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, ok := f.nodes[address]
	if !ok {
		return fmt.Errorf("unreachable: %s", address)
	}
	if n.provisioned {
		return nil // idempotent: already provisioned
	}
	n.provisioned = true
	n.cfg = config
	return nil
}

func (f *fakeAdmin) ListReplicaStates(ctx context.Context, address string) ([]*transport.ReplicaState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, ok := f.nodes[address]
	if !ok {
		return nil, fmt.Errorf("unreachable: %s", address)
	}
	return n.states, nil
}

func (f *fakeAdmin) ListReplicaSnapshots(ctx context.Context, address, replicaId string) ([]*transport.RaftSnapshot, error) {
	return nil, nil
}
func (f *fakeAdmin) TriggerSnapshot(ctx context.Context, address, replicaId string) error { return nil }
func (f *fakeAdmin) LeadershipTransfer(ctx context.Context, address, replicaId string) error {
	return nil
}
func (f *fakeAdmin) Close() error { return nil }

func (f *fakeAdmin) version(addr string) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.nodes[addr].cfg.Version
}

// --- planner / fold / sequence tests --------------------------------------

func TestPlanAddNode(t *testing.T) {
	base := baseConfig(t)
	seq, err := PlanAddNode(base, "node_4", "node_4:9004")
	require.NoError(t, err)

	require.Equal(t, KindAddNode, seq.Kind)
	require.Equal(t, base.Version, seq.BaseVersion)
	require.Len(t, seq.Steps, 1)
	step := seq.Steps[0]
	require.Equal(t, StepApplyConfig, step.Kind)
	require.Equal(t, base.Version+1, step.Version)
	require.Equal(t, []Mutation{{Kind: MutationAddNode, NodeId: "node_4", GrpcAddress: "node_4:9004"}}, step.Mutations)
	require.Equal(t, []Gate{{Kind: GateConfigConverged}}, step.Gates)

	// Determinism: same inputs -> byte-identical JSON (CreatedAt is left empty).
	seq2, err := PlanAddNode(base, "node_4", "node_4:9004")
	require.NoError(t, err)
	b1, _ := json.Marshal(seq)
	b2, _ := json.Marshal(seq2)
	require.Equal(t, string(b1), string(b2))
}

func TestPlanAddNodeRejectsExisting(t *testing.T) {
	base := baseConfig(t)
	_, err := PlanAddNode(base, "node_1", "node_1")
	require.Error(t, err)
}

// moveBaseConfig is a 4-node config with one shard replicated on node_1/2/3;
// node_4 hosts none of it (a valid move-shard target).
func moveBaseConfig(t *testing.T) *cluster.Config {
	t.Helper()
	c := cluster.CreateEmptyConfig()
	for _, id := range []string{"node_1", "node_2", "node_3", "node_4"} {
		_, err := c.CreateNode(id, id)
		require.NoError(t, err)
	}
	a, err := c.CreateApplication("Core", "Core", 3)
	require.NoError(t, err)
	s, err := c.CreateShard(a.Name, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, "")
	require.NoError(t, err)
	for _, id := range []string{"node_1", "node_2", "node_3"} {
		_, err := c.CreateReplica(a.Name, s.Id, id)
		require.NoError(t, err)
	}
	require.NoError(t, c.Validate())
	return c
}

func TestPlanMoveShard(t *testing.T) {
	base := moveBaseConfig(t)
	shardId := base.Applications[0].Shards[0].Id
	oldReplicaId := base.Applications[0].Shards[0].Replicas[0].Id // on node_1

	seq, err := PlanMoveShard(base, shardId, "node_1", "node_4", 30*time.Second)
	require.NoError(t, err)

	require.Equal(t, KindMoveShard, seq.Kind)
	require.Len(t, seq.Steps, 3)

	// Step 0: add the new replica on node_4.
	add := seq.Steps[0]
	require.Equal(t, StepApplyConfig, add.Kind)
	require.Equal(t, base.Version+1, add.Version)
	require.Len(t, add.Mutations, 1)
	require.Equal(t, MutationAddReplica, add.Mutations[0].Kind)
	require.Equal(t, "node_4", add.Mutations[0].ReplicaNodeId)
	newReplicaId := add.Mutations[0].ReplicaId
	require.NotEmpty(t, newReplicaId)
	require.Contains(t, gateKinds(add.Gates), GateLeaderElected)
	require.Contains(t, gateKinds(add.Gates), GateReplicaCaughtUp)

	// Step 1: the bake step — no config change, waits, re-checks catch-up.
	bake := seq.Steps[1]
	require.Equal(t, StepBake, bake.Kind)
	require.Empty(t, bake.Mutations)
	require.Equal(t, add.Version, bake.Version, "bake must not change the version")
	require.Equal(t, "30s", bake.WaitFor)
	require.Contains(t, gateKinds(bake.Gates), GateReplicaCaughtUp)

	// Step 2: remove the old replica, with a leadership-transfer pre-action.
	rm := seq.Steps[2]
	require.Equal(t, StepApplyConfig, rm.Kind)
	require.Equal(t, base.Version+2, rm.Version)
	require.Equal(t, []Mutation{{Kind: MutationRemoveReplica, ShardId: shardId, ReplicaId: oldReplicaId}}, rm.Mutations)
	require.Equal(t, []Action{{Kind: ActionLeadershipTransfer, ShardId: shardId, ReplicaId: oldReplicaId}}, rm.PreActions)

	// Folding the whole sequence moves the replica: node_1 out, node_4 in, count kept.
	final, err := BuildStepConfig(base, seq, 2)
	require.NoError(t, err)
	require.Equal(t, base.Version+2, final.Version)
	fShard, err := final.GetShard(shardId)
	require.NoError(t, err)
	require.Len(t, fShard.Replicas, 3)
	nodes := map[string]bool{}
	for _, r := range fShard.Replicas {
		nodes[r.NodeId] = true
	}
	require.False(t, nodes["node_1"])
	require.True(t, nodes["node_4"])

	// Deterministic: same inputs -> byte-identical JSON.
	seq2, err := PlanMoveShard(base, shardId, "node_1", "node_4", 30*time.Second)
	require.NoError(t, err)
	b1, _ := json.Marshal(seq)
	b2, _ := json.Marshal(seq2)
	require.Equal(t, string(b1), string(b2))
}

func TestPlanMoveShardValidation(t *testing.T) {
	base := moveBaseConfig(t)
	shardId := base.Applications[0].Shards[0].Id

	// Same source and target.
	_, err := PlanMoveShard(base, shardId, "node_1", "node_1", 0)
	require.Error(t, err)
	// Target not in config.
	_, err = PlanMoveShard(base, shardId, "node_1", "node_x", 0)
	require.Error(t, err)
	// Source hosts no replica of this shard (node_4 has none).
	_, err = PlanMoveShard(base, shardId, "node_4", "node_2", 0)
	require.Error(t, err)
	// Target already hosts a replica of this shard.
	_, err = PlanMoveShard(base, shardId, "node_1", "node_2", 0)
	require.Error(t, err)
	// Unknown shard.
	_, err = PlanMoveShard(base, "no_such_shard", "node_1", "node_4", 0)
	require.Error(t, err)
}

func gateKinds(gates []Gate) []string {
	out := make([]string, len(gates))
	for i, g := range gates {
		out[i] = g.Kind
	}
	return out
}

func TestFoldReproducesConfig(t *testing.T) {
	base := baseConfig(t)
	shardId := base.Applications[0].Shards[0].Id
	seq := &Sequence{
		BaseVersion: base.Version,
		Steps: []*Step{
			{Version: base.Version + 1, Mutations: []Mutation{{Kind: MutationAddNode, NodeId: "node_4", GrpcAddress: "node_4:9004"}}},
			{Version: base.Version + 2, Mutations: []Mutation{{Kind: MutationAddReplica, ApplicationName: "Core", ShardId: shardId, ReplicaId: "replica_new", ReplicaNodeId: "node_4"}}},
			{Version: base.Version + 3, Mutations: []Mutation{{Kind: MutationRemoveReplica, ShardId: shardId, ReplicaId: "replica_new"}}},
		},
	}

	// After step 0: node added.
	c0, err := BuildStepConfig(base, seq, 0)
	require.NoError(t, err)
	require.Equal(t, base.Version+1, c0.Version)
	_, err = c0.GetNode("node_4")
	require.NoError(t, err)

	// After step 1: replica added on node_4.
	c1, err := BuildStepConfig(base, seq, 1)
	require.NoError(t, err)
	r, err := c1.GetReplica("replica_new")
	require.NoError(t, err)
	require.Equal(t, "node_4", r.NodeId)

	// After step 2: replica removed again.
	c2, err := BuildStepConfig(base, seq, 2)
	require.NoError(t, err)
	_, err = c2.GetReplica("replica_new")
	require.Error(t, err)
	require.Equal(t, base.Version+3, c2.Version)

	// upto < 0 == base clone, base untouched.
	cNeg, err := BuildStepConfig(base, seq, -1)
	require.NoError(t, err)
	require.Equal(t, base.Version, cNeg.Version)
	_, err = base.GetNode("node_4")
	require.Error(t, err, "base config must not be mutated by folding")
}

func TestSequenceRoundTrip(t *testing.T) {
	base := baseConfig(t)
	seq, err := PlanAddNode(base, "node_4", "node_4:9004")
	require.NoError(t, err)
	seq.CreatedAt = "2026-01-01T00:00:00Z"

	path := filepath.Join(t.TempDir(), "seq.json")
	require.NoError(t, SaveSequence(path, seq))
	got, err := LoadSequence(path)
	require.NoError(t, err)
	require.Equal(t, seq, got)
}

func TestVerifyBaseMismatch(t *testing.T) {
	base := baseConfig(t)
	seq, err := PlanAddNode(base, "node_4", "node_4:9004")
	require.NoError(t, err)

	// Wrong version.
	bumped := proto.Clone(base).(*cluster.Config)
	bumped.Version = base.Version + 10
	require.Error(t, verifyBase(bumped, seq))

	// Wrong hash (same version, different topology).
	altered := proto.Clone(base).(*cluster.Config)
	_, err = altered.CreateNode("node_x", "node_x")
	require.NoError(t, err)
	altered.Version = base.Version
	require.Error(t, verifyBase(altered, seq))

	// Exact base passes.
	require.NoError(t, verifyBase(base, seq))
}

// --- executor tests --------------------------------------------------------

func TestExecutorAddNode(t *testing.T) {
	base := baseConfig(t)
	seq, err := PlanAddNode(base, "node_4", "node_4:9004")
	require.NoError(t, err)

	fa := newFakeAdmin()
	for _, n := range base.Nodes {
		fa.addProvisioned(n.GrpcAddress, proto.Clone(base).(*cluster.Config))
	}
	fa.addUnprovisioned("node_4:9004")

	exec := NewExecutor(fa, base, filepath.Join(t.TempDir(), "seq.json"), testOpts())
	require.NoError(t, exec.Run(context.Background(), seq))

	require.Equal(t, StatusCompleted, seq.Status)
	for _, addr := range []string{"node_1", "node_2", "node_3", "node_4:9004"} {
		require.Equal(t, base.Version+1, fa.version(addr), "node %s", addr)
	}
	require.True(t, fa.nodes["node_4:9004"].provisioned)
}

func TestExecutorAddNodeAlreadyBootstrapped(t *testing.T) {
	// An operator manually bootstrapped the new node (at the target config) before
	// running add-node. The executor must still complete cleanly.
	base := baseConfig(t)
	seq, err := PlanAddNode(base, "node_4", "node_4:9004")
	require.NoError(t, err)
	target, err := BuildStepConfig(base, seq, 0)
	require.NoError(t, err)

	fa := newFakeAdmin()
	for _, n := range base.Nodes {
		fa.addProvisioned(n.GrpcAddress, proto.Clone(base).(*cluster.Config))
	}
	fa.addProvisioned("node_4:9004", proto.Clone(target).(*cluster.Config))

	exec := NewExecutor(fa, base, filepath.Join(t.TempDir(), "seq.json"), testOpts())
	require.NoError(t, exec.Run(context.Background(), seq))
	require.Equal(t, StatusCompleted, seq.Status)
	for _, addr := range []string{"node_1", "node_2", "node_3", "node_4:9004"} {
		require.Equal(t, base.Version+1, fa.version(addr), "node %s", addr)
	}
}

func TestExecutorDriftRejection(t *testing.T) {
	base := baseConfig(t)
	seq, err := PlanAddNode(base, "node_4", "node_4:9004")
	require.NoError(t, err)

	fa := newFakeAdmin()
	for _, n := range base.Nodes {
		fa.addProvisioned(n.GrpcAddress, proto.Clone(base).(*cluster.Config))
	}
	fa.addUnprovisioned("node_4:9004")
	// node_1 is at an unexpected version -> drift.
	fa.nodes["node_1"].cfg.Version = base.Version + 9

	exec := NewExecutor(fa, base, filepath.Join(t.TempDir(), "seq.json"), testOpts())
	err = exec.Run(context.Background(), seq)
	require.Error(t, err)
	// Refused before pushing: the new node is untouched.
	require.False(t, fa.nodes["node_4:9004"].provisioned)
	require.Equal(t, StatusFailed, seq.Status)
}

func TestExecutorResumeIdempotent(t *testing.T) {
	base := baseConfig(t)
	seq, err := PlanAddNode(base, "node_4", "node_4:9004")
	require.NoError(t, err)

	// Simulate a crash after the step was fully applied but before the checkpoint:
	// every node is already at the target version, cursor still 0.
	target, err := BuildStepConfig(base, seq, 0)
	require.NoError(t, err)
	fa := newFakeAdmin()
	for _, n := range base.Nodes {
		fa.addProvisioned(n.GrpcAddress, proto.Clone(target).(*cluster.Config))
	}
	fa.addProvisioned("node_4:9004", proto.Clone(target).(*cluster.Config))

	// The fake rejects re-pushing the same version, so a clean run proves the
	// executor skips already-applied nodes.
	exec := NewExecutor(fa, base, filepath.Join(t.TempDir(), "seq.json"), testOpts())
	require.NoError(t, exec.Run(context.Background(), seq))
	require.Equal(t, StatusCompleted, seq.Status)
}

func TestExecutorAlreadyCompletedIsNoop(t *testing.T) {
	base := baseConfig(t)
	seq, err := PlanAddNode(base, "node_4", "node_4:9004")
	require.NoError(t, err)
	seq.Status = StatusCompleted
	seq.Cursor = len(seq.Steps)

	fa := newFakeAdmin() // no nodes reachable
	exec := NewExecutor(fa, base, filepath.Join(t.TempDir(), "seq.json"), testOpts())
	require.NoError(t, exec.Run(context.Background(), seq))
}

// --- gate logic tests ------------------------------------------------------

func TestGates(t *testing.T) {
	base := baseConfig(t)
	shard := base.Applications[0].Shards[0]
	shardId := shard.Id
	r1, r2, r3 := shard.Replicas[0], shard.Replicas[1], shard.Replicas[2]

	fa := newFakeAdmin()
	for _, n := range base.Nodes {
		fa.addProvisioned(n.GrpcAddress, proto.Clone(base).(*cluster.Config))
	}
	set := func(nodeId string, s *transport.ReplicaState) { fa.nodes[nodeId].states = []*transport.ReplicaState{s} }
	set(r1.NodeId, &transport.ReplicaState{ReplicaId: r1.Id, RaftState: transport.RaftStateLeader, Stats: transport.RaftStats{CommitIndex: 100, AppliedIndex: 100}})
	set(r2.NodeId, &transport.ReplicaState{ReplicaId: r2.Id, RaftState: transport.RaftStateFollower, Stats: transport.RaftStats{AppliedIndex: 100}})
	set(r3.NodeId, &transport.ReplicaState{ReplicaId: r3.Id, RaftState: transport.RaftStateFollower, Stats: transport.RaftStats{AppliedIndex: 90}})

	exec := NewExecutor(fa, base, "", testOpts())
	ctx := context.Background()

	ok, err := exec.hasLeader(ctx, base, shardId)
	require.NoError(t, err)
	require.True(t, ok)

	// r2 is fully caught up.
	ok, err = exec.caughtUp(ctx, base, shardId, r2.Id, 0)
	require.NoError(t, err)
	require.True(t, ok)

	// r3 lags by 10: fails with maxLag 0, passes with maxLag 10.
	ok, err = exec.caughtUp(ctx, base, shardId, r3.Id, 0)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = exec.caughtUp(ctx, base, shardId, r3.Id, 10)
	require.NoError(t, err)
	require.True(t, ok)

	// No leader -> hasLeader false, caughtUp false.
	fa.nodes[r1.NodeId].states[0].RaftState = transport.RaftStateFollower
	ok, err = exec.hasLeader(ctx, base, shardId)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = exec.caughtUp(ctx, base, shardId, r2.Id, 0)
	require.NoError(t, err)
	require.False(t, ok)
}
