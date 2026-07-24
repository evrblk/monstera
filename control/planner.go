package control

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/evrblk/monstera/cluster"
)

// moveShardMaxLag is the catch-up tolerance (in Raft log entries) the move-shard
// gates allow between the new replica's applied index and the leader's commit
// index. 0 means "fully caught up at the moment of the check".
const moveShardMaxLag = 0

// PlanAddNode builds a one-step sequence that adds a node to the cluster. Adding a
// node touches no replicas, so the single apply_config step's only gate is
// config_converged. The node must not already exist in the base config.
//
// CreatedAt is left empty here so the planner is deterministic (same inputs →
// byte-identical JSON); the caller stamps it at persist time.
func PlanAddNode(base *cluster.Config, nodeId string, grpcAddress string) (*Sequence, error) {
	if nodeId == "" || grpcAddress == "" {
		return nil, fmt.Errorf("add-node requires a node id and grpc address")
	}
	if _, err := base.GetNode(nodeId); err == nil {
		return nil, fmt.Errorf("node %q already exists in the config", nodeId)
	}

	hash, err := base.Hash()
	if err != nil {
		return nil, err
	}

	seq := &Sequence{
		Name:        fmt.Sprintf("add-node-%s", nodeId),
		Kind:        KindAddNode,
		BaseVersion: base.Version,
		BaseHash:    hash,
		Cursor:      0,
		Status:      StatusPending,
		Steps: []*Step{
			{
				Index:       0,
				Description: fmt.Sprintf("add node %s (%s)", nodeId, grpcAddress),
				Kind:        StepApplyConfig,
				Version:     base.Version + 1,
				Mutations: []Mutation{
					{
						Kind:        MutationAddNode,
						NodeId:      nodeId,
						GrpcAddress: grpcAddress,
					},
				},
				Gates:  []Gate{{Kind: GateConfigConverged}},
				Status: StatusPending,
			},
		},
	}

	// Validate the whole plan at build time: each step yields a valid config and is
	// a safe transition from the previous one.
	if err := validateSequence(base, seq); err != nil {
		return nil, err
	}

	return seq, nil
}

// PlanMoveShard builds a sequence that moves one of a shard's replicas from
// fromNodeId to toNodeId. Because ValidateTransition forbids add+remove in one
// step, it is three steps: add the new replica on the target, bake (soak) while it
// stabilizes, then remove the old replica from the source. RF is preserved (one
// added, one removed), with a transient extra voter during the bake — safe for
// RF>=3 since the existing majority keeps committing.
//
//   - Step 0 (apply_config): add_replica on toNodeId. Gates: leader_elected +
//     replica_caught_up(newReplica).
//   - Step 1 (bake): wait bakeFor, then re-confirm the new replica is caught up and
//     the shard has a leader. No config change.
//   - Step 2 (apply_config): remove_replica on fromNodeId. Pre-action:
//     leadership_transfer away from the old replica if it leads. Gates:
//     config_converged + leader_elected.
//
// The new replica id is derived deterministically from the base config hash + shard
// + target, so the plan is reproducible. CreatedAt is left empty (stamped by the
// caller at persist time).
func PlanMoveShard(base *cluster.Config, shardId, fromNodeId, toNodeId string, bakeFor time.Duration) (*Sequence, error) {
	if fromNodeId == toNodeId {
		return nil, fmt.Errorf("source and target nodes are the same (%q)", fromNodeId)
	}
	shard, err := base.GetShard(shardId)
	if err != nil {
		return nil, err
	}
	if _, err := base.GetNode(toNodeId); err != nil {
		return nil, fmt.Errorf("target node %q not in config: %w", toNodeId, err)
	}
	appName, err := applicationForShard(base, shardId)
	if err != nil {
		return nil, err
	}

	var oldReplicaId string
	for _, r := range shard.Replicas {
		if r.NodeId == toNodeId {
			return nil, fmt.Errorf("shard %q already has a replica on target node %q", shardId, toNodeId)
		}
		if r.NodeId == fromNodeId {
			oldReplicaId = r.Id
		}
	}
	if oldReplicaId == "" {
		return nil, fmt.Errorf("shard %q has no replica on source node %q", shardId, fromNodeId)
	}

	hash, err := base.Hash()
	if err != nil {
		return nil, err
	}
	newReplicaId := deterministicReplicaId(hash, shardId, toNodeId)

	seq := &Sequence{
		Name:        fmt.Sprintf("move-shard-%s-%s-to-%s", shardId, fromNodeId, toNodeId),
		Kind:        KindMoveShard,
		BaseVersion: base.Version,
		BaseHash:    hash,
		Cursor:      0,
		Status:      StatusPending,
		Steps: []*Step{
			{
				Index:       0,
				Description: fmt.Sprintf("add replica %s of shard %s on node %s", newReplicaId, shardId, toNodeId),
				Kind:        StepApplyConfig,
				Version:     base.Version + 1,
				Mutations: []Mutation{{
					Kind:            MutationAddReplica,
					ApplicationName: appName,
					ShardId:         shardId,
					ReplicaId:       newReplicaId,
					ReplicaNodeId:   toNodeId,
				}},
				Gates: []Gate{
					{Kind: GateLeaderElected, ShardId: shardId},
					{Kind: GateReplicaCaughtUp, ShardId: shardId, ReplicaId: newReplicaId, MaxLagEntries: moveShardMaxLag},
				},
				Status: StatusPending,
			},
			{
				Index:       1,
				Description: fmt.Sprintf("bake for %s while replica %s stabilizes", bakeFor, newReplicaId),
				Kind:        StepBake,
				Version:     base.Version + 1, // unchanged: bake makes no config change
				WaitFor:     bakeFor.String(),
				Gates: []Gate{
					{Kind: GateReplicaCaughtUp, ShardId: shardId, ReplicaId: newReplicaId, MaxLagEntries: moveShardMaxLag},
					{Kind: GateLeaderElected, ShardId: shardId},
				},
				Status: StatusPending,
			},
			{
				Index:       2,
				Description: fmt.Sprintf("remove replica %s of shard %s from node %s", oldReplicaId, shardId, fromNodeId),
				Kind:        StepApplyConfig,
				Version:     base.Version + 2,
				Mutations: []Mutation{{
					Kind:      MutationRemoveReplica,
					ShardId:   shardId,
					ReplicaId: oldReplicaId,
				}},
				PreActions: []Action{{Kind: ActionLeadershipTransfer, ShardId: shardId, ReplicaId: oldReplicaId}},
				Gates: []Gate{
					{Kind: GateConfigConverged},
					{Kind: GateLeaderElected, ShardId: shardId},
				},
				Status: StatusPending,
			},
		},
	}

	if err := validateSequence(base, seq); err != nil {
		return nil, err
	}
	return seq, nil
}

// applicationForShard returns the name of the application that owns shardId.
func applicationForShard(cfg *cluster.Config, shardId string) (string, error) {
	for _, a := range cfg.Applications {
		for _, s := range a.Shards {
			if s.Id == shardId {
				return a.Name, nil
			}
		}
	}
	return "", fmt.Errorf("shard %q not found", shardId)
}

// deterministicReplicaId derives a stable, unique-looking replica id from the base
// config hash, shard, and target node, so PlanMoveShard is reproducible. The
// "<shardId>_<hex>" shape matches cluster's own generated ids.
func deterministicReplicaId(baseHash, shardId, toNodeId string) string {
	sum := sha256.Sum256([]byte(baseHash + "|" + shardId + "|" + toNodeId))
	return fmt.Sprintf("%s_%x", shardId, sum[:4])
}

// splitShardMaxLag is the catch-up tolerance (in parent Raft log entries) the
// children_seeded gate allows before the cutoff is sent. The cutoff drain is
// proportional to this lag, so keep it small.
const splitShardMaxLag = 16

// PlanSplitShard builds a sequence that splits an active shard into two
// children at splitAt (the first shard key of the second child; 4 bytes). See
// notes/shard-split-design.md for the full model. The steps:
//
//   - Step 0 (apply_config): parent -> splitting; create the two ACTIVATING
//     children, co-located with the parent's replicas. Nodes start seeding.
//     Gates: config_converged + children_seeded.
//   - Step 1 (send_command): deliver the CUTOFF through the parent's Raft log;
//     every replica freezes the parent at the same index, finalizes and
//     promotes its local children. Gates: leader_elected on both children.
//   - Step 2 (apply_config): the flip — parent -> inactive, children -> active.
//     Gates: config_converged + leader_elected on both children.
//   - Step 3 (bake): soak, re-confirming the children still lead.
//
// Child shard ids follow cluster.CreateShard's "<app>_<lower>_<upper>" scheme
// and child replica ids are derived deterministically from the base hash, so
// the plan is reproducible. CreatedAt is left empty (stamped at persist time).
func PlanSplitShard(base *cluster.Config, shardId string, splitAt []byte, bakeFor time.Duration) (*Sequence, error) {
	shard, err := base.GetShard(shardId)
	if err != nil {
		return nil, err
	}
	if shard.State != cluster.ShardState_SHARD_STATE_ACTIVE {
		return nil, fmt.Errorf("shard %q is %v; only active shards can split", shardId, shard.State)
	}
	appName, err := applicationForShard(base, shardId)
	if err != nil {
		return nil, err
	}
	if len(splitAt) != 4 {
		return nil, fmt.Errorf("split point must be 4 bytes, got %d", len(splitAt))
	}
	if bytes.Compare(splitAt, shard.LowerBound) <= 0 || bytes.Compare(splitAt, shard.UpperBound) > 0 {
		return nil, fmt.Errorf("split point %x must be within (%x, %x]", splitAt, shard.LowerBound, shard.UpperBound)
	}

	hash, err := base.Hash()
	if err != nil {
		return nil, err
	}

	// Children bounds: [lower, splitAt-1] and [splitAt, upper].
	firstUpper := make([]byte, 4)
	binary.BigEndian.PutUint32(firstUpper, binary.BigEndian.Uint32(splitAt)-1)
	children := []SplitChildSpec{
		childSpec(appName, hash, shard, shard.LowerBound, firstUpper),
		childSpec(appName, hash, shard, splitAt, shard.UpperBound),
	}

	childLeaderGates := []Gate{
		{Kind: GateLeaderElected, ShardId: children[0].ShardId},
		{Kind: GateLeaderElected, ShardId: children[1].ShardId},
	}

	seq := &Sequence{
		Name:        fmt.Sprintf("split-shard-%s-at-%x", shardId, splitAt),
		Kind:        KindSplitShard,
		BaseVersion: base.Version,
		BaseHash:    hash,
		Cursor:      0,
		Status:      StatusPending,
		Steps: []*Step{
			{
				Index:       0,
				Description: fmt.Sprintf("split shard %s at %x into %s and %s", shardId, splitAt, children[0].ShardId, children[1].ShardId),
				Kind:        StepApplyConfig,
				Version:     base.Version + 1,
				Mutations: []Mutation{{
					Kind:            MutationSplitShard,
					ApplicationName: appName,
					ShardId:         shardId,
					SplitChildren:   children,
				}},
				Gates: []Gate{
					{Kind: GateConfigConverged},
					{Kind: GateChildrenSeeded, ShardId: shardId, MaxLagEntries: splitShardMaxLag},
				},
				Status: StatusPending,
			},
			{
				Index:       1,
				Description: fmt.Sprintf("cutoff: freeze shard %s and activate its children", shardId),
				Kind:        StepSendCommand,
				Version:     base.Version + 1, // unchanged: the cutoff is not a config change
				Command:     &ControlCommand{Kind: CommandSplitCutoff, ShardId: shardId},
				Gates:       childLeaderGates,
				Status:      StatusPending,
			},
			{
				Index:       2,
				Description: fmt.Sprintf("flip: retire shard %s, activate %s and %s", shardId, children[0].ShardId, children[1].ShardId),
				Kind:        StepApplyConfig,
				Version:     base.Version + 2,
				Mutations: []Mutation{{
					Kind:    MutationCompleteSplit,
					ShardId: shardId,
				}},
				Gates:  append([]Gate{{Kind: GateConfigConverged}}, childLeaderGates...),
				Status: StatusPending,
			},
			{
				Index:       3,
				Description: fmt.Sprintf("bake for %s while the children stabilize", bakeFor),
				Kind:        StepBake,
				Version:     base.Version + 2, // unchanged
				WaitFor:     bakeFor.String(),
				Gates:       childLeaderGates,
				Status:      StatusPending,
			},
		},
	}

	if err := validateSequence(base, seq); err != nil {
		return nil, err
	}
	return seq, nil
}

// childSpec freezes one split child: id from the bounds (cluster.CreateShard's
// scheme), replicas co-located with the parent's, ids derived from the base
// hash.
func childSpec(appName, baseHash string, parent *cluster.Shard, lower, upper []byte) SplitChildSpec {
	sl, su := cluster.ShortenBounds(lower, upper)
	childId := fmt.Sprintf("%s_%x_%x", appName, sl, su)
	spec := SplitChildSpec{
		ShardId:    childId,
		LowerBound: hex.EncodeToString(lower),
		UpperBound: hex.EncodeToString(upper),
	}
	for _, r := range parent.Replicas {
		spec.Replicas = append(spec.Replicas, SplitChildReplica{
			ReplicaId: deterministicReplicaId(baseHash, childId, r.NodeId),
			NodeId:    r.NodeId,
		})
	}
	return spec
}
