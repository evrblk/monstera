package control

import (
	"crypto/sha256"
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

// PlanSplitShard is unsupported until the split model (a serving flag on Shard) and
// the Raft cutoff command land. See notes/sequences-design.md, Shard split.
func PlanSplitShard(base *cluster.Config, shardId string) (*Sequence, error) {
	return nil, fmt.Errorf("split-shard is unsupported until the split model and cutoff command are implemented")
}
