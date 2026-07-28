package control

import (
	"encoding/hex"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/evrblk/monstera/cluster"
)

// applyMutation applies a single mutation to cfg in place, using the frozen ids in
// the mutation so folding is deterministic.
func applyMutation(cfg *cluster.Config, m Mutation) error {
	switch m.Kind {
	case MutationAddNode:
		_, err := cfg.CreateNode(m.NodeId, m.GrpcAddress)
		return err
	case MutationAddReplica:
		_, err := cfg.AddReplica(m.ApplicationName, m.ShardId, m.ReplicaId, m.ReplicaNodeId)
		return err
	case MutationRemoveReplica:
		return removeReplica(cfg, m.ShardId, m.ReplicaId)
	case MutationSplitShard:
		return splitShard(cfg, m)
	case MutationCompleteSplit:
		return completeSplit(cfg, m.ShardId)
	default:
		return fmt.Errorf("unknown mutation kind %q", m.Kind)
	}
}

// splitShard marks the parent SPLITTING and creates the ACTIVATING children
// exactly as frozen in the mutation.
func splitShard(cfg *cluster.Config, m Mutation) error {
	for _, a := range cfg.Applications {
		if a.Name != m.ApplicationName {
			continue
		}
		var parent *cluster.Shard
		for _, s := range a.Shards {
			if s.Id == m.ShardId {
				parent = s
				break
			}
		}
		if parent == nil {
			return fmt.Errorf("shard %q not found in application %q", m.ShardId, m.ApplicationName)
		}
		parent.State = cluster.ShardState_SHARD_STATE_SPLITTING

		for _, spec := range m.SplitChildren {
			lower, err := decodeBoundHex(spec.LowerBound)
			if err != nil {
				return fmt.Errorf("child %s lower bound: %w", spec.ShardId, err)
			}
			upper, err := decodeBoundHex(spec.UpperBound)
			if err != nil {
				return fmt.Errorf("child %s upper bound: %w", spec.ShardId, err)
			}
			child := &cluster.Shard{
				Id:         spec.ShardId,
				LowerBound: uint32(lower),
				UpperBound: uint32(upper),
				State:      cluster.ShardState_SHARD_STATE_ACTIVATING,
				ParentId:   parent.Id,
			}
			for _, r := range spec.Replicas {
				child.Replicas = append(child.Replicas, &cluster.Replica{Id: r.ReplicaId, NodeId: r.NodeId})
			}
			a.Shards = append(a.Shards, child)
		}
		return nil
	}
	return fmt.Errorf("application %q not found", m.ApplicationName)
}

// completeSplit flips the split: the parent retires to INACTIVE and its
// activating children become ACTIVE.
func completeSplit(cfg *cluster.Config, parentShardId string) error {
	for _, a := range cfg.Applications {
		var parent *cluster.Shard
		for _, s := range a.Shards {
			if s.Id == parentShardId {
				parent = s
				break
			}
		}
		if parent == nil {
			continue
		}
		parent.State = cluster.ShardState_SHARD_STATE_INACTIVE
		for _, s := range a.Shards {
			if s.ParentId == parentShardId && s.State == cluster.ShardState_SHARD_STATE_ACTIVATING {
				s.State = cluster.ShardState_SHARD_STATE_ACTIVE
			}
		}
		return nil
	}
	return fmt.Errorf("shard %q not found", parentShardId)
}

// removeReplica filters a replica out of its shard.
func removeReplica(cfg *cluster.Config, shardId, replicaId string) error {
	for _, a := range cfg.Applications {
		for _, s := range a.Shards {
			if s.Id != shardId {
				continue
			}
			for i, r := range s.Replicas {
				if r.Id == replicaId {
					s.Replicas = append(s.Replicas[:i], s.Replicas[i+1:]...)
					return nil
				}
			}
			return fmt.Errorf("replica %q not found in shard %q", replicaId, shardId)
		}
	}
	return fmt.Errorf("shard %q not found", shardId)
}

// foldStep applies a step's mutations onto a clone of current and sets the
// resulting version. current is never mutated.
func foldStep(current *cluster.Config, step *Step) (*cluster.Config, error) {
	target := proto.Clone(current).(*cluster.Config)
	for _, m := range step.Mutations {
		if err := applyMutation(target, m); err != nil {
			return nil, err
		}
	}
	target.Version = step.Version
	return target, nil
}

// BuildStepConfig re-derives the config as of step `upto` (inclusive) by folding
// mutations onto a clone of base. upto < 0 returns a clone of base unchanged (the
// resume point when Cursor == 0).
func BuildStepConfig(base *cluster.Config, seq *Sequence, upto int) (*cluster.Config, error) {
	cfg := proto.Clone(base).(*cluster.Config)
	for i := 0; i <= upto && i < len(seq.Steps); i++ {
		for _, m := range seq.Steps[i].Mutations {
			if err := applyMutation(cfg, m); err != nil {
				return nil, fmt.Errorf("step %d: %w", i, err)
			}
		}
		cfg.Version = seq.Steps[i].Version
	}
	return cfg, nil
}

// verifyBase checks that the loaded base config is the exact one the sequence was
// planned against (version + hash), so folding reproduces the planned configs.
func verifyBase(base *cluster.Config, seq *Sequence) error {
	if base.Version != seq.BaseVersion {
		return fmt.Errorf("base config version %d does not match sequence base version %d", base.Version, seq.BaseVersion)
	}
	h, err := base.Hash()
	if err != nil {
		return fmt.Errorf("hashing base config: %w", err)
	}
	if h != seq.BaseHash {
		return fmt.Errorf("base config hash mismatch (config %s, sequence %s): wrong base config for this sequence", h, seq.BaseHash)
	}
	return nil
}

// validateSequence folds the whole sequence onto base and checks every step yields
// a valid config and every consecutive transition passes ValidateTransition (the
// two-version safety contract). Used at plan time.
func validateSequence(base *cluster.Config, seq *Sequence) error {
	prev := proto.Clone(base).(*cluster.Config)
	for i, step := range seq.Steps {
		target, err := foldStep(prev, step)
		if err != nil {
			return fmt.Errorf("step %d fold: %w", i, err)
		}

		switch step.Kind {
		case StepApplyConfig:
			if err := target.Validate(); err != nil {
				return fmt.Errorf("step %d produces an invalid config: %w", i, err)
			}
			if err := cluster.ValidateTransition(prev, target); err != nil {
				return fmt.Errorf("step %d is not a safe transition: %w", i, err)
			}
			prev = target
		case StepBake:
			// A bake step must not change the config; it carries the running config
			// forward unchanged (no transition to validate).
			if len(step.Mutations) != 0 || target.Version != prev.Version {
				return fmt.Errorf("step %d (bake) must not change the config or version", i)
			}
		case StepSendCommand:
			// A command step delivers a shard-level command through Raft; the
			// config is untouched.
			if len(step.Mutations) != 0 || target.Version != prev.Version {
				return fmt.Errorf("step %d (send_command) must not change the config or version", i)
			}
			if step.Command == nil || step.Command.Kind == "" {
				return fmt.Errorf("step %d (send_command) has no command", i)
			}
		default:
			return fmt.Errorf("step %d has unsupported kind %q", i, step.Kind)
		}
	}
	return nil
}

// decodeBoundHex parses a full 8-hex-character shard bound (the sequence-file
// JSON representation) into a shard key.
func decodeBoundHex(s string) (cluster.ShardKey, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return 0, err
	}
	return cluster.ShardKeyFromBytes(b)
}
