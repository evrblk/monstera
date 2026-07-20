package control

import (
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
	default:
		return fmt.Errorf("unknown mutation kind %q", m.Kind)
	}
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
		default:
			return fmt.Errorf("step %d has unsupported kind %q", i, step.Kind)
		}
	}
	return nil
}
