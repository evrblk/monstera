package control

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"google.golang.org/grpc/status"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/transport"
)

// Options tunes executor timing and logging.
type Options struct {
	// PollInterval is the wait between convergence/gate polls.
	PollInterval time.Duration
	// RPCTimeout bounds each admin-plane RPC.
	RPCTimeout time.Duration
	// Logf receives progress messages. Defaults to log.Printf; set to a no-op to silence.
	Logf func(format string, args ...any)
}

// DefaultOptions returns sensible executor options.
func DefaultOptions() Options {
	return Options{
		PollInterval: 250 * time.Millisecond,
		RPCTimeout:   5 * time.Second,
		Logf:         log.Printf,
	}
}

// Executor replays a Sequence over the transport admin plane, addressing nodes by
// their advertised grpc address (from the config being applied). The base config
// is the pinned, heavy input; every step's target config is re-derived by folding.
type Executor struct {
	admin transport.AdminPlane
	base  *cluster.Config
	path  string // sequence checkpoint path
	opts  Options
}

// NewExecutor builds an executor. base must be the exact config the sequence was
// planned against (verified in Run); path is where the sequence is checkpointed
// after every completed step.
func NewExecutor(admin transport.AdminPlane, base *cluster.Config, path string, opts Options) *Executor {
	if opts.PollInterval <= 0 {
		opts.PollInterval = 250 * time.Millisecond
	}
	if opts.RPCTimeout <= 0 {
		opts.RPCTimeout = 5 * time.Second
	}
	if opts.Logf == nil {
		opts.Logf = func(string, ...any) {}
	}
	return &Executor{admin: admin, base: base, path: path, opts: opts}
}

// Run executes (or resumes) the sequence to completion, checkpointing after each
// step. It is safe to call again on an already-completed sequence (clean no-op) or
// to resume a partially-run one (re-derives the running config from the cursor and
// continues; per-step work is idempotent).
func (e *Executor) Run(ctx context.Context, seq *Sequence) error {
	if err := verifyBase(e.base, seq); err != nil {
		return err
	}
	if seq.Status == StatusCompleted {
		e.opts.Logf("sequence %q already completed", seq.Name)
		return nil
	}

	seq.Status = StatusRunning

	// Rebuild the running config for the resume point (== base when Cursor == 0).
	current, err := BuildStepConfig(e.base, seq, seq.Cursor-1)
	if err != nil {
		return e.fail(seq, err)
	}

	for seq.Cursor < len(seq.Steps) {
		step := seq.Steps[seq.Cursor]
		target, err := foldStep(current, step)
		if err != nil {
			return e.fail(seq, fmt.Errorf("step %d fold: %w", step.Index, err))
		}

		e.opts.Logf("step %d/%d: %s (-> v%d)", seq.Cursor+1, len(seq.Steps), step.Description, target.Version)
		if err := e.executeStep(ctx, current, target, step); err != nil {
			return e.fail(seq, fmt.Errorf("step %d (%s): %w", step.Index, step.Description, err))
		}

		step.Status = StatusDone
		current = target
		seq.Cursor++
		if err := SaveSequence(e.path, seq); err != nil {
			return err
		}
	}

	seq.Status = StatusCompleted
	if err := SaveSequence(e.path, seq); err != nil {
		return err
	}
	e.opts.Logf("sequence %q completed at version %d", seq.Name, current.Version)
	return nil
}

func (e *Executor) fail(seq *Sequence, err error) error {
	seq.Status = StatusFailed
	_ = SaveSequence(e.path, seq)
	return err
}

func (e *Executor) executeStep(ctx context.Context, current, target *cluster.Config, step *Step) error {
	switch step.Kind {
	case StepApplyConfig:
		// handled below
	case StepBake:
		return e.executeBake(ctx, target, step)
	case StepSendCommand:
		return fmt.Errorf("send_command steps are not yet supported")
	default:
		return fmt.Errorf("unknown step kind %q", step.Kind)
	}

	// 1. Drift / precondition check.
	if err := e.checkPreconditions(ctx, current, target); err != nil {
		return err
	}

	// 2. Pre-actions (e.g. leadership transfer away from a replica about to be removed).
	for _, a := range step.PreActions {
		if err := e.runAction(ctx, a, current); err != nil {
			return err
		}
	}

	// 3. Push the target config to every node in it.
	for _, n := range target.ListNodes() {
		if err := e.pushConfig(ctx, n, target); err != nil {
			return fmt.Errorf("pushing config to node %s (%s): %w", n.Id, n.GrpcAddress, err)
		}
	}

	// 4. Converge — the no-3-versions gate: every node must reach target.Version
	//    before the next step introduces version+1.
	if err := e.awaitConverged(ctx, target); err != nil {
		return err
	}

	// 5. Step gates.
	for _, g := range step.Gates {
		if err := e.awaitGate(ctx, g, target); err != nil {
			return err
		}
	}
	return nil
}

// checkPreconditions verifies the cluster is where we expect before pushing:
// every existing node is at current.Version (or already at target.Version, which
// is a safe resume of a partially-applied step), and the transition is safe.
func (e *Executor) checkPreconditions(ctx context.Context, current, target *cluster.Config) error {
	for _, n := range current.ListNodes() {
		cfg, err := e.getConfig(ctx, n.GrpcAddress)
		if err != nil {
			return fmt.Errorf("drift check: node %s (%s): %w", n.Id, n.GrpcAddress, err)
		}
		if cfg == nil {
			return fmt.Errorf("drift check: node %s (%s) reports no config", n.Id, n.GrpcAddress)
		}
		if cfg.Version != current.Version && cfg.Version != target.Version {
			return fmt.Errorf("drift: node %s at version %d, expected %d (or already-applied %d)", n.Id, cfg.Version, current.Version, target.Version)
		}
	}
	if err := cluster.ValidateTransition(current, target); err != nil {
		return fmt.Errorf("unsafe transition v%d -> v%d: %w", current.Version, target.Version, err)
	}
	return nil
}

// pushConfig installs target on a node, skipping nodes already at target.Version
// (idempotent re-push / resume) and bootstrapping unprovisioned nodes (e.g.
// add-node's fresh node) instead of pushing an update.
func (e *Executor) pushConfig(ctx context.Context, node *cluster.Node, target *cluster.Config) error {
	if cfg, err := e.getConfig(ctx, node.GrpcAddress); err == nil && cfg != nil && cfg.Version == target.Version {
		return nil
	}

	cctx, cancel := context.WithTimeout(ctx, e.opts.RPCTimeout)
	defer cancel()

	err := e.admin.UpdateClusterConfig(cctx, node.GrpcAddress, target)
	if err != nil && isNotProvisioned(err) {
		e.opts.Logf("node %s (%s) is not provisioned; bootstrapping it", node.Id, node.GrpcAddress)
		return e.admin.Bootstrap(cctx, node.GrpcAddress, node.Id, target)
	}
	return err
}

// executeBake soaks for the step's WaitFor duration (no config change), then
// re-confirms the step's gates still hold — e.g. the new replica is still caught up
// and the shard still has a leader before the sequence advances to the removal.
func (e *Executor) executeBake(ctx context.Context, cfg *cluster.Config, step *Step) error {
	if step.WaitFor != "" {
		d, err := time.ParseDuration(step.WaitFor)
		if err != nil {
			return fmt.Errorf("invalid bake duration %q: %w", step.WaitFor, err)
		}
		if d > 0 {
			e.opts.Logf("baking for %s before continuing", d)
			select {
			case <-ctx.Done():
				return fmt.Errorf("bake interrupted: %w", ctx.Err())
			case <-time.After(d):
			}
		}
	}
	for _, g := range step.Gates {
		if err := e.awaitGate(ctx, g, cfg); err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) awaitConverged(ctx context.Context, target *cluster.Config) error {
	return e.poll(ctx, fmt.Sprintf("all nodes to converge to version %d", target.Version), func(ctx context.Context) (bool, error) {
		for _, n := range target.ListNodes() {
			cfg, err := e.getConfig(ctx, n.GrpcAddress)
			if err != nil || cfg == nil || cfg.Version != target.Version {
				return false, nil
			}
		}
		return true, nil
	})
}

func (e *Executor) awaitGate(ctx context.Context, g Gate, cfg *cluster.Config) error {
	switch g.Kind {
	case GateConfigConverged:
		return e.awaitConverged(ctx, cfg)
	case GateLeaderElected:
		return e.poll(ctx, fmt.Sprintf("shard %s to elect a leader", g.ShardId), func(ctx context.Context) (bool, error) {
			return e.hasLeader(ctx, cfg, g.ShardId)
		})
	case GateReplicaCaughtUp:
		return e.poll(ctx, fmt.Sprintf("replica %s to catch up", g.ReplicaId), func(ctx context.Context) (bool, error) {
			return e.caughtUp(ctx, cfg, g.ShardId, g.ReplicaId, g.MaxLagEntries)
		})
	default:
		return fmt.Errorf("unknown gate kind %q", g.Kind)
	}
}

func (e *Executor) runAction(ctx context.Context, a Action, cfg *cluster.Config) error {
	switch a.Kind {
	case ActionLeadershipTransfer:
		states, err := e.shardReplicaStates(ctx, cfg, a.ShardId)
		if err != nil {
			return err
		}
		s, ok := states[a.ReplicaId]
		if !ok || s.RaftState != transport.RaftStateLeader {
			return nil // not the leader; nothing to transfer
		}
		addr, err := e.replicaAddress(cfg, a.ShardId, a.ReplicaId)
		if err != nil {
			return err
		}
		cctx, cancel := context.WithTimeout(ctx, e.opts.RPCTimeout)
		err = e.admin.LeadershipTransfer(cctx, addr, a.ReplicaId)
		cancel()
		if err != nil {
			return err
		}
		// Wait for a new leader that is not the replica we transferred away from.
		return e.poll(ctx, fmt.Sprintf("leadership to move away from replica %s", a.ReplicaId), func(ctx context.Context) (bool, error) {
			st, err := e.shardReplicaStates(ctx, cfg, a.ShardId)
			if err != nil {
				return false, err
			}
			for id, s := range st {
				if s.RaftState == transport.RaftStateLeader && id != a.ReplicaId {
					return true, nil
				}
			}
			return false, nil
		})
	default:
		return fmt.Errorf("unknown action kind %q", a.Kind)
	}
}

// poll runs fn every PollInterval until it returns true, fn errors, or ctx ends.
func (e *Executor) poll(ctx context.Context, what string, fn func(context.Context) (bool, error)) error {
	for {
		ok, err := fn(ctx)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for %s: %w", what, ctx.Err())
		case <-time.After(e.opts.PollInterval):
		}
	}
}

func (e *Executor) getConfig(ctx context.Context, address string) (*cluster.Config, error) {
	cctx, cancel := context.WithTimeout(ctx, e.opts.RPCTimeout)
	defer cancel()
	return e.admin.GetClusterConfig(cctx, address)
}

// isNotProvisioned reports whether err indicates the node is not yet provisioned
// (UNPROVISIONED), so the executor should Bootstrap it instead of pushing.
func isNotProvisioned(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if st, ok := status.FromError(err); ok {
		msg = st.Message()
	}
	return strings.Contains(msg, "not in READY state") || strings.Contains(msg, "not provisioned")
}
