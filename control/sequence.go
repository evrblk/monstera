// Package control drives deterministic, resumable cluster reconfiguration
// sequences (add node, move shard, ...) over the transport admin plane. A planner
// turns a base cluster.Config into a Sequence — an ordered list of small,
// individually-safe config mutations with all generated ids frozen in. The
// executor replays the sequence step by step: push each step's config to every
// node, wait for full convergence and any gates, checkpoint, advance. The sequence
// never embeds configs; each step's target config is re-derived by folding
// mutations onto the pinned base config, so execution is deterministic and resume
// is just "reload base + sequence, re-fold up to the cursor, continue".
package control

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Sequence kinds.
const (
	KindAddNode    = "add_node"
	KindMoveShard  = "move_shard"
	KindSplitShard = "split_shard"
)

// Step kinds.
const (
	StepApplyConfig = "apply_config"
	StepBake        = "bake"         // wait/soak between config steps; no config change
	StepSendCommand = "send_command" // split cutoff; placeholder, unsupported
)

// Mutation kinds.
const (
	MutationAddNode       = "add_node"
	MutationAddReplica    = "add_replica"
	MutationRemoveReplica = "remove_replica"
)

// Gate kinds.
const (
	GateConfigConverged = "config_converged"
	GateReplicaCaughtUp = "replica_caught_up"
	GateLeaderElected   = "leader_elected"
)

// Action kinds.
const (
	ActionLeadershipTransfer = "leadership_transfer"
)

// Sequence and Step statuses.
const (
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusDone      = "done"
)

// Sequence is a compact, persisted operation log: the base config's identity
// (version + hash) plus the ordered mutation steps, a cursor (the next step to
// run == resume point), and status. It is the single source of truth for resume.
type Sequence struct {
	Name        string  `json:"name"`
	Kind        string  `json:"kind"`         // add_node | move_shard | split_shard
	BaseVersion int64   `json:"base_version"` // version of the base config this plan derives from
	BaseHash    string  `json:"base_hash"`    // hash of the base config (identity/integrity check)
	CreatedAt   string  `json:"created_at"`   // RFC3339; stamped by the caller at persist time
	Steps       []*Step `json:"steps"`
	Cursor      int     `json:"cursor"` // next step to run == resume point
	Status      string  `json:"status"` // pending | running | completed | failed
}

// Step is one config transition (apply_config) or, for the future split cutoff, a
// control command (send_command). Its target config is BaseVersion+Index+1 and is
// re-derived by folding Mutations onto the running config.
type Step struct {
	Index       int             `json:"index"`
	Description string          `json:"description"`
	Kind        string          `json:"kind"`                  // apply_config | send_command
	Version     int64           `json:"version"`               // resulting config version (== BaseVersion + Index + 1)
	Mutations   []Mutation      `json:"mutations,omitempty"`   // config delta folded onto the running config
	PreActions  []Action        `json:"pre_actions,omitempty"` // e.g. leadership_transfer before a removal
	Gates       []Gate          `json:"gates,omitempty"`       // completion conditions
	WaitFor     string          `json:"wait_for,omitempty"`    // for bake steps: soak duration (Go duration string)
	Command     *ControlCommand `json:"command,omitempty"`     // for send_command (split cutoff; placeholder)
	Status      string          `json:"status"`                // pending | running | done
	StartedAt   string          `json:"started_at,omitempty"`
	CompletedAt string          `json:"completed_at,omitempty"`
}

// Mutation is a deterministic, self-contained edit to a cluster.Config. All
// generated ids (e.g. a new replica id) are frozen here at plan time so folding
// reproduces byte-identical configs on every run.
type Mutation struct {
	Kind            string `json:"kind"`                       // add_node | add_replica | remove_replica
	NodeId          string `json:"node_id,omitempty"`          // add_node
	GrpcAddress     string `json:"grpc_address,omitempty"`     // add_node
	ApplicationName string `json:"application_name,omitempty"` // replica ops
	ShardId         string `json:"shard_id,omitempty"`         // replica ops
	ReplicaId       string `json:"replica_id,omitempty"`       // replica ops (frozen at plan time)
	ReplicaNodeId   string `json:"replica_node_id,omitempty"`  // add_replica target node
}

// Gate is a completion condition for a step, polled over the transport.
type Gate struct {
	Kind          string `json:"kind"` // config_converged | replica_caught_up | leader_elected
	ShardId       string `json:"shard_id,omitempty"`
	ReplicaId     string `json:"replica_id,omitempty"`
	MaxLagEntries uint64 `json:"max_lag_entries,omitempty"`
}

// Action is a side effect run before a step's push (a pre-action).
type Action struct {
	Kind      string `json:"kind"` // leadership_transfer
	ShardId   string `json:"shard_id,omitempty"`
	ReplicaId string `json:"replica_id,omitempty"` // replica to move leadership away from
}

// ControlCommand is an opaque command delivered over Raft (the split cutoff).
// Placeholder; send_command steps are not yet executable.
type ControlCommand struct {
	Kind    string `json:"kind"`
	ShardId string `json:"shard_id,omitempty"`
	Payload []byte `json:"payload,omitempty"`
}

// LoadSequence reads a sequence from a JSON file.
func LoadSequence(path string) (*Sequence, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var seq Sequence
	if err := json.Unmarshal(data, &seq); err != nil {
		return nil, fmt.Errorf("parsing sequence %s: %w", path, err)
	}
	return &seq, nil
}

// SaveSequence writes a sequence to a JSON file atomically (temp file in the same
// directory, then rename), so a crash mid-write never corrupts the checkpoint.
func SaveSequence(path string, seq *Sequence) error {
	data, err := json.MarshalIndent(seq, "", "  ")
	if err != nil {
		return err
	}

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
