package testutils

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera/transport"
)

// ReplicaStateLister is the one admin-plane call the state helpers need.
// Both grpc.AdminClient (target = address) and local.LocalTransport (target =
// node id) satisfy it, so the same assertions work over either harness.
type ReplicaStateLister interface {
	ListReplicaStates(ctx context.Context, target string) ([]*transport.ReplicaState, error)
}

// ListReplicaStates returns the replica states of one node, keyed by replica
// id.
func ListReplicaStates(admin ReplicaStateLister, target string) (map[string]*transport.ReplicaState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	states, err := admin.ListReplicaStates(ctx, target)
	if err != nil {
		return nil, err
	}
	byId := make(map[string]*transport.ReplicaState, len(states))
	for _, s := range states {
		byId[s.ReplicaId] = s
	}
	return byId, nil
}

// AllReplicaStates concatenates the replica states of every target,
// skipping unreachable ones.
func AllReplicaStates(admin ReplicaStateLister, targets []string) []*transport.ReplicaState {
	var all []*transport.ReplicaState
	for _, target := range targets {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		states, err := admin.ListReplicaStates(ctx, target)
		cancel()
		if err != nil {
			continue
		}
		all = append(all, states...)
	}
	return all
}

// FindLeader returns the first replica reporting itself the Raft leader.
func FindLeader(states []*transport.ReplicaState) (*transport.ReplicaState, bool) {
	for _, s := range states {
		if s.RaftState == transport.RaftStateLeader {
			return s, true
		}
	}
	return nil, false
}

// FindReplicaState returns the state of the replica with the given id.
func FindReplicaState(states []*transport.ReplicaState, replicaId string) (*transport.ReplicaState, bool) {
	for _, s := range states {
		if s.ReplicaId == replicaId {
			return s, true
		}
	}
	return nil, false
}

// RequireLeader waits until some replica on some target reports itself the
// Raft leader. A non-nil replicaIds set restricts the wait to those replicas
// (e.g. one shard's); nil accepts any replica.
func RequireLeader(t *testing.T, admin ReplicaStateLister, targets []string, replicaIds map[string]bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, s := range AllReplicaStates(admin, targets) {
			if s.RaftState == transport.RaftStateLeader && (replicaIds == nil || replicaIds[s.ReplicaId]) {
				return true
			}
		}
		return false
	}, 20*time.Second, 100*time.Millisecond, "no leader elected")
}
