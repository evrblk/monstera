package control

import (
	"context"
	"fmt"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/transport"
)

// shardReplicaStates gathers the observed Raft state of a shard's replicas, keyed
// by replica id, by querying every node that hosts one of them over the admin
// plane. Nodes that fail to answer are skipped (best-effort): a gate simply reads
// as "not satisfied yet" until they respond.
func (e *Executor) shardReplicaStates(ctx context.Context, cfg *cluster.Config, shardId string) (map[string]*transport.ReplicaState, error) {
	shard, err := cfg.GetShard(shardId)
	if err != nil {
		return nil, err
	}

	want := make(map[string]bool, len(shard.Replicas))
	seenAddr := make(map[string]bool)
	var addrs []string
	for _, r := range shard.Replicas {
		want[r.Id] = true
		node, err := cfg.GetNode(r.NodeId)
		if err != nil {
			return nil, err
		}
		if !seenAddr[node.GrpcAddress] {
			seenAddr[node.GrpcAddress] = true
			addrs = append(addrs, node.GrpcAddress)
		}
	}

	out := make(map[string]*transport.ReplicaState)
	for _, addr := range addrs {
		cctx, cancel := context.WithTimeout(ctx, e.opts.RPCTimeout)
		states, err := e.admin.ListReplicaStates(cctx, addr)
		cancel()
		if err != nil {
			continue
		}
		for _, s := range states {
			if want[s.ReplicaId] {
				out[s.ReplicaId] = s
			}
		}
	}
	return out, nil
}

// hasLeader reports whether the shard currently has a replica that is the Raft leader.
func (e *Executor) hasLeader(ctx context.Context, cfg *cluster.Config, shardId string) (bool, error) {
	states, err := e.shardReplicaStates(ctx, cfg, shardId)
	if err != nil {
		return false, err
	}
	for _, s := range states {
		if s.RaftState == transport.RaftStateLeader {
			return true, nil
		}
	}
	return false, nil
}

// caughtUp reports whether the replica is a follower whose applied index is within
// maxLag entries of the shard leader's commit index.
func (e *Executor) caughtUp(ctx context.Context, cfg *cluster.Config, shardId, replicaId string, maxLag uint64) (bool, error) {
	states, err := e.shardReplicaStates(ctx, cfg, shardId)
	if err != nil {
		return false, err
	}

	var leaderCommit uint64
	haveLeader := false
	for _, s := range states {
		if s.RaftState == transport.RaftStateLeader {
			leaderCommit = s.Stats.CommitIndex
			haveLeader = true
		}
	}
	if !haveLeader {
		return false, nil
	}

	r, ok := states[replicaId]
	if !ok || r.RaftState != transport.RaftStateFollower {
		return false, nil
	}
	// applied + maxLag >= leaderCommit, written to avoid unsigned underflow.
	return r.Stats.AppliedIndex+maxLag >= leaderCommit, nil
}

// replicaAddress returns the grpc address of the node hosting a shard's replica.
func (e *Executor) replicaAddress(cfg *cluster.Config, shardId, replicaId string) (string, error) {
	shard, err := cfg.GetShard(shardId)
	if err != nil {
		return "", err
	}
	for _, r := range shard.Replicas {
		if r.Id == replicaId {
			node, err := cfg.GetNode(r.NodeId)
			if err != nil {
				return "", err
			}
			return node.GrpcAddress, nil
		}
	}
	return "", fmt.Errorf("replica %q not found in shard %q", replicaId, shardId)
}
