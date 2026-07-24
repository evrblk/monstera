package grpc

import (
	"fmt"
	"time"

	"github.com/evrblk/monstera/internal/raft"
	"github.com/evrblk/monstera/transport"
	"github.com/evrblk/monstera/transport/grpc/monsterapb"
)

// decodeReplicaStates converts the wire ListReplicaStates response into the
// transport-level DTOs. It is shared by both the data plane (addressed by nodeId)
// and the admin plane (addressed by raw address), which decode identically.
func decodeReplicaStates(resp *monsterapb.ListReplicaStatesResponse) ([]*transport.ReplicaState, error) {
	states := make([]*transport.ReplicaState, len(resp.ReplicaStates))
	for i, r := range resp.ReplicaStates {
		var protoState monsterapb.RaftState
		if r.RaftStats != nil {
			protoState = r.RaftStats.State
		}

		var raftState transport.RaftState
		switch protoState {
		case monsterapb.RaftState_RAFT_STATE_FOLLOWER:
			raftState = transport.RaftStateFollower
		case monsterapb.RaftState_RAFT_STATE_CANDIDATE:
			raftState = transport.RaftStateCandidate
		case monsterapb.RaftState_RAFT_STATE_LEADER:
			raftState = transport.RaftStateLeader
		case monsterapb.RaftState_RAFT_STATE_SHUTDOWN:
			raftState = transport.RaftStateDead
		case monsterapb.RaftState_RAFT_STATE_SEEDING:
			raftState = transport.RaftStateSeeding
		default:
			return nil, fmt.Errorf("unknown raft state: %v", protoState)
		}

		var stats transport.RaftStats
		if r.RaftStats != nil {
			stats = transport.RaftStats{
				Term:              r.RaftStats.Term,
				LastLogIndex:      r.RaftStats.LastLogIndex,
				LastLogTerm:       r.RaftStats.LastLogTerm,
				CommitIndex:       r.RaftStats.CommitIndex,
				AppliedIndex:      r.RaftStats.AppliedIndex,
				FSMPending:        r.RaftStats.FsmPending,
				LastSnapshotIndex: r.RaftStats.LastSnapshotIndex,
				LastSnapshotTerm:  r.RaftStats.LastSnapshotTerm,
				NumPeers:          int(r.RaftStats.NumPeers),
				LastContact:       time.Duration(r.RaftStats.LastContactNanos),
			}
		}

		states[i] = &transport.ReplicaState{
			ReplicaId:   r.ReplicaId,
			RaftState:   raftState,
			Stats:       stats,
			Seeding:     r.Seeding,
			SeededIndex: r.SeededIndex,
			Frozen:      r.Frozen,
		}
	}
	return states, nil
}

// encodeReplicaStates renders the transport-level replica states onto the wire.
// The inverse of decodeReplicaStates.
func encodeReplicaStates(states []*transport.ReplicaState) []*monsterapb.ReplicaState {
	out := make([]*monsterapb.ReplicaState, len(states))
	for i, s := range states {
		out[i] = &monsterapb.ReplicaState{
			ReplicaId: s.ReplicaId,
			RaftStats: &monsterapb.RaftStats{
				State:             encodeTransportRaftState(s.RaftState),
				Term:              s.Stats.Term,
				LastLogIndex:      s.Stats.LastLogIndex,
				LastLogTerm:       s.Stats.LastLogTerm,
				CommitIndex:       s.Stats.CommitIndex,
				AppliedIndex:      s.Stats.AppliedIndex,
				FsmPending:        s.Stats.FSMPending,
				LastSnapshotIndex: s.Stats.LastSnapshotIndex,
				LastSnapshotTerm:  s.Stats.LastSnapshotTerm,
				NumPeers:          int32(s.Stats.NumPeers),
				LastContactNanos:  int64(s.Stats.LastContact),
			},
			Seeding:     s.Seeding,
			SeededIndex: s.SeededIndex,
			Frozen:      s.Frozen,
		}
	}
	return out
}

func encodeTransportRaftState(s transport.RaftState) monsterapb.RaftState {
	switch s {
	case transport.RaftStateFollower:
		return monsterapb.RaftState_RAFT_STATE_FOLLOWER
	case transport.RaftStateCandidate:
		return monsterapb.RaftState_RAFT_STATE_CANDIDATE
	case transport.RaftStateLeader:
		return monsterapb.RaftState_RAFT_STATE_LEADER
	case transport.RaftStateDead:
		return monsterapb.RaftState_RAFT_STATE_SHUTDOWN
	case transport.RaftStateSeeding:
		return monsterapb.RaftState_RAFT_STATE_SEEDING
	default:
		panic(fmt.Sprintf("unknown transport raft state: %v", s))
	}
}

// decodeRaftSnapshots converts the wire snapshot metadata into transport-level DTOs.
func decodeRaftSnapshots(s []*monsterapb.RaftSnapshot) []*transport.RaftSnapshot {
	ret := make([]*transport.RaftSnapshot, len(s))
	for i, snap := range s {
		ret[i] = &transport.RaftSnapshot{
			Id:    snap.Id,
			Index: snap.Index,
			Term:  snap.Term,
			Size:  snap.Size,
		}
	}
	return ret
}

func encodeRaftSnapshots(s []raft.SnapshotMetadata) []*monsterapb.RaftSnapshot {
	ret := make([]*monsterapb.RaftSnapshot, len(s))
	for i, s := range s {
		ret[i] = &monsterapb.RaftSnapshot{
			Id:    s.Id,
			Index: s.Index,
			Term:  s.Term,
			Size:  s.Size,
		}
	}
	return ret
}
