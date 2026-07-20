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
			ReplicaId: r.ReplicaId,
			RaftState: raftState,
			Stats:     stats,
		}
	}
	return states, nil
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

// encodeRaftStats renders the structured Raft stats onto the wire message. The
// field set is Monstera's own contract, independent of the underlying Raft
// library.
func encodeRaftStats(s raft.RaftStats) *monsterapb.RaftStats {
	return &monsterapb.RaftStats{
		State:             encodeRaftState(s.State),
		Term:              s.Term,
		LastLogIndex:      s.LastLogIndex,
		LastLogTerm:       s.LastLogTerm,
		CommitIndex:       s.CommitIndex,
		AppliedIndex:      s.AppliedIndex,
		FsmPending:        s.FSMPending,
		LastSnapshotIndex: s.LastSnapshotIndex,
		LastSnapshotTerm:  s.LastSnapshotTerm,
		NumPeers:          int32(s.NumPeers),
		LastContactNanos:  int64(s.LastContact),
	}
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

func encodeRaftState(s raft.RaftState) monsterapb.RaftState {
	switch s {
	case raft.Follower:
		return monsterapb.RaftState_RAFT_STATE_FOLLOWER
	case raft.Candidate:
		return monsterapb.RaftState_RAFT_STATE_CANDIDATE
	case raft.Leader:
		return monsterapb.RaftState_RAFT_STATE_LEADER
	case raft.Shutdown:
		return monsterapb.RaftState_RAFT_STATE_SHUTDOWN
	default:
		panic(fmt.Sprintf("Unknown enum value %v", s))
	}
}
