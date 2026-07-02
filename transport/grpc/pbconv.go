package grpc

import (
	"fmt"

	"github.com/evrblk/monstera/internal/raft"
	"github.com/evrblk/monstera/transport/grpc/monsterapb"
)

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
	case raft.Shutdown:
		return monsterapb.RaftState_RAFT_STATE_SHUTDOWN
	case raft.Leader:
		return monsterapb.RaftState_RAFT_STATE_LEADER
	default:
		panic(fmt.Sprintf("Unknown enum value %v", s))
	}
}
