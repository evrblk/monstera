package monstera

import (
	"fmt"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func encodeRPCHeader(s hraft.RPCHeader) *RPCHeader {
	return &RPCHeader{
		ProtocolVersion: int64(s.ProtocolVersion),
		Id:              s.ID,
		Addr:            s.Addr,
	}
}

func encodeRaftState(s hraft.RaftState) RaftState {
	switch s {
	case hraft.Follower:
		return RaftState_RAFT_STATE_FOLLOWER
	case hraft.Candidate:
		return RaftState_RAFT_STATE_CANDIDATE
	case hraft.Shutdown:
		return RaftState_RAFT_STATE_SHUTDOWN
	case hraft.Leader:
		return RaftState_RAFT_STATE_LEADER
	default:
		panic(fmt.Sprintf("Unknown enum value %v", s))
	}
}

func encodeAppendEntriesResponse(s *hraft.AppendEntriesResponse) *AppendEntriesResponse {
	return &AppendEntriesResponse{
		RpcHeader:      encodeRPCHeader(s.RPCHeader),
		Term:           s.Term,
		LastLog:        s.LastLog,
		Success:        s.Success,
		NoRetryBackoff: s.NoRetryBackoff,
	}
}

func encodeRequestVoteResponse(s *hraft.RequestVoteResponse) *RequestVoteResponse {
	return &RequestVoteResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
		Term:      s.Term,
		Peers:     s.Peers,
		Granted:   s.Granted,
	}
}

func encodeInstallSnapshotResponse(s *hraft.InstallSnapshotResponse) *InstallSnapshotResponse {
	return &InstallSnapshotResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
		Term:      s.Term,
		Success:   s.Success,
	}
}

func encodeTimeoutNowResponse(s *hraft.TimeoutNowResponse) *TimeoutNowResponse {
	return &TimeoutNowResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
	}
}

func decodeAppendEntriesRequest(m *AppendEntriesRequest) *hraft.AppendEntriesRequest {
	return &hraft.AppendEntriesRequest{
		RPCHeader:         decodeRPCHeader(m.RpcHeader),
		Term:              m.Term,
		PrevLogEntry:      m.PrevLogEntry,
		PrevLogTerm:       m.PrevLogTerm,
		Entries:           decodeLogs(m.Entries),
		LeaderCommitIndex: m.LeaderCommitIndex,
	}
}

func decodeRPCHeader(m *RPCHeader) hraft.RPCHeader {
	return hraft.RPCHeader{
		ProtocolVersion: hraft.ProtocolVersion(m.ProtocolVersion),
		ID:              m.Id,
		Addr:            m.Addr,
	}
}

func decodeLogs(m []*Log) []*hraft.Log {
	ret := make([]*hraft.Log, len(m))
	for i, l := range m {
		ret[i] = decodeLog(l)
	}
	return ret
}

func decodeLog(m *Log) *hraft.Log {
	return &hraft.Log{
		Index:      m.Index,
		Term:       m.Term,
		Type:       decodeLogType(m.Type),
		Data:       m.Data,
		Extensions: m.Extensions,
		AppendedAt: m.AppendedAt.AsTime(),
	}
}

func decodeLogType(m Log_LogType) hraft.LogType {
	switch m {
	case Log_LOG_TYPE_COMMAND:
		return hraft.LogCommand
	case Log_LOG_TYPE_NOOP:
		return hraft.LogNoop
	case Log_LOG_TYPE_ADD_PEER_DEPRECATED:
		return hraft.LogAddPeerDeprecated
	case Log_LOG_TYPE_REMOVE_PEER_DEPRECATED:
		return hraft.LogRemovePeerDeprecated
	case Log_LOG_TYPE_BARRIER:
		return hraft.LogBarrier
	case Log_LOG_TYPE_CONFIGURATION:
		return hraft.LogConfiguration
	default:
		panic("invalid LogType")
	}
}

func decodeRequestVoteRequest(m *RequestVoteRequest) *hraft.RequestVoteRequest {
	return &hraft.RequestVoteRequest{
		RPCHeader:          decodeRPCHeader(m.RpcHeader),
		Term:               m.Term,
		LastLogIndex:       m.LastLogIndex,
		LastLogTerm:        m.LastLogTerm,
		LeadershipTransfer: m.LeadershipTransfer,
	}
}

func decodeInstallSnapshotRequest(m *InstallSnapshotRequest) *hraft.InstallSnapshotRequest {
	return &hraft.InstallSnapshotRequest{
		RPCHeader:          decodeRPCHeader(m.RpcHeader),
		SnapshotVersion:    hraft.SnapshotVersion(m.SnapshotVersion),
		Term:               m.Term,
		Leader:             m.Leader,
		LastLogIndex:       m.LastLogIndex,
		LastLogTerm:        m.LastLogTerm,
		Peers:              m.Peers,
		Configuration:      m.Configuration,
		ConfigurationIndex: m.ConfigurationIndex,
		Size:               m.Size,
	}
}

func decodeTimeoutNowRequest(m *TimeoutNowRequest) *hraft.TimeoutNowRequest {
	return &hraft.TimeoutNowRequest{
		RPCHeader: decodeRPCHeader(m.RpcHeader),
	}
}

func decodeAppendEntriesResponse(m *AppendEntriesResponse) *hraft.AppendEntriesResponse {
	return &hraft.AppendEntriesResponse{
		RPCHeader:      decodeRPCHeader(m.RpcHeader),
		Term:           m.Term,
		LastLog:        m.LastLog,
		Success:        m.Success,
		NoRetryBackoff: m.NoRetryBackoff,
	}
}

func decodeRequestVoteResponse(m *RequestVoteResponse) *hraft.RequestVoteResponse {
	return &hraft.RequestVoteResponse{
		RPCHeader: decodeRPCHeader(m.RpcHeader),
		Term:      m.Term,
		Peers:     m.Peers,
		Granted:   m.Granted,
	}
}
func decodeInstallSnapshotResponse(m *InstallSnapshotResponse) *hraft.InstallSnapshotResponse {
	return &hraft.InstallSnapshotResponse{
		RPCHeader: decodeRPCHeader(m.RpcHeader),
		Term:      m.Term,
		Success:   m.Success,
	}
}

func decodeTimeoutNowResponse(m *TimeoutNowResponse) *hraft.TimeoutNowResponse {
	return &hraft.TimeoutNowResponse{
		RPCHeader: decodeRPCHeader(m.RpcHeader),
	}
}

func encodeAppendEntriesRequest(s *hraft.AppendEntriesRequest, targetReplicaId string) *AppendEntriesRequest {
	return &AppendEntriesRequest{
		RpcHeader:         encodeRPCHeader(s.RPCHeader),
		Term:              s.Term,
		PrevLogEntry:      s.PrevLogEntry,
		PrevLogTerm:       s.PrevLogTerm,
		Entries:           encodeLogs(s.Entries),
		LeaderCommitIndex: s.LeaderCommitIndex,
		TargetReplicaId:   targetReplicaId,
	}
}

func encodeLogs(s []*hraft.Log) []*Log {
	ret := make([]*Log, len(s))
	for i, l := range s {
		ret[i] = encodeLog(l)
	}
	return ret
}

func encodeLog(s *hraft.Log) *Log {
	return &Log{
		Index:      s.Index,
		Term:       s.Term,
		Type:       encodeLogType(s.Type),
		Data:       s.Data,
		Extensions: s.Extensions,
		AppendedAt: timestamppb.New(s.AppendedAt),
	}
}

func encodeLogType(s hraft.LogType) Log_LogType {
	switch s {
	case hraft.LogCommand:
		return Log_LOG_TYPE_COMMAND
	case hraft.LogNoop:
		return Log_LOG_TYPE_NOOP
	case hraft.LogAddPeerDeprecated:
		return Log_LOG_TYPE_ADD_PEER_DEPRECATED
	case hraft.LogRemovePeerDeprecated:
		return Log_LOG_TYPE_REMOVE_PEER_DEPRECATED
	case hraft.LogBarrier:
		return Log_LOG_TYPE_BARRIER
	case hraft.LogConfiguration:
		return Log_LOG_TYPE_CONFIGURATION
	default:
		panic("invalid LogType")
	}
}

func encodeRequestVoteRequest(s *hraft.RequestVoteRequest, targetReplicaId string) *RequestVoteRequest {
	return &RequestVoteRequest{
		RpcHeader:          encodeRPCHeader(s.RPCHeader),
		Term:               s.Term,
		LastLogIndex:       s.LastLogIndex,
		LastLogTerm:        s.LastLogTerm,
		LeadershipTransfer: s.LeadershipTransfer,
		TargetReplicaId:    targetReplicaId,
	}
}

func encodeInstallSnapshotRequest(s *hraft.InstallSnapshotRequest, targetReplicaId string) *InstallSnapshotRequest {
	return &InstallSnapshotRequest{
		RpcHeader:          encodeRPCHeader(s.RPCHeader),
		SnapshotVersion:    int64(s.SnapshotVersion),
		Term:               s.Term,
		Leader:             s.Leader,
		LastLogIndex:       s.LastLogIndex,
		LastLogTerm:        s.LastLogTerm,
		Peers:              s.Peers,
		Configuration:      s.Configuration,
		ConfigurationIndex: s.ConfigurationIndex,
		Size:               s.Size,
		TargetReplicaId:    targetReplicaId,
	}
}

func encodeTimeoutNowRequest(s *hraft.TimeoutNowRequest, targetReplicaId string) *TimeoutNowRequest {
	return &TimeoutNowRequest{
		RpcHeader:       encodeRPCHeader(s.RPCHeader),
		TargetReplicaId: targetReplicaId,
	}
}
