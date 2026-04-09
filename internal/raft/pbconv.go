package raft

import (
	"time"

	hraft "github.com/hashicorp/raft"

	"github.com/evrblk/monstera/internal/raft/raftpb"
)

func encodeRPCHeader(s hraft.RPCHeader) *raftpb.RPCHeader {
	return &raftpb.RPCHeader{
		ProtocolVersion: int64(s.ProtocolVersion),
		Id:              s.ID,
		Addr:            s.Addr,
	}
}

func encodeAppendEntriesResponse(s *hraft.AppendEntriesResponse) *raftpb.AppendEntriesResponse {
	return &raftpb.AppendEntriesResponse{
		RpcHeader:      encodeRPCHeader(s.RPCHeader),
		Term:           s.Term,
		LastLog:        s.LastLog,
		Success:        s.Success,
		NoRetryBackoff: s.NoRetryBackoff,
	}
}

func encodeRequestVoteResponse(s *hraft.RequestVoteResponse) *raftpb.RequestVoteResponse {
	return &raftpb.RequestVoteResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
		Term:      s.Term,
		Peers:     s.Peers,
		Granted:   s.Granted,
	}
}

func encodeInstallSnapshotChunkResponse(s *hraft.InstallSnapshotResponse) *raftpb.InstallSnapshotChunkResponse {
	return &raftpb.InstallSnapshotChunkResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
		Term:      s.Term,
		Success:   s.Success,
	}
}

func encodeTimeoutNowResponse(s *hraft.TimeoutNowResponse) *raftpb.TimeoutNowResponse {
	return &raftpb.TimeoutNowResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
	}
}

func decodeAppendEntriesRequest(m *raftpb.AppendEntriesRequest) *hraft.AppendEntriesRequest {
	return &hraft.AppendEntriesRequest{
		RPCHeader:         decodeRPCHeader(m.RpcHeader),
		Term:              m.Term,
		PrevLogEntry:      m.PrevLogEntry,
		PrevLogTerm:       m.PrevLogTerm,
		Entries:           decodeLogs(m.Entries),
		LeaderCommitIndex: m.LeaderCommitIndex,
	}
}

func decodeRPCHeader(m *raftpb.RPCHeader) hraft.RPCHeader {
	return hraft.RPCHeader{
		ProtocolVersion: hraft.ProtocolVersion(m.ProtocolVersion),
		ID:              m.Id,
		Addr:            m.Addr,
	}
}

func decodeLogs(m []*raftpb.Log) []*hraft.Log {
	ret := make([]*hraft.Log, len(m))
	for i, l := range m {
		ret[i] = decodeLog(l)
	}
	return ret
}

func decodeLog(m *raftpb.Log) *hraft.Log {
	return &hraft.Log{
		Index:      m.Index,
		Term:       m.Term,
		Type:       decodeLogType(m.Type),
		Data:       m.Data,
		Extensions: m.Extensions,
		AppendedAt: time.Unix(0, m.AppendedAt),
	}
}

func decodeLogType(m raftpb.Log_LogType) hraft.LogType {
	switch m {
	case raftpb.Log_LOG_TYPE_COMMAND:
		return hraft.LogCommand
	case raftpb.Log_LOG_TYPE_NOOP:
		return hraft.LogNoop
	case raftpb.Log_LOG_TYPE_ADD_PEER_DEPRECATED:
		return hraft.LogAddPeerDeprecated
	case raftpb.Log_LOG_TYPE_REMOVE_PEER_DEPRECATED:
		return hraft.LogRemovePeerDeprecated
	case raftpb.Log_LOG_TYPE_BARRIER:
		return hraft.LogBarrier
	case raftpb.Log_LOG_TYPE_CONFIGURATION:
		return hraft.LogConfiguration
	default:
		panic("invalid LogType")
	}
}

func decodeRequestVoteRequest(m *raftpb.RequestVoteRequest) *hraft.RequestVoteRequest {
	return &hraft.RequestVoteRequest{
		RPCHeader:          decodeRPCHeader(m.RpcHeader),
		Term:               m.Term,
		LastLogIndex:       m.LastLogIndex,
		LastLogTerm:        m.LastLogTerm,
		LeadershipTransfer: m.LeadershipTransfer,
	}
}

func decodeInstallSnapshotInitRequest(m *raftpb.InstallSnapshotInitRequest) *hraft.InstallSnapshotRequest {
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

func decodeTimeoutNowRequest(m *raftpb.TimeoutNowRequest) *hraft.TimeoutNowRequest {
	return &hraft.TimeoutNowRequest{
		RPCHeader: decodeRPCHeader(m.RpcHeader),
	}
}

func decodeAppendEntriesResponse(m *raftpb.AppendEntriesResponse) *hraft.AppendEntriesResponse {
	return &hraft.AppendEntriesResponse{
		RPCHeader:      decodeRPCHeader(m.RpcHeader),
		Term:           m.Term,
		LastLog:        m.LastLog,
		Success:        m.Success,
		NoRetryBackoff: m.NoRetryBackoff,
	}
}

func decodeRequestVoteResponse(m *raftpb.RequestVoteResponse) *hraft.RequestVoteResponse {
	return &hraft.RequestVoteResponse{
		RPCHeader: decodeRPCHeader(m.RpcHeader),
		Term:      m.Term,
		Peers:     m.Peers,
		Granted:   m.Granted,
	}
}
func decodeInstallSnapshotChunkResponse(m *raftpb.InstallSnapshotChunkResponse) *hraft.InstallSnapshotResponse {
	return &hraft.InstallSnapshotResponse{
		RPCHeader: decodeRPCHeader(m.RpcHeader),
		Term:      m.Term,
		Success:   m.Success,
	}
}

func decodeTimeoutNowResponse(m *raftpb.TimeoutNowResponse) *hraft.TimeoutNowResponse {
	return &hraft.TimeoutNowResponse{
		RPCHeader: decodeRPCHeader(m.RpcHeader),
	}
}

func encodeAppendEntriesRequest(s *hraft.AppendEntriesRequest) *raftpb.AppendEntriesRequest {
	return &raftpb.AppendEntriesRequest{
		RpcHeader:         encodeRPCHeader(s.RPCHeader),
		Term:              s.Term,
		PrevLogEntry:      s.PrevLogEntry,
		PrevLogTerm:       s.PrevLogTerm,
		Entries:           encodeLogs(s.Entries),
		LeaderCommitIndex: s.LeaderCommitIndex,
	}
}

func encodeLogs(s []*hraft.Log) []*raftpb.Log {
	ret := make([]*raftpb.Log, len(s))
	for i, l := range s {
		ret[i] = encodeLog(l)
	}
	return ret
}

func encodeLog(s *hraft.Log) *raftpb.Log {
	return &raftpb.Log{
		Index:      s.Index,
		Term:       s.Term,
		Type:       encodeLogType(s.Type),
		Data:       s.Data,
		Extensions: s.Extensions,
		AppendedAt: s.AppendedAt.UnixNano(),
	}
}

func encodeLogType(s hraft.LogType) raftpb.Log_LogType {
	switch s {
	case hraft.LogCommand:
		return raftpb.Log_LOG_TYPE_COMMAND
	case hraft.LogNoop:
		return raftpb.Log_LOG_TYPE_NOOP
	case hraft.LogAddPeerDeprecated:
		return raftpb.Log_LOG_TYPE_ADD_PEER_DEPRECATED
	case hraft.LogRemovePeerDeprecated:
		return raftpb.Log_LOG_TYPE_REMOVE_PEER_DEPRECATED
	case hraft.LogBarrier:
		return raftpb.Log_LOG_TYPE_BARRIER
	case hraft.LogConfiguration:
		return raftpb.Log_LOG_TYPE_CONFIGURATION
	default:
		panic("invalid LogType")
	}
}

func encodeRequestVoteRequest(s *hraft.RequestVoteRequest) *raftpb.RequestVoteRequest {
	return &raftpb.RequestVoteRequest{
		RpcHeader:          encodeRPCHeader(s.RPCHeader),
		Term:               s.Term,
		LastLogIndex:       s.LastLogIndex,
		LastLogTerm:        s.LastLogTerm,
		LeadershipTransfer: s.LeadershipTransfer,
	}
}

func encodeInstallSnapshotInitRequest(s *hraft.InstallSnapshotRequest) *raftpb.InstallSnapshotInitRequest {
	return &raftpb.InstallSnapshotInitRequest{
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
	}
}

func encodeTimeoutNowRequest(s *hraft.TimeoutNowRequest, targetReplicaId string) *raftpb.TimeoutNowRequest {
	return &raftpb.TimeoutNowRequest{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
	}
}
