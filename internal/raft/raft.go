package raft

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	hraft "github.com/hashicorp/raft"

	"github.com/evrblk/monstera/internal/raft/raftpb"
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/transport"
)

type RaftState uint32

const (
	Follower RaftState = iota
	Candidate
	Leader
	Shutdown
)

func (s RaftState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Shutdown:
		return "Shutdown"
	default:
		return "Unknown"
	}
}

type snapshotSessionResult struct {
	resp *hraft.InstallSnapshotResponse
	err  error
}

type snapshotSession struct {
	pipeWriter    *io.PipeWriter
	expectedSize  int64
	bytesReceived int64
	resultCh      chan snapshotSessionResult
}

// Raft wraps the HashiCorp Raft implementation with a Badger store backend.
type Raft struct {
	hraft        *hraft.Raft
	hstore       *HraftBadgerStore
	hfss         *hraft.FileSnapshotStore
	commandCodec CommandCodec
	transport    *RaftTransport

	snapshotSession   *snapshotSession
	snapshotSessionMu sync.Mutex
}

func (r *Raft) TriggerSnapshot() {
	r.hraft.Snapshot()
}

func (r *Raft) ListSnapshots() ([]*hraft.SnapshotMeta, error) {
	return r.hfss.List()
}

func (r *Raft) LeadershipTransfer() error {
	return r.hraft.LeadershipTransfer().Error()
}

func (r *Raft) IsBootstrapped() bool {
	ok, err := hraft.HasExistingState(r.hstore, r.hstore, r.hfss)
	if err != nil {
		panic(err)
	}
	return ok
}

func (r *Raft) Update(request []byte) ([]byte, error) {
	commandBytes, err := r.commandCodec.Encode(request)
	if err != nil {
		return nil, err
	}

	f := r.hraft.Apply(commandBytes, 30*time.Second) // TODO timeout
	if err := f.Error(); err != nil {
		// TODO retry
		// TODO what if FSM panics?
		return nil, err
	}

	response, ok := f.Response().([]byte)
	if !ok {
		return nil, fmt.Errorf("invalid response type %v", f.Response())
	}

	return response, nil
}

func (r *Raft) Close() error {
	// Shutdown the Raft node
	f := r.hraft.Shutdown()
	return f.Error()
}

func (r *Raft) GetRaftStats() map[string]string {
	return r.hraft.Stats()
}

func (r *Raft) GetRaftState() RaftState {
	switch r.hraft.State() {
	case hraft.Follower:
		return Follower
	case hraft.Candidate:
		return Candidate
	case hraft.Leader:
		return Leader
	case hraft.Shutdown:
		return Shutdown
	default:
		panic(fmt.Errorf("unknown raft state: %v", r.hraft.State()))
	}
}

// GetRaftLeader returns the leader's replica ID as a string. It blocks if a leader is
// unknown currently and waits until a leader is known or context is canceled.
func (r *Raft) GetRaftLeader(ctx context.Context) (string, error) {
	_, id := r.hraft.LeaderWithID()
	if id != "" {
		return string(id), nil
	}

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(100 * time.Millisecond):
			_, id := r.hraft.LeaderWithID()
			if id != "" {
				return string(id), nil
			}
		}
	}
}

// WaitForNewLeader blocks until a leader other than excludeId is elected or ctx is done.
// Use this when the previously known leader is unreachable and a new election is expected.
func (r *Raft) WaitForNewLeader(ctx context.Context, excludeId string) (string, error) {
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(100 * time.Millisecond):
			_, id := r.hraft.LeaderWithID()
			if id != "" && string(id) != excludeId {
				return string(id), nil
			}
		}
	}
}

func (r *Raft) Bootstrap(servers []hraft.Server) error {
	cfg := hraft.Configuration{
		Servers: servers,
	}
	f := r.hraft.BootstrapCluster(cfg)
	if err := f.Error(); err != nil && err != hraft.ErrCantBootstrap {
		return err
	}

	return nil
}

func (r *Raft) RaftMessage(request *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
	switch request.MessageType {
	case AppendEntriesRequest:
		msg := &raftpb.AppendEntriesRequest{}
		if err := msg.UnmarshalVT(request.Message); err != nil {
			return nil, err
		}
		resp, err := r.appendEntries(msg)
		if err != nil {
			return nil, err
		}
		data, err := resp.MarshalVT()
		if err != nil {
			return nil, err
		}
		return &transport.RaftMessageResponse{
			MessageType: AppendEntriesResponse,
			Message:     data,
		}, nil

	case RequestVoteRequest:
		msg := &raftpb.RequestVoteRequest{}
		if err := msg.UnmarshalVT(request.Message); err != nil {
			return nil, err
		}
		resp, err := r.requestVote(msg)
		if err != nil {
			return nil, err
		}
		data, err := resp.MarshalVT()
		if err != nil {
			return nil, err
		}
		return &transport.RaftMessageResponse{
			MessageType: RequestVoteResponse,
			Message:     data,
		}, nil

	case TimeoutNowRequest:
		msg := &raftpb.TimeoutNowRequest{}
		if err := msg.UnmarshalVT(request.Message); err != nil {
			return nil, err
		}
		resp, err := r.timeoutNow(msg)
		if err != nil {
			return nil, err
		}
		data, err := resp.MarshalVT()
		if err != nil {
			return nil, err
		}
		return &transport.RaftMessageResponse{
			MessageType: TimeoutNowResponse,
			Message:     data,
		}, nil

	case InstallSnapshotInitRequest:
		msg := &raftpb.InstallSnapshotInitRequest{}
		if err := msg.UnmarshalVT(request.Message); err != nil {
			return nil, err
		}
		return r.handleInstallSnapshotInitMessage(msg)

	case InstallSnapshotChunkRequest:
		msg := &raftpb.InstallSnapshotChunkRequest{}
		if err := msg.UnmarshalVT(request.Message); err != nil {
			return nil, err
		}
		return r.handleInstallSnapshotChunkMessage(msg)

	default:
		return nil, fmt.Errorf("unknown message type: %v", request.MessageType)
	}
}

// handleInstallSnapshotInitMessage starts a new snapshot session.
func (r *Raft) handleInstallSnapshotInitMessage(msg *raftpb.InstallSnapshotInitRequest) (*transport.RaftMessageResponse, error) {
	pr, pw := io.Pipe()
	session := &snapshotSession{
		pipeWriter:   pw,
		expectedSize: msg.Size,
		resultCh:     make(chan snapshotSessionResult, 1),
	}

	r.snapshotSessionMu.Lock()
	r.snapshotSession = session
	r.snapshotSessionMu.Unlock()

	hreq := decodeInstallSnapshotInitRequest(msg)
	go func() {
		resp, err := r.installSnapshot(hreq, pr)
		session.resultCh <- snapshotSessionResult{resp: resp, err: err}
	}()

	if msg.Size == 0 {
		pw.Close()
		return r.finishSnapshotSession(session)
	}

	return &transport.RaftMessageResponse{MessageType: InstallSnapshotInitResponse}, nil
}

// handleInstallSnapshotChunkMessage writes data to the active session.
func (r *Raft) handleInstallSnapshotChunkMessage(msg *raftpb.InstallSnapshotChunkRequest) (*transport.RaftMessageResponse, error) {
	r.snapshotSessionMu.Lock()
	session := r.snapshotSession
	r.snapshotSessionMu.Unlock()

	if session == nil {
		return nil, fmt.Errorf("no active snapshot session")
	}

	if _, err := session.pipeWriter.Write(msg.Data); err != nil {
		session.pipeWriter.CloseWithError(err)
		r.snapshotSessionMu.Lock()
		r.snapshotSession = nil
		r.snapshotSessionMu.Unlock()
		<-session.resultCh
		return nil, err
	}
	session.bytesReceived += int64(len(msg.Data))

	if session.bytesReceived >= session.expectedSize {
		session.pipeWriter.Close()
		return r.finishSnapshotSession(session)
	}

	return &transport.RaftMessageResponse{MessageType: InstallSnapshotChunkResponse}, nil
}

func (r *Raft) finishSnapshotSession(session *snapshotSession) (*transport.RaftMessageResponse, error) {
	r.snapshotSessionMu.Lock()
	r.snapshotSession = nil
	r.snapshotSessionMu.Unlock()

	result := <-session.resultCh
	if result.err != nil {
		return nil, result.err
	}
	data, err := encodeInstallSnapshotChunkResponse(result.resp).MarshalVT()
	if err != nil {
		return nil, err
	}
	return &transport.RaftMessageResponse{
		MessageType: InstallSnapshotChunkResponse,
		Message:     data,
	}, nil
}

func (r *Raft) appendEntries(request *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	ch := make(chan hraft.RPCResponse, 1)
	rpc := hraft.RPC{
		Command:  decodeAppendEntriesRequest(request),
		RespChan: ch,
	}

	// Heartbeat detection: use the fast path to avoid head-of-line blocking.
	if request.Term != 0 && request.PrevLogEntry == 0 && request.PrevLogTerm == 0 &&
		len(request.Entries) == 0 && request.LeaderCommitIndex == 0 {
		r.transport.Heartbeat(rpc)
	} else {
		r.transport.Producer() <- rpc
	}

	resp := <-ch
	if resp.Error != nil {
		return nil, resp.Error
	}
	return encodeAppendEntriesResponse(resp.Response.(*hraft.AppendEntriesResponse)), nil
}

func (r *Raft) requestVote(request *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	ch := make(chan hraft.RPCResponse, 1)
	rpc := hraft.RPC{
		Command:  decodeRequestVoteRequest(request),
		RespChan: ch,
	}

	r.transport.Producer() <- rpc

	resp := <-ch
	if resp.Error != nil {
		return nil, resp.Error
	}
	return encodeRequestVoteResponse(resp.Response.(*hraft.RequestVoteResponse)), nil
}

func (r *Raft) timeoutNow(request *raftpb.TimeoutNowRequest) (*raftpb.TimeoutNowResponse, error) {
	ch := make(chan hraft.RPCResponse, 1)
	rpc := hraft.RPC{
		Command:  decodeTimeoutNowRequest(request),
		RespChan: ch,
	}

	r.transport.Producer() <- rpc

	resp := <-ch
	if resp.Error != nil {
		return nil, resp.Error
	}
	return encodeTimeoutNowResponse(resp.Response.(*hraft.TimeoutNowResponse)), nil
}

func (r *Raft) installSnapshot(request *hraft.InstallSnapshotRequest, data io.Reader) (*hraft.InstallSnapshotResponse, error) {
	ch := make(chan hraft.RPCResponse, 1)
	rpc := hraft.RPC{
		Command:  request,
		RespChan: ch,
		Reader:   data,
	}

	r.transport.Producer() <- rpc

	resp := <-ch
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.Response.(*hraft.InstallSnapshotResponse), nil
}

func NewRaft(baseDir string, myAddress string, replicaId string, core AppCore, trans transport.Transport, raftStore *store.BadgerStore, restoreSnapshotOnStart bool) *Raft {
	cfg := hraft.DefaultConfig()
	cfg.LocalID = hraft.ServerID(replicaId)
	cfg.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   replicaId,
		Level:  hclog.LevelFromString("error"),
		Output: os.Stdout,
	}) // TODO: pass logger
	cfg.NoSnapshotRestoreOnStart = !restoreSnapshotOnStart
	cfg.NoLegacyTelemetry = true
	cfg.ElectionTimeout = 2 * time.Second
	cfg.HeartbeatTimeout = 2 * time.Second

	raftDir := filepath.Join(baseDir, replicaId, "raft")
	if err := os.MkdirAll(raftDir, os.ModePerm); err != nil {
		panic(err)
	}

	logCodec := &protoLogCodec{}
	cmdCodec := &protoCommandCodec{}

	hstore := NewHraftBadgerStore(raftStore, []byte(replicaId), logCodec)

	hfss, err := hraft.NewFileSnapshotStore(raftDir, 3, os.Stderr)
	if err != nil {
		panic(fmt.Errorf("raft.NewFileSnapshotStore(%q, ...): %v", raftDir, err))
	}

	transport := NewRaftTransport(myAddress, trans)

	fsm := NewFSMAdapter(core, cmdCodec)

	r, err := hraft.NewRaft(cfg, fsm, hstore, hstore, hfss, transport)
	if err != nil {
		panic(err)
	}

	return &Raft{
		hraft:        r,
		hstore:       hstore,
		hfss:         hfss,
		transport:    transport,
		commandCodec: cmdCodec,
	}
}
