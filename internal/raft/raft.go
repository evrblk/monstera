package raft

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
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
	startTime     time.Time
}

// Raft wraps the HashiCorp Raft implementation with a Badger store backend.
type Raft struct {
	hraft     *hraft.Raft
	hstore    *HraftBadgerStore
	hfss      *hraft.FileSnapshotStore
	transport *RaftTransport

	// applicationName, shardId and replicaId identify this replica in metrics.
	applicationName string
	shardId         string
	replicaId       string

	snapshotSession   *snapshotSession
	snapshotSessionMu sync.Mutex

	// updateTimeout bounds how long r.hraft.Apply waits for a log entry to be
	// committed and applied before returning an error.
	updateTimeout time.Duration
}

func (r *Raft) TriggerSnapshot() {
	r.hraft.Snapshot()
}

// SnapshotMetadata describes a stored Raft snapshot. It is a library-agnostic
// view of the underlying snapshot metadata so callers outside this package do
// not depend on the concrete Raft implementation.
type SnapshotMetadata struct {
	Id    string
	Index uint64
	Term  uint64
	Size  int64
}

func (r *Raft) ListSnapshots() ([]SnapshotMetadata, error) {
	metas, err := r.hfss.List()
	if err != nil {
		return nil, err
	}

	result := make([]SnapshotMetadata, len(metas))
	for i, m := range metas {
		result[i] = SnapshotMetadata{
			Id:    m.ID,
			Index: m.Index,
			Term:  m.Term,
			Size:  m.Size,
		}
	}
	return result, nil
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

func (r *Raft) Update(request []byte) (any, error) {
	t1 := time.Now()
	f := r.hraft.Apply(request, r.updateTimeout)
	err := f.Error()
	applyDuration.WithLabelValues(r.applicationName, r.shardId, r.replicaId).Observe(time.Since(t1).Seconds())
	if err != nil {
		// TODO retry
		// TODO what if FSM panics?
		applyErrorsTotal.WithLabelValues(r.applicationName, r.shardId, r.replicaId, applyErrorReason(err)).Inc()
		return nil, err
	}

	return f.Response(), nil
}

func (r *Raft) Close() error {
	// Shutdown the Raft node
	f := r.hraft.Shutdown()
	return f.Error()
}

// RaftStats is a library-agnostic snapshot of a replica's Raft state, used for
// health and monitoring. Most fields are standard Raft concepts (term, log and
// commit/apply indexes, snapshot index/term, membership). FSMPending and
// LastContact are not part of the Raft paper but are generic operational
// signals any implementation can supply.
type RaftStats struct {
	// State is the current role of this replica in its Raft group.
	State RaftState
	// Term is the current Raft term.
	Term uint64
	// LastLogIndex and LastLogTerm describe the last entry in the local log.
	LastLogIndex uint64
	LastLogTerm  uint64
	// CommitIndex is the highest log index known to be committed.
	CommitIndex uint64
	// AppliedIndex is the highest log index applied to the FSM.
	AppliedIndex uint64
	// FSMPending is the number of committed entries queued to apply to the FSM.
	FSMPending uint64
	// LastSnapshotIndex and LastSnapshotTerm describe the most recent snapshot.
	LastSnapshotIndex uint64
	LastSnapshotTerm  uint64
	// NumPeers is the number of other voting members in the group.
	NumPeers int
	// LastContact is the time since this replica last heard from the leader. It
	// is 0 on the leader itself, and -1 when there has been no contact yet.
	LastContact time.Duration
}

func (r *Raft) GetRaftStats() RaftStats {
	s := r.hraft.Stats()
	return RaftStats{
		State:             r.GetRaftState(),
		Term:              statsUint64(s, "term"),
		LastLogIndex:      statsUint64(s, "last_log_index"),
		LastLogTerm:       statsUint64(s, "last_log_term"),
		CommitIndex:       statsUint64(s, "commit_index"),
		AppliedIndex:      statsUint64(s, "applied_index"),
		FSMPending:        statsUint64(s, "fsm_pending"),
		LastSnapshotIndex: statsUint64(s, "last_snapshot_index"),
		LastSnapshotTerm:  statsUint64(s, "last_snapshot_term"),
		NumPeers:          statsInt(s, "num_peers"),
		LastContact:       parseLastContact(s["last_contact"]),
	}
}

func statsUint64(m map[string]string, key string) uint64 {
	v, err := strconv.ParseUint(m[key], 10, 64)
	if err != nil {
		return 0
	}
	return v
}

func statsInt(m map[string]string, key string) int {
	v, err := strconv.Atoi(m[key])
	if err != nil {
		return 0
	}
	return v
}

// parseLastContact interprets the "last_contact" stat. The underlying value is
// "never" (no contact yet), "0" (this replica is the leader), or a duration
// string. They map to -1, 0, and the parsed duration respectively.
func parseLastContact(s string) time.Duration {
	switch s {
	case "never":
		return -1
	case "", "0":
		return 0
	default:
		d, err := time.ParseDuration(s)
		if err != nil {
			return -1
		}
		return d
	}
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

// RaftServer identifies a voting member of a Raft group. It is a
// library-agnostic bootstrap descriptor: ReplicaId is the member's id and
// NodeId is the address the transport resolves to a network endpoint.
type RaftServer struct {
	ReplicaId string
	NodeId    string
}

func (r *Raft) Bootstrap(servers []RaftServer) error {
	hservers := make([]hraft.Server, len(servers))
	for i, s := range servers {
		hservers[i] = hraft.Server{
			Suffrage: hraft.Voter,
			ID:       hraft.ServerID(s.ReplicaId),
			Address:  hraft.ServerAddress(s.NodeId),
		}
	}

	cfg := hraft.Configuration{
		Servers: hservers,
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
		startTime:    time.Now(),
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
	RecordSnapshot(r.applicationName, r.shardId, r.replicaId, "install", time.Since(session.startTime), session.bytesReceived, result.err)
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

func NewRaft(baseDir string, myAddress string, applicationName string, shardId string, replicaId string, core AppCore, trans transport.Transport, raftStore *store.BadgerStore, restoreSnapshotOnStart bool, updateTimeout time.Duration) *Raft {
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

	hstore := NewHraftBadgerStore(raftStore, []byte(replicaId), logCodec)

	hfss, err := hraft.NewFileSnapshotStore(raftDir, 3, os.Stderr)
	if err != nil {
		panic(fmt.Errorf("raft.NewFileSnapshotStore(%q, ...): %v", raftDir, err))
	}

	transport := NewRaftTransport(myAddress, trans)

	fsm := NewFSMAdapter(core)

	r, err := hraft.NewRaft(cfg, fsm, hstore, hstore, hfss, transport)
	if err != nil {
		panic(err)
	}

	return &Raft{
		hraft:           r,
		hstore:          hstore,
		hfss:            hfss,
		transport:       transport,
		applicationName: applicationName,
		shardId:         shardId,
		replicaId:       replicaId,
		updateTimeout:   updateTimeout,
	}
}
