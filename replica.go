package monstera

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

var (
	errNotALeader = errors.New("updates are only available on a leader")
)

type MonsteraReplica struct {
	ApplicationName string
	KeyspaceName    string
	ShardId         string
	ReplicaId       string
	NodeId          string

	applicationCore ApplicationCore
	hraft           *hraft.Raft
	transport       *RaftAPI
	hstore          *HraftBadgerStore
	hfss            *hraft.FileSnapshotStore
}

// ApplicationCore is the interface that must be implemented by clients to be used
// with Monstera framework.
type ApplicationCore interface {
	// Read is used to read a value directly from the application core.
	// Reads can be performed concurrently with updates, other reads,
	// and snapshots. Read should panic on internal errors.
	Read(request []byte) []byte

	// Update is used to update the application core state.
	// All updates are applied to the application core sequentially,
	// in the order they are committed to the Raft log. This method is called
	// by the Raft thread. Update should panic on internal errors.
	Update(request []byte) []byte

	// Snapshot returns an ApplicationCoreSnapshot used to: support Raft log compaction,
	// to restore the application core to a previous state, or to bring out-of-date
	// Raft followers up to a recent log index.
	//
	// The Snapshot implementation should return quickly, because Update can not
	// be called while Snapshot is running. Generally this means Snapshot should
	// only capture a pointer to the state, and any expensive IO should happen
	// as part of ApplicationCoreSnapshot.Write.
	//
	// Update and Snapshot are always called from the same thread, but Update will
	// be called concurrently with ApplicationCoreSnapshot.Write. This means the
	// application core should be implemented to allow for concurrent updates while
	// a snapshot is happening.
	Snapshot() ApplicationCoreSnapshot // TODO partition range

	// Restore is used to restore an application core from a snapshot. It is not
	// called concurrently with any other command. The application core must discard
	// all previous state before restoring the snapshot.
	Restore(reader io.ReadCloser) error

	// Close is used to clean up resources used by the application core. Do not
	// clean up resources that are shared by multiple cores. Close is called
	// after a shard split or a shard move while Monstera node is running, and
	// is called for each core after Monstera node is shutdown.
	Close()
}

type ApplicationCoreSnapshot interface {
	Write(w io.Writer) error
	Release()
}

func (b *MonsteraReplica) Read(request []byte) ([]byte, error) {
	// TODO shutdown raft if Read panics
	response := b.applicationCore.Read(request)
	return response, nil
}

func (b *MonsteraReplica) Update(request []byte) ([]byte, error) {
	command := &MonsteraCommand{
		Payload: request,
	}
	commandBytes, err := proto.Marshal(command)
	if err != nil {
		return nil, err
	}

	f := b.hraft.Apply(commandBytes, 30*time.Second) // TODO timeout
	if err := f.Error(); err != nil {
		// TODO retry
		log.Print(err)
		return nil, err
	}

	response, ok := f.Response().([]byte)
	if !ok {
		return nil, fmt.Errorf("invalid response type %v", f.Response())
	}

	return response, nil
}

func (b *MonsteraReplica) Close() {
	// Shutdown the Raft node.
	f := b.hraft.Shutdown()
	err := f.Error()
	if err != nil {
		log.Printf("Failed to shutdown hraft: %v", err)
	}

	// Close the application core.
	b.applicationCore.Close()
}

func (b *MonsteraReplica) GetRaftStats() map[string]string {
	return b.hraft.Stats()
}

func (b *MonsteraReplica) GetRaftState() hraft.RaftState {
	return b.hraft.State()
}

func (b *MonsteraReplica) GetRaftLeader() (hraft.ServerAddress, hraft.ServerID) {
	return b.hraft.LeaderWithID()
}

func (b *MonsteraReplica) Bootstrap(servers []hraft.Server) {
	cfg := hraft.Configuration{
		Servers: servers,
	}
	f := b.hraft.BootstrapCluster(cfg)
	if err := f.Error(); err != nil && err != hraft.ErrCantBootstrap {
		panic(fmt.Errorf("raft.Raft.BootstrapCluster: %v", err))
	}
}

func (b *MonsteraReplica) AddVoter(replicaId string, address string) error {
	if b.hraft.State() != hraft.Leader {
		return errNotALeader
	}

	future := b.hraft.AddVoter(hraft.ServerID(replicaId), hraft.ServerAddress(address), 0, 10*time.Second)
	err := future.Error()
	if err != nil {
		return err
	}

	return nil
}

func (b *MonsteraReplica) AppendEntries(request *hraft.AppendEntriesRequest) (*hraft.AppendEntriesResponse, error) {
	ch := make(chan hraft.RPCResponse, 1)
	rpc := hraft.RPC{
		Command:  request,
		RespChan: ch,
		Reader:   nil,
	}

	// If heartbeat
	if request.Term != 0 && request.PrevLogEntry == 0 && request.PrevLogTerm == 0 && len(request.Entries) == 0 && request.LeaderCommitIndex == 0 {
		b.transport.Heartbeat(rpc)
	} else {
		b.transport.Producer() <- rpc
	}

	resp := <-ch
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.Response.(*hraft.AppendEntriesResponse), nil
}

func (b *MonsteraReplica) RequestVote(request *hraft.RequestVoteRequest) (*hraft.RequestVoteResponse, error) {
	ch := make(chan hraft.RPCResponse, 1)
	rpc := hraft.RPC{
		Command:  request,
		RespChan: ch,
		Reader:   nil,
	}

	b.transport.Producer() <- rpc

	resp := <-ch
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.Response.(*hraft.RequestVoteResponse), nil
}

func (b *MonsteraReplica) TimeoutNow(request *hraft.TimeoutNowRequest) (*hraft.TimeoutNowResponse, error) {
	ch := make(chan hraft.RPCResponse, 1)
	rpc := hraft.RPC{
		Command:  request,
		RespChan: ch,
		Reader:   nil,
	}

	b.transport.Producer() <- rpc

	resp := <-ch
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.Response.(*hraft.TimeoutNowResponse), nil
}

func (b *MonsteraReplica) InstallSnapshot(request *hraft.InstallSnapshotRequest, data io.Reader) (*hraft.InstallSnapshotResponse, error) {
	ch := make(chan hraft.RPCResponse, 1)
	rpc := hraft.RPC{
		Command:  request,
		RespChan: ch,
		Reader:   data,
	}

	b.transport.Producer() <- rpc

	resp := <-ch
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.Response.(*hraft.InstallSnapshotResponse), nil
}

func (b *MonsteraReplica) IsBootstrapped() bool {
	ok, err := hraft.HasExistingState(b.hstore, b.hstore, b.hfss)
	if err != nil {
		panic(fmt.Errorf("raft.HasExistingState: %v", err))
	}
	return ok
}

func NewMonsteraReplica(baseDir string, applicationName string, shardId string, replicaId string,
	myAddress string, applicationCore ApplicationCore, pool *MonsteraConnectionPool, raftStore *BadgerStore, restoreSnapshotOnStart bool) *MonsteraReplica {
	c := hraft.DefaultConfig()
	c.LocalID = hraft.ServerID(replicaId)
	c.LogLevel = "off"
	c.NoSnapshotRestoreOnStart = !restoreSnapshotOnStart
	c.NoLegacyTelemetry = true

	raftDir := filepath.Join(baseDir, replicaId, "raft")
	if err := os.MkdirAll(raftDir, os.ModePerm); err != nil {
		panic(err)
	}

	hstore := NewHraftBadgerStore(raftStore, []byte(replicaId)) // TODO shorten prefix

	hfss, err := hraft.NewFileSnapshotStore(raftDir, 2, os.Stderr)
	if err != nil {
		panic(fmt.Errorf("raft.NewFileSnapshotStore(%q, ...): %v", raftDir, err))
	}

	tm := NewTransport(myAddress, pool)

	fsm := NewRaftFSMAdapter(applicationCore)

	r, err := hraft.NewRaft(c, fsm, hstore, hstore, hfss, tm)
	if err != nil {
		panic(fmt.Errorf("raft.NewRaft: %v", err))
	}

	return &MonsteraReplica{
		ApplicationName: applicationName,
		ShardId:         shardId,
		ReplicaId:       replicaId,
		applicationCore: applicationCore,
		hraft:           r,
		hstore:          hstore,
		hfss:            hfss,
		transport:       tm,
	}
}
