package monstera

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	hraft "github.com/hashicorp/raft"

	"github.com/evrblk/monstera/internal/raft"
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/transport"
)

// replica manages a single Raft replica for one shard.
type replica struct {
	applicationName string
	shardId         string
	replicaId       string

	core *appCoreAdapter
	raft *raft.Raft

	logger *log.Logger
}

func (r *replica) Read(request []byte) (response []byte, err error) {
	defer func() {
		if p := recover(); p != nil {
			r.logger.Printf("panic in core.Read, shutting down raft: %v", p)
			r.raft.Close()
			err = fmt.Errorf("core.Read panicked: %v", p)
		}
	}()

	return r.core.Read(request)
}

func (r *replica) Update(request []byte) ([]byte, error) {
	return r.raft.Update(request)
}

func (r *replica) Close() {
	// Close the Raft node
	r.raft.Close()

	// Close the application core
	r.core.Close()
}

func (r *replica) GetRaftStats() map[string]string {
	return r.raft.GetRaftStats()
}

func (r *replica) GetRaftState() raft.RaftState {
	return r.raft.GetRaftState()
}

func (r *replica) IsLeader() bool {
	return r.raft.GetRaftState() == raft.Leader
}

func (r *replica) GetRaftLeader(ctx context.Context) (string, error) {
	return r.raft.GetRaftLeader(ctx)
}

func (r *replica) WaitForNewLeader(ctx context.Context, excludeId string) (string, error) {
	return r.raft.WaitForNewLeader(ctx, excludeId)
}

func (r *replica) Bootstrap(servers []hraft.Server) error {
	return r.raft.Bootstrap(servers)
}

func (r *replica) IsBootstrapped() bool {
	return r.raft.IsBootstrapped()
}

func (r *replica) TriggerSnapshot() {
	r.raft.TriggerSnapshot()
}

func (r *replica) RaftMessage(request *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
	return r.raft.RaftMessage(request)
}

func (r *replica) ListSnapshots() ([]*hraft.SnapshotMeta, error) {
	return r.raft.ListSnapshots()
}

func (r *replica) LeadershipTransfer() error {
	return r.raft.LeadershipTransfer()
}

func (r *replica) GetReplicaId() string {
	return r.replicaId
}

func newReplica(baseDir string, applicationName string, shardId string, replicaId string,
	myAddress string, core ApplicationCore, trans transport.Transport, raftStore *store.BadgerStore, restoreSnapshotOnStart bool) *replica {
	adapter := &appCoreAdapter{core: core}

	rep := &replica{
		applicationName: applicationName,
		shardId:         shardId,
		replicaId:       replicaId,
		core:            adapter,
		logger:          log.New(os.Stderr, fmt.Sprintf("[%s]", replicaId), log.LstdFlags),
	}

	rep.raft = raft.NewRaft(baseDir, myAddress, replicaId, adapter, trans, raftStore, restoreSnapshotOnStart)

	return rep
}

type appCoreAdapter struct {
	// coreMu protects core from concurrent reads during snapshot restoration.
	// Read acquires RLock; Restore acquires Lock.
	coreMu sync.RWMutex
	core   ApplicationCore
}

var _ raft.AppCore = (*appCoreAdapter)(nil)

func (a *appCoreAdapter) Read(request []byte) ([]byte, error) {
	a.coreMu.RLock()
	defer a.coreMu.RUnlock()
	response := a.core.Read(request)
	return response, nil
}

func (a *appCoreAdapter) Update(request []byte) []byte {
	return a.core.Update(request)
}

func (a *appCoreAdapter) Snapshot() raft.AppCoreSnapshot {
	return a.core.Snapshot()
}

func (a *appCoreAdapter) Restore(reader io.ReadCloser) error {
	a.coreMu.Lock()
	defer a.coreMu.Unlock()
	return a.core.Restore(reader)
}

func (a *appCoreAdapter) Close() {
	a.core.Close()
}
