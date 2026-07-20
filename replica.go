package monstera

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/evrblk/monstera/internal/raft"
	"github.com/evrblk/monstera/internal/replication"
	"github.com/evrblk/monstera/internal/replication/replicationpb"
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/transport"
)

// replica manages a single Raft replica for one shard.
type replica struct {
	nodeId          string
	applicationName string
	shardId         string
	replicaId       string

	core         *appCoreAdapter
	raft         *raft.Raft
	commandCodec replication.CommandCodec

	logger *log.Logger
}

func (r *replica) Read(request []byte) (response *ReadResponse, err error) {
	// Registered first so it runs last (LIFO): it observes the final err,
	// including one synthesized from a recovered panic by the defer below.
	t1 := time.Now()
	defer func() {
		result := "ok"
		if err != nil {
			result = "error"
		}
		replicaReadDuration.WithLabelValues(r.nodeId, r.applicationName, r.shardId, r.replicaId).Observe(time.Since(t1).Seconds())
		replicaReadsTotal.WithLabelValues(r.nodeId, r.applicationName, r.shardId, r.replicaId, result).Inc()
	}()

	defer func() {
		if p := recover(); p != nil {
			r.logger.Printf("panic in core.Read, shutting down raft: %v", p)
			r.raft.Close()
			err = fmt.Errorf("core.Read panicked: %v", p)
		}
	}()

	return r.core.Read(request), nil
}

func (r *replica) Update(request []byte) (updateResponse *UpdateResponse, err error) {
	t1 := time.Now()
	defer func() {
		result := "ok"
		if err != nil {
			result = "error"
		}
		replicaUpdateDuration.WithLabelValues(r.nodeId, r.applicationName, r.shardId, r.replicaId).Observe(time.Since(t1).Seconds())
		replicaUpdatesTotal.WithLabelValues(r.nodeId, r.applicationName, r.shardId, r.replicaId, result).Inc()
	}()

	cmdBytes, err := r.commandCodec.Encode(&replicationpb.MonsteraCommand{
		Payload: request,
		Type:    replicationpb.CommandType_COMMAND_TYPE_UPDATE,
	})
	if err != nil {
		return nil, err
	}

	replicaCommandBytes.WithLabelValues(r.nodeId, r.applicationName, r.shardId, r.replicaId).Observe(float64(len(cmdBytes)))

	response, err := r.raft.Update(cmdBytes)
	if err != nil {
		return nil, err
	}
	updateResponse, ok := response.(*UpdateResponse)
	if !ok {
		return nil, fmt.Errorf("invalid response type %v", updateResponse)
	}

	// TODO emit events

	return updateResponse, nil
}

func (r *replica) Close() {
	// Close the Raft node
	r.raft.Close()

	// Close the application core
	r.core.Close()
}

func (r *replica) GetRaftStats() raft.RaftStats {
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

func (r *replica) Bootstrap(servers []raft.RaftServer) error {
	return r.raft.Bootstrap(servers)
}

func (r *replica) AddVoter(replicaId string, nodeId string) error {
	return r.raft.AddVoter(replicaId, nodeId)
}

func (r *replica) RemoveServer(replicaId string) error {
	return r.raft.RemoveServer(replicaId)
}

func (r *replica) GetConfiguration() ([]raft.RaftServer, error) {
	return r.raft.GetConfiguration()
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

func (r *replica) ListSnapshots() ([]raft.SnapshotMetadata, error) {
	return r.raft.ListSnapshots()
}

func (r *replica) LeadershipTransfer() error {
	return r.raft.LeadershipTransfer()
}

func (r *replica) GetReplicaId() string {
	return r.replicaId
}

// newReplica creates a replica hosted on the node identified by nodeId. The
// node id doubles as this replica's Raft transport address and labels its
// metrics.
func newReplica(baseDir string, applicationName string, shardId string, replicaId string,
	nodeId string, core ApplicationCore, trans transport.DataPlane, raftStore *store.BadgerStore, restoreSnapshotOnStart bool, updateTimeout time.Duration) *replica {
	commandCodec := &replication.ProtoCommandCodec{}
	adapter := &appCoreAdapter{
		core:            core,
		commandCodec:    commandCodec,
		nodeId:          nodeId,
		applicationName: applicationName,
		shardId:         shardId,
		replicaId:       replicaId,
	}

	rep := &replica{
		nodeId:          nodeId,
		applicationName: applicationName,
		shardId:         shardId,
		replicaId:       replicaId,
		core:            adapter,
		commandCodec:    commandCodec,
		logger:          log.New(os.Stderr, fmt.Sprintf("[%s]", replicaId), log.LstdFlags),
	}

	rep.raft = raft.NewRaft(baseDir, nodeId, applicationName, shardId, replicaId, adapter, trans, raftStore, restoreSnapshotOnStart, updateTimeout)

	return rep
}

type appCoreAdapter struct {
	// coreMu protects core from concurrent reads during snapshot restoration.
	// Read acquires RLock; Restore acquires Lock.
	coreMu       sync.RWMutex
	core         ApplicationCore
	commandCodec replication.CommandCodec

	// nodeId, applicationName, shardId and replicaId identify this replica in
	// the apply/commit/snapshot metrics emitted at this boundary.
	nodeId          string
	applicationName string
	shardId         string
	replicaId       string
}

var _ raft.AppCore = (*appCoreAdapter)(nil)

func (a *appCoreAdapter) Read(request []byte) *ReadResponse {
	a.coreMu.RLock()
	defer a.coreMu.RUnlock()

	resp, err := a.core.Read(request)
	if err != nil {
		panic(err)
	}
	return &ReadResponse{
		Data: resp.Data,
	}
}

func (a *appCoreAdapter) Apply(request []byte) any {
	t1 := time.Now()

	cmd, err := a.commandCodec.Decode(request)
	if err != nil {
		panic(err)
	}

	switch cmd.Type {
	case replicationpb.CommandType_COMMAND_TYPE_UPDATE:
		resp, err := a.core.Update(cmd.Payload)
		if err != nil {
			panic(err)
		}
		fsmApplyDuration.WithLabelValues(a.nodeId, a.applicationName, a.shardId, a.replicaId).Observe(time.Since(t1).Seconds())
		commitsTotal.WithLabelValues(a.nodeId, a.applicationName, a.shardId, a.replicaId).Inc()
		return resp
	default:
		panic(fmt.Sprintf("unknown command type: %v", cmd.Type))
	}
}

func (a *appCoreAdapter) Snapshot() raft.AppCoreSnapshot {
	return &instrumentedSnapshot{
		inner:           a.core.Snapshot(),
		nodeId:          a.nodeId,
		applicationName: a.applicationName,
		shardId:         a.shardId,
		replicaId:       a.replicaId,
	}
}

func (a *appCoreAdapter) Restore(reader io.ReadCloser) error {
	a.coreMu.Lock()
	defer a.coreMu.Unlock()

	t1 := time.Now()
	cr := &countingReadCloser{r: reader}
	err := a.core.Restore(cr)
	raft.RecordSnapshot(a.nodeId, a.applicationName, a.shardId, a.replicaId, "restore", time.Since(t1), cr.n, err)
	return err
}

func (a *appCoreAdapter) Close() {
	a.core.Close()
}

// instrumentedSnapshot wraps an ApplicationCoreSnapshot to measure snapshot
// persist duration and size. The actual Write is driven later by the Raft
// snapshotting machinery, so timing happens here rather than at Snapshot().
type instrumentedSnapshot struct {
	inner ApplicationCoreSnapshot

	nodeId          string
	applicationName string
	shardId         string
	replicaId       string
}

var _ raft.AppCoreSnapshot = (*instrumentedSnapshot)(nil)

func (s *instrumentedSnapshot) Write(w io.Writer) error {
	t1 := time.Now()
	cw := &countingWriter{w: w}
	err := s.inner.Write(cw)
	raft.RecordSnapshot(s.nodeId, s.applicationName, s.shardId, s.replicaId, "persist", time.Since(t1), cw.n, err)
	return err
}

func (s *instrumentedSnapshot) Release() {
	s.inner.Release()
}
