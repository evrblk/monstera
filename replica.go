package monstera

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/evrblk/monstera/cluster"
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

	// splitting is set by the node (from its applied config) while this
	// replica's shard is SPLITTING. A splitting leader stamps every proposed
	// update with its shard key so the split seeder can route entries to
	// children without decoding payloads.
	splitting atomic.Bool

	logger *log.Logger
}

// setSplitting marks this replica's shard as splitting (or not); toggled by
// the node on every config reconcile.
func (r *replica) setSplitting(v bool) {
	r.splitting.Store(v)
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

// Update proposes an application update through Raft. shardKey is the
// update's shard key (hasShardKey is false for shard-wide, unsharded updates);
// it is stamped into the replicated command's routing only while the shard is
// splitting, so seeded entries can be routed to children by key.
func (r *replica) Update(request []byte, shardKey cluster.ShardKey, hasShardKey bool) (updateResponse *UpdateResponse, err error) {
	t1 := time.Now()
	defer func() {
		result := "ok"
		if err != nil {
			result = "error"
		}
		replicaUpdateDuration.WithLabelValues(r.nodeId, r.applicationName, r.shardId, r.replicaId).Observe(time.Since(t1).Seconds())
		replicaUpdatesTotal.WithLabelValues(r.nodeId, r.applicationName, r.shardId, r.replicaId, result).Inc()
	}()

	cmd := &replicationpb.MonsteraCommand{
		Payload: request,
		Type:    replicationpb.CommandType_COMMAND_TYPE_UPDATE,
	}
	if r.splitting.Load() {
		if hasShardKey {
			cmd.Routing = replicationpb.CommandRouting_COMMAND_ROUTING_SHARDED
			cmd.ShardKey = uint32(shardKey)
		} else {
			cmd.Routing = replicationpb.CommandRouting_COMMAND_ROUTING_SHARD_WIDE
		}
	}

	cmdBytes, err := r.commandCodec.Encode(cmd)
	if err != nil {
		return nil, err
	}

	replicaCommandBytes.WithLabelValues(r.nodeId, r.applicationName, r.shardId, r.replicaId).Observe(float64(len(cmdBytes)))

	response, err := r.raft.Update(cmdBytes)
	if err != nil {
		return nil, err
	}
	switch resp := response.(type) {
	case *UpdateResponse:
		// TODO emit events
		return resp, nil
	case *splitRejection:
		// Committed after the shard froze: the write mutated nothing and the
		// caller must re-route it to the children.
		return nil, errShardFrozen
	default:
		return nil, fmt.Errorf("invalid response type %T", response)
	}
}

// frozenAt returns the log index this replica's shard was frozen at by a
// split CUTOFF, or 0 if it is live. Deterministic across replicas once the
// cutoff entry is applied; durable across restarts.
func (r *replica) frozenAt() uint64 {
	return r.core.frozen.Load()
}

// SplitCutoff proposes the split CUTOFF command through this replica (which
// must be the Raft leader). It returns the log index the shard froze at.
// Idempotent: re-proposing on an already-frozen shard returns the original
// cutoff index.
func (r *replica) SplitCutoff(childShardIds []string) (uint64, error) {
	payload, err := (&replicationpb.Cutoff{
		ParentShardId: r.shardId,
		ChildShardIds: childShardIds,
	}).MarshalVT()
	if err != nil {
		return 0, err
	}
	cmdBytes, err := r.commandCodec.Encode(&replicationpb.MonsteraCommand{
		Type:    replicationpb.CommandType_COMMAND_TYPE_CUTOFF,
		Payload: payload,
	})
	if err != nil {
		return 0, err
	}

	response, err := r.raft.Update(cmdBytes)
	if err != nil {
		return 0, err
	}
	result, ok := response.(*cutoffResult)
	if !ok {
		return 0, fmt.Errorf("invalid CUTOFF response type %T", response)
	}
	r.logger.Printf("Shard %s frozen by split cutoff at index %d", r.shardId, result.index)
	return result.index, nil
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

// TakeAndOpenSnapshot triggers (or reuses) a snapshot of this replica and
// opens it for reading. The base-snapshot source for split seeding.
func (r *replica) TakeAndOpenSnapshot() (raft.SnapshotMetadata, io.ReadCloser, error) {
	return r.raft.TakeAndOpenSnapshot()
}

// GetLogEntry reads one entry from this replica's log store; used by the
// split seeding tailer.
func (r *replica) GetLogEntry(index uint64) (raft.LogEntry, error) {
	return r.raft.GetLogEntry(index)
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

	// The cutoff marker must be readable and writable before the Raft instance
	// starts: a restarting replica may replay (or have already recorded) the
	// split CUTOFF, and the frozen state must be in force from the very first
	// apply.
	cutoffMarker := raft.NewCutoffMarker(raftStore, replicaId)
	cutoffIndex, err := cutoffMarker.Get()
	if err != nil {
		panic(fmt.Errorf("reading cutoff marker for replica %s: %w", replicaId, err))
	}

	adapter := &appCoreAdapter{
		core:            core,
		commandCodec:    commandCodec,
		cutoffMarker:    cutoffMarker,
		nodeId:          nodeId,
		applicationName: applicationName,
		shardId:         shardId,
		replicaId:       replicaId,
	}
	adapter.frozen.Store(cutoffIndex)

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

	// cutoffMarker persists the split freeze; frozen caches its value (the
	// cutoff log index, 0 = live). frozen is written on the Raft FSM thread
	// (applying CUTOFF) and read from request paths and the splitter.
	cutoffMarker *raft.CutoffMarker
	frozen       atomic.Uint64

	// nodeId, applicationName, shardId and replicaId identify this replica in
	// the apply/commit/snapshot metrics emitted at this boundary.
	nodeId          string
	applicationName string
	shardId         string
	replicaId       string
}

// cutoffResult is the FSM apply result of a CUTOFF command: the log index the
// shard froze at (the first CUTOFF's index, also for idempotent re-proposals).
type cutoffResult struct {
	index uint64
}

// splitRejection is the FSM apply result of any update committed after the
// shard froze: the core is not touched and the caller must re-route to the
// children.
type splitRejection struct{}

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

func (a *appCoreAdapter) Apply(index uint64, request []byte) any {
	t1 := time.Now()

	cmd, err := a.commandCodec.Decode(request)
	if err != nil {
		panic(err)
	}

	// A frozen shard applies nothing to the core anymore: updates committed
	// after the cutoff are rejected deterministically on every replica (the
	// node re-routes them to the children), and a repeated CUTOFF returns the
	// original cutoff index (idempotent).
	if frozenAt := a.frozen.Load(); frozenAt > 0 {
		switch cmd.Type {
		case replicationpb.CommandType_COMMAND_TYPE_CUTOFF:
			return &cutoffResult{index: frozenAt}
		case replicationpb.CommandType_COMMAND_TYPE_NOOP:
			return &UpdateResponse{}
		default:
			return &splitRejection{}
		}
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
	case replicationpb.CommandType_COMMAND_TYPE_NOOP:
		// Index-contiguity filler in seeded child logs: applied without
		// touching the core.
		return &UpdateResponse{}
	case replicationpb.CommandType_COMMAND_TYPE_CUTOFF:
		var cutoff replicationpb.Cutoff
		if err := cutoff.UnmarshalVT(cmd.Payload); err != nil {
			panic(fmt.Sprintf("decoding CUTOFF payload: %v", err))
		}
		if cutoff.ParentShardId != a.shardId {
			panic(fmt.Sprintf("CUTOFF for shard %s applied to shard %s", cutoff.ParentShardId, a.shardId))
		}
		// Persist the freeze first: the marker (not log replay) is what makes
		// the freeze survive restarts for every core type.
		if err := a.cutoffMarker.Set(index); err != nil {
			panic(fmt.Sprintf("persisting cutoff marker: %v", err))
		}
		a.frozen.Store(index)
		return &cutoffResult{index: index}
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
