package monstera

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"runtime/debug"
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
			// The core-log line for this was already written synchronously
			// by appCoreAdapter.Read/flushReadLog before it re-panicked
			// here with the same error — this recover only needs to shut
			// the replica down and surface a Go error to the caller.
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
func (r *replica) Update(request []byte, shardKey cluster.ShardKey, hasShardKey bool) (updateResponse *UpdateResponse, raftLogIndex uint64, err error) {
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
		return nil, 0, err
	}

	replicaCommandBytes.WithLabelValues(r.nodeId, r.applicationName, r.shardId, r.replicaId).Observe(float64(len(cmdBytes)))

	response, err := r.raft.Update(cmdBytes)
	if err != nil {
		return nil, 0, err
	}
	switch resp := response.(type) {
	case *appliedUpdateResult:
		// TODO emit events
		return resp.response, resp.index, nil
	case *splitRejection:
		// Committed after the shard froze: the write mutated nothing and the
		// caller must re-route it to the children.
		return nil, 0, errShardFrozen
	default:
		return nil, 0, fmt.Errorf("invalid response type %T", response)
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
	nodeId string, core ApplicationCore, trans transport.DataPlane, raftStore *store.BadgerStore, restoreSnapshotOnStart bool, updateTimeout time.Duration, snapshotSessionTimeout time.Duration,
	coreLogPolicy CoreLogPolicy, coreLogQueue *coreLogQueue, coreLogDest slog.Handler) *replica {
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
		coreLogPolicy:   coreLogPolicy,
		coreLogQueue:    coreLogQueue,
		coreLogDest:     coreLogDest,
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

	rep.raft = raft.NewRaft(baseDir, nodeId, applicationName, shardId, replicaId, adapter, trans, raftStore, restoreSnapshotOnStart, updateTimeout, snapshotSessionTimeout)

	// raft and replayFloor can only be set after raft.NewRaft returns (the
	// adapter is handed to it as the AppCore before the *raft.Raft it wraps
	// exists) — mirrors notes/monstera/events-design.md §5's identical
	// bootstrapping for the Events bus's replay gate. replayFloor is the
	// CommitIndex this replica already knew about before this process/attach
	// cycle began; every Apply at or below it is replay, not a new commit.
	adapter.raft = rep.raft
	adapter.replayFloor = rep.raft.GetRaftStats().CommitIndex

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

	// raft backs isLeader() — appCoreAdapter has no other way to know
	// current leadership. Set once, right after raft.NewRaft returns in
	// newReplica, for the same chicken-and-egg reason replayFloor is (see
	// its own comment there).
	raft *raft.Raft

	// replayFloor is the CommitIndex this replica's raft instance reported
	// at the moment it started. Apply's index <= replayFloor iff that entry
	// is being replayed (restart log-tail replay, follower/rejoin catch-up)
	// rather than a genuinely new live commit — see flushCoreLog.
	replayFloor uint64

	// coreLogPolicy and coreLogQueue implement the core diagnostic log's
	// success-path disposition (docs/core-implementation.md, "Logging from
	// a core"); coreLogDest is where the fatal path writes synchronously,
	// bypassing coreLogPolicy entirely — see flushCoreLog.
	coreLogPolicy CoreLogPolicy
	coreLogQueue  *coreLogQueue
	coreLogDest   slog.Handler
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

// appliedUpdateResult is the FSM apply result of a COMMAND_TYPE_UPDATE
// command: the ApplicationCore's response paired with the Raft log index it
// committed at. index is framework/Raft metadata that ApplicationCore never
// sees — it is attached here, one layer above the core, and surfaced to
// callers only via monstera.Client's response.
type appliedUpdateResult struct {
	response *UpdateResponse
	index    uint64
}

var _ raft.AppCore = (*appCoreAdapter)(nil)

func (a *appCoreAdapter) Read(request []byte) *ReadResponse {
	a.coreMu.RLock()
	defer a.coreMu.RUnlock()

	logger, h := newCoreLogger()
	resp, err := a.callCoreRead(request, logger)
	a.flushReadLog(h.lines, err)
	if err != nil {
		panic(err) // unchanged: internal errors still crash the node, on purpose
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
		logger, h := newCoreLogger()
		resp, err := a.callCoreUpdate(cmd.Payload, logger)
		a.flushCoreLog(index, h.lines, err)
		if err != nil {
			panic(err) // unchanged: internal errors still crash the node, on purpose
		}
		fsmApplyDuration.WithLabelValues(a.nodeId, a.applicationName, a.shardId, a.replicaId).Observe(time.Since(t1).Seconds())
		commitsTotal.WithLabelValues(a.nodeId, a.applicationName, a.shardId, a.replicaId).Inc()
		return &appliedUpdateResult{response: resp, index: index}
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

// callCoreUpdate converts a raw panic in a.core.Update into an error — the
// same pattern replica.Read already uses for its own panic recovery — and,
// in both failure shapes (a returned error or a recovered panic), logs the
// failure through the same logger the core was already writing to. That
// guarantees flushCoreLog always sees a record of the failure, whether or
// not the core itself logged anything before hitting it.
func (a *appCoreAdapter) callCoreUpdate(payload []byte, logger *slog.Logger) (resp *UpdateResponse, err error) {
	defer func() {
		if p := recover(); p != nil {
			logger.Error("panic in core.Update", "panic", p, "stack", string(debug.Stack()))
			err = fmt.Errorf("core.Update panicked: %v", p)
		}
	}()
	resp, err = a.core.Update(payload, logger)
	if err != nil {
		logger.Error("core.Update returned an error", "error", err)
	}
	return resp, err
}

// isLeader reports whether this replica's raft instance currently believes
// itself to be the leader. Used only to tag core-log entries — a diagnostic,
// best-effort label — unlike replica.IsLeader, nothing correctness-sensitive
// depends on it.
func (a *appCoreAdapter) isLeader() bool {
	return a.raft != nil && a.raft.GetRaftState() == raft.Leader
}

// flushCoreLog disposes of one Update call's buffered log lines. The
// success and fatal paths diverge only in disposition, not in mechanism —
// both eventually replay the same records into a real slog.Handler via
// writeCoreLogBatch, just synchronously here or later on the drain
// goroutine (see coreLogQueue).
func (a *appCoreAdapter) flushCoreLog(index uint64, records []slog.Record, err error) {
	if len(records) == 0 {
		return
	}

	replay := index <= a.replayFloor
	leader := a.isLeader()
	tags := []slog.Attr{
		slog.String("node_id", a.nodeId),
		slog.String("application_name", a.applicationName),
		slog.String("shard_id", a.shardId),
		slog.String("replica_id", a.replicaId),
		slog.Bool("is_leader", leader),
		slog.Bool("replay", replay),
		slog.Uint64("index", index),
		slog.Bool("fatal", err != nil),
	}

	if err != nil {
		// Fatal path: every CoreLogPolicy filter is skipped on purpose (see
		// CoreLogPolicy's doc comment) and the write happens synchronously,
		// right here, before Apply panics — the non-blocking queue's whole
		// justification (protect raft throughput) no longer applies once
		// the node is about to crash, and an async batch risks never being
		// drained before the process exits.
		writeCoreLogBatch(a.coreLogDest, coreLogBatch{tags: tags, records: records})
		return
	}

	if (!a.coreLogPolicy.IncludeReplay && replay) || (a.coreLogPolicy.LeaderOnly && !leader) {
		return
	}
	kept := filterByLevel(records, a.coreLogPolicy.MinLevel)
	if len(kept) == 0 {
		return
	}
	a.coreLogQueue.enqueueNonBlocking(tags, kept)
}

// callCoreRead is callCoreUpdate's exact counterpart for the Read side:
// converts a raw panic in a.core.Read into an error, and logs either
// failure shape through the same logger the core was already writing to.
func (a *appCoreAdapter) callCoreRead(request []byte, logger *slog.Logger) (resp *ReadResponse, err error) {
	defer func() {
		if p := recover(); p != nil {
			logger.Error("panic in core.Read", "panic", p, "stack", string(debug.Stack()))
			err = fmt.Errorf("core.Read panicked: %v", p)
		}
	}()
	resp, err = a.core.Read(request, logger)
	if err != nil {
		logger.Error("core.Read returned an error", "error", err)
	}
	return resp, err
}

// flushReadLog is flushCoreLog's counterpart for Read: same disposition
// shape (CoreLogPolicy on success, unfiltered synchronous write on
// failure), minus the replay dimension, which is simply moot here — Read
// is never part of raft log replay, so there's nothing to gate on.
// LeaderOnly still applies: a core can choose to only keep lines from
// leader-served reads, same as for Update, even though a follower serving
// a Read is itself perfectly valid (AllowReadFromFollowers).
func (a *appCoreAdapter) flushReadLog(records []slog.Record, err error) {
	if len(records) == 0 {
		return
	}

	leader := a.isLeader()
	tags := []slog.Attr{
		slog.String("node_id", a.nodeId),
		slog.String("application_name", a.applicationName),
		slog.String("shard_id", a.shardId),
		slog.String("replica_id", a.replicaId),
		slog.Bool("is_leader", leader),
		slog.Bool("fatal", err != nil),
	}

	if err != nil {
		writeCoreLogBatch(a.coreLogDest, coreLogBatch{tags: tags, records: records})
		return
	}

	if a.coreLogPolicy.LeaderOnly && !leader {
		return
	}
	kept := filterByLevel(records, a.coreLogPolicy.MinLevel)
	if len(kept) == 0 {
		return
	}
	a.coreLogQueue.enqueueNonBlocking(tags, kept)
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
