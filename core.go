package monstera

import (
	"fmt"
	"io"

	"github.com/evrblk/monstera/cluster"
)

// ApplicationCore is the interface that must be implemented by clients to be
// used with the Monstera framework.
type ApplicationCore interface {
	// Read is used to read a value directly from the application core.
	// Reads can be performed concurrently with updates, other reads,
	// and snapshots. Read must return internal errors, but all application
	// errors should be returned as part of the ReadResponse.
	Read(req []byte) (*ReadResponse, error)

	// Update is used to update the application core state.
	// All updates are applied to the application core sequentially,
	// in the order they are committed to the Raft log. This method is called
	// by the Raft thread. Update must return internal errors, but all application
	// errors should be returned as part of the UpdateResponse.
	Update(req []byte) (*UpdateResponse, error)

	// Snapshot returns an ApplicationCoreSnapshot used to support Raft log
	// compaction, state restoration, and follower catch-up.
	//
	// Snapshot must return quickly. Expensive I/O belongs in
	// ApplicationCoreSnapshot.Write. Update and Snapshot are always called
	// from the same thread, but Update will be called concurrently with
	// ApplicationCoreSnapshot.Write.
	Snapshot() ApplicationCoreSnapshot

	// Restore replaces the application core state with the data from the
	// given snapshot streams that belongs to this core's shard bounds —
	// "replace with the union of these streams". Callers pass one stream
	// (Raft restore on start, follower snapshot install, split seeding) or
	// two (merge seeding — one per merging parent). Streams are disjoint
	// after bounds filtering: the caller guarantees the producing shards'
	// ranges do not overlap, so no logical row appears in more than one
	// stream and stream order is irrelevant. It is not called concurrently
	// with any other command.
	Restore(readers ...io.ReadCloser) error

	// Close cleans up resources used by the application core. Do not clean up
	// resources shared by multiple cores. Close is called after a shard split
	// or move, and for each core after the Monstera node shuts down.
	Close()
}

// ApplicationCoreSnapshot is returned by ApplicationCore.Snapshot and is
// written to persistent storage by the Raft snapshotting machinery.
type ApplicationCoreSnapshot interface {
	// Write should dump all necessary state to the Writer.
	Write(w io.Writer) error

	// Release is invoked when we are finished with the snapshot.
	Release()
}

// ReadResponse is the response returned by ApplicationCore.Read.
type ReadResponse struct {
	// Data is the marshaled form of the response that the application core
	// returns as the result of a read.
	Data []byte
}

// UpdateResponse is the response returned by ApplicationCore.Update.
type UpdateResponse struct {
	// Data is the marshaled form of the response that the application core
	// produces as the result of an update.
	Data []byte

	// Events are emitted by the application core after an update is applied.
	// Can be zero or more events. The events are related to the update and are
	// used to notify subscribers of the changes that happened as a result of
	// the update.
	Events []Event
}

// Event is an event that is emitted by the application core after an update is applied.
type Event struct {
	// Data is the marshaled form of the event data.
	Data []byte

	// Topic that the event is published on.
	Topic string
}

// ApplicationCoreDescriptors map is used to register application cores with Monstera.
// Key: the name of the application core, it should match Application.Implementation in ClusterConfig.
// Value: application core descriptor.
type ApplicationCoreDescriptors = map[string]ApplicationCoreDescriptor

// CoreType declares the storage model of an application core. The framework
// derives all storage-dependent behavior from it: whether the latest Raft
// snapshot is restored into the core on start, and how children of a
// splitting shard are seeded. The zero value is invalid: the type must be
// declared explicitly.
type CoreType int

const (
	// CoreTypeInMemory: core state lives only in RAM; the Raft log and
	// snapshots are its durability. On start, the latest snapshot is restored
	// (via ApplicationCore.Restore) and the log tail is replayed. Split
	// seeding copies the parent snapshot as the child's base plus the routed
	// log tail; activation replays it through the bounds-filtered Restore.
	CoreTypeInMemory CoreType = iota + 1

	// CoreTypePersistedShared: core state is durable, keyed by shard-key
	// range only, in a store shared across cores — cores with overlapping
	// bounds alias the same physical rows. No restore on start. Split
	// seeding is nothing at all: the children's rows are the parent's live
	// rows, and the split is just the cutoff.
	CoreTypePersistedShared

	// CoreTypePersistedExclusive: core state is durable and every row lives
	// under a shard-unique prefix — no row is readable or writable by more
	// than one core (the physical store may still be shared per node). No
	// restore on start. Split seeding restores the parent snapshot into the
	// child core plus a live routed Update tail until the cutoff.
	CoreTypePersistedExclusive
)

// RestoreSnapshotOnStart reports whether cores of this type restore their
// state from the latest Raft snapshot on start (true only for in-memory
// cores, whose snapshots+log are the durable state).
func (t CoreType) RestoreSnapshotOnStart() bool {
	return t == CoreTypeInMemory
}

func (t CoreType) String() string {
	switch t {
	case CoreTypeInMemory:
		return "InMemory"
	case CoreTypePersistedShared:
		return "PersistedShared"
	case CoreTypePersistedExclusive:
		return "PersistedExclusive"
	default:
		return fmt.Sprintf("CoreType(%d)", int(t))
	}
}

// ApplicationCoreDescriptor is used to register an application core with Monstera.
type ApplicationCoreDescriptor struct {
	// CoreFactoryFunc is a function that creates a new application core. It is called when
	// Monstera node starts for every replica on this node, and also for every new replica that
	// is added to the node while it is running.
	CoreFactoryFunc func(shard *cluster.Shard, replica *cluster.Replica) ApplicationCore

	// CoreType declares the storage model of this application's cores (see
	// the CoreType constants). Everything storage-dependent — restore on
	// start, shard-split seeding mechanism — is derived from it. Required;
	// the zero value is rejected at node start.
	CoreType CoreType
}
