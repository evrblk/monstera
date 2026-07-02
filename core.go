package monstera

import (
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

	// Restore replaces the application core state with the snapshot read from
	// reader. It is not called concurrently with any other command.
	Restore(reader io.ReadCloser) error

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

// ApplicationCoreDescriptor is used to register an application core with Monstera.
type ApplicationCoreDescriptor struct {
	// CoreFactoryFunc is a function that creates a new application core. It is called when
	// Monstera node starts for every replica on this node, and also for every new replica that
	// is added to the node while it is running.
	CoreFactoryFunc func(shard *cluster.Shard, replica *cluster.Replica) ApplicationCore

	// RestoreSnapshotOnStart is a flag that indicates if the application core should restore its
	// state from a snapshot on start (via ApplicationCore.Restore). For fully in-memory applications,
	// this flag should be true. For applications that are backed by an on-disk embedded storage this
	// might or might not be necessary, depending on implementation.
	RestoreSnapshotOnStart bool
}
