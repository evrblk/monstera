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
	// and snapshots. Read must panic on internal errors.
	Read(request []byte) []byte

	// Update is used to update the application core state.
	// All updates are applied to the application core sequentially,
	// in the order they are committed to the Raft log. This method is called
	// by the Raft thread. Update must panic on internal errors.
	Update(request []byte) []byte

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
