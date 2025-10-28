package monstera

import (
	"fmt"
	"io"
	"log"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

// raftFSMAdapter is an adapter between the hashicorp/raft.FSM interface
// and a Monstera application core.
type raftFSMAdapter struct {
	core ApplicationCore
}

var _ hraft.FSM = &raftFSMAdapter{}

// Apply is called once a log entry is committed by a majority of the cluster.
//
// Apply should apply the log to the FSM. Apply must be deterministic and
// produce the same result on all peers in the cluster.
//
// The returned value is returned to the client as the ApplyFuture.Response.
func (f *raftFSMAdapter) Apply(l *hraft.Log) interface{} {
	command := &MonsteraCommand{}
	if err := proto.Unmarshal(l.Data, command); err != nil {
		panic(err)
	}

	return f.core.Update(command.Payload)
}

// Snapshot returns an FSMSnapshot used to: support log compaction, to
// restore the FSM to a previous state, or to bring out-of-date followers up
// to a recent log index.
//
// The Snapshot implementation should return quickly, because Apply can not
// be called while Snapshot is running. Generally this means Snapshot should
// only capture a pointer to the state, and any expensive IO should happen
// as part of FSMSnapshot.Persist.
//
// Apply and Snapshot are always called from the same thread, but Apply will
// be called concurrently with FSMSnapshot.Persist. This means the FSM should
// be implemented to allow for concurrent updates while a snapshot is happening.
//
// Clients of this library should make no assumptions about whether a returned
// Snapshot() will actually be stored by Raft. In fact, it's quite possible that
// any Snapshot returned by this call will be discarded, and that
// FSMSnapshot.Persist will never be called. However, Raft will always call
// FSMSnapshot.Release.
func (f *raftFSMAdapter) Snapshot() (hraft.FSMSnapshot, error) {
	return &snapshot{
		stream: f.core.Snapshot(),
	}, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state before restoring the snapshot.
func (f *raftFSMAdapter) Restore(reader io.ReadCloser) error {
	err := f.core.Restore(reader)
	if err != nil {
		log.Printf("error restoring raft snapshot: %s", err)
	}
	return err
}

// snapshot is an adapter between the hashicorp/raft.FSMSnapshot interface
// and a Monstera application core snapshot.
type snapshot struct {
	stream ApplicationCoreSnapshot
}

var _ hraft.FSMSnapshot = &snapshot{}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (s *snapshot) Persist(sink hraft.SnapshotSink) error {
	err := s.stream.Write(sink)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("sink.Write(): %v", err)
	}
	return sink.Close()
}

// Release is invoked when we are finished with the snapshot.
func (s *snapshot) Release() {
	s.stream.Release()
}

func newRaftFSMAdapter(core ApplicationCore) hraft.FSM {
	return &raftFSMAdapter{
		core: core,
	}
}
