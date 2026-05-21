package raft

import (
	"fmt"
	"io"
	"log"

	hraft "github.com/hashicorp/raft"
)

// AppCore is the subset of ApplicationCore used by the FSM adapter.
type AppCore interface {
	Apply(request []byte) any
	Snapshot() AppCoreSnapshot
	Restore(reader io.ReadCloser) error
}

type AppCoreSnapshot interface {
	Write(w io.Writer) error
	Release()
}

// fsmAdapter bridges the hashicorp/raft FSM interface with an AppCore.
type fsmAdapter struct {
	core AppCore
}

var _ hraft.FSM = &fsmAdapter{}

// Apply is called once a log entry is committed by a majority of the cluster.
func (f *fsmAdapter) Apply(l *hraft.Log) any {
	return f.core.Apply(l.Data)
}

// Snapshot returns an FSMSnapshot used to support log compaction and follower
// catch-up. It must return quickly; expensive I/O belongs in Persist.
func (f *fsmAdapter) Snapshot() (hraft.FSMSnapshot, error) {
	return &fsmSnapshot{stream: f.core.Snapshot()}, nil
}

// Restore replaces the FSM state with the snapshot read from reader.
func (f *fsmAdapter) Restore(reader io.ReadCloser) error {
	err := f.core.Restore(reader)
	if err != nil {
		log.Printf("error restoring raft snapshot: %s", err)
	}
	return err
}

// fsmSnapshot adapts AppCoreSnapshot to hraft.FSMSnapshot
type fsmSnapshot struct {
	stream AppCoreSnapshot
}

var _ hraft.FSMSnapshot = &fsmSnapshot{}

func (s *fsmSnapshot) Persist(sink hraft.SnapshotSink) error {
	err := s.stream.Write(sink)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("sink.Write(): %v", err)
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {
	s.stream.Release()
}

// NewFSMAdapter creates a new Raft FSM that delegates to the given AppCore.
func NewFSMAdapter(core AppCore) *fsmAdapter {
	return &fsmAdapter{
		core: core,
	}
}
