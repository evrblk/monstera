package raft

import (
	"fmt"
	"io"
	"log"

	hraft "github.com/hashicorp/raft"
)

// AppCore is the subset of ApplicationCore used by the FSM adapter.
// The main package's ApplicationCore satisfies this interface.
type AppCore interface {
	Update(request []byte) []byte
	Snapshot() AppCoreSnapshot
	Restore(reader io.ReadCloser) error
}

// AppCoreSnapshot is the subset of ApplicationCoreSnapshot used by the FSM adapter.
type AppCoreSnapshot interface {
	Write(w io.Writer) error
	Release()
}

// FsmAdapter bridges the hashicorp/raft FSM interface with an AppCore and a
// CommandCodec. It is intentionally unexported; callers use NewFSMAdapter.
type FsmAdapter struct {
	core         AppCore
	commandCodec CommandCodec
}

var _ hraft.FSM = &FsmAdapter{}

// Apply is called once a log entry is committed by a majority of the cluster.
func (f *FsmAdapter) Apply(l *hraft.Log) any {
	payload, err := f.commandCodec.Decode(l.Data)
	if err != nil {
		panic(fmt.Sprintf("CommandCodec.Decode: %v", err))
	}
	return f.core.Update(payload)
}

// Snapshot returns an FSMSnapshot used to support log compaction and follower
// catch-up. It must return quickly; expensive I/O belongs in Persist.
func (f *FsmAdapter) Snapshot() (hraft.FSMSnapshot, error) {
	return &FsmSnapshot{stream: f.core.Snapshot()}, nil
}

// Restore replaces the FSM state with the snapshot read from reader.
func (f *FsmAdapter) Restore(reader io.ReadCloser) error {
	err := f.core.Restore(reader)
	if err != nil {
		log.Printf("error restoring raft snapshot: %s", err)
	}
	return err
}

// FsmSnapshot adapts AppCoreSnapshot to hraft.FSMSnapshot
type FsmSnapshot struct {
	stream AppCoreSnapshot
}

var _ hraft.FSMSnapshot = &FsmSnapshot{}

func (s *FsmSnapshot) Persist(sink hraft.SnapshotSink) error {
	err := s.stream.Write(sink)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("sink.Write(): %v", err)
	}
	return sink.Close()
}

func (s *FsmSnapshot) Release() {
	s.stream.Release()
}

// NewFSMAdapter creates a new Raft FSM that delegates to the given AppCore,
// using codec to decode committed log entries into application payloads.
func NewFSMAdapter(core AppCore, commandCodec CommandCodec) *FsmAdapter {
	return &FsmAdapter{
		core:         core,
		commandCodec: commandCodec,
	}
}
