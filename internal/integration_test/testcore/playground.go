// Package testcore provides application cores and client stubs shared by the
// Monstera integration tests. It is a normal package (not _test) so it can be
// imported across test packages, but nothing in production depends on it.
package testcore

import (
	"encoding/binary"
	"encoding/gob"
	"io"
	"maps"

	"github.com/evrblk/monstera"
)

// PlaygroundCore is a simple in-memory key/value ApplicationCore. Keys are uint64,
// values are strings. It supports snapshot/restore so tests can exercise Raft
// snapshotting and read-after-write behavior.
type PlaygroundCore struct {
	state map[uint64]string
}

var _ monstera.ApplicationCore = &PlaygroundCore{}

func NewPlaygroundCore() *PlaygroundCore {
	return &PlaygroundCore{
		state: make(map[uint64]string),
	}
}

func (c *PlaygroundCore) Close() {}

func (c *PlaygroundCore) Restore(snapshot io.ReadCloser) error {
	c.state = make(map[uint64]string)

	dec := gob.NewDecoder(snapshot)
	if err := dec.Decode(&c.state); err != nil {
		return err
	}

	return nil
}

func (c *PlaygroundCore) Read(request []byte) (*monstera.ReadResponse, error) {
	r, ok := c.state[binary.BigEndian.Uint64(request)]
	if !ok {
		return &monstera.ReadResponse{
			Data: []byte{},
		}, nil
	}
	return &monstera.ReadResponse{
		Data: []byte(r),
	}, nil
}

func (c *PlaygroundCore) Update(request []byte) (*monstera.UpdateResponse, error) {
	key := binary.BigEndian.Uint64(request[:8])
	value := string(request[8:])
	c.state[key] = value
	return &monstera.UpdateResponse{
		Data: []byte(value),
	}, nil
}

func (c *PlaygroundCore) Snapshot() monstera.ApplicationCoreSnapshot {
	clone := make(map[uint64]string)
	maps.Copy(clone, c.state)

	return &PlaygroundCoreSnapshot{
		state: clone,
	}
}

type PlaygroundCoreSnapshot struct {
	state map[uint64]string
}

func (s *PlaygroundCoreSnapshot) Write(w io.Writer) error {
	enc := gob.NewEncoder(w)
	if err := enc.Encode(s.state); err != nil {
		return err
	}
	return nil
}

func (s *PlaygroundCoreSnapshot) Release() {}
