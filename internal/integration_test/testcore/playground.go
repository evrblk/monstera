// Package testcore provides application cores and client stubs shared by the
// Monstera integration tests. It is a normal package (not _test) so it can be
// imported across test packages, but nothing in production depends on it.
package testcore

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"maps"
	"sync"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/utils"
)

// PlaygroundCore is a simple in-memory key/value ApplicationCore. Keys are uint64,
// values are strings. It supports snapshot/restore so tests can exercise Raft
// snapshotting and read-after-write behavior.
//
// Its Restore honors the PORTABLE, bounds-filtered contract (see
// docs/snapshot-and-restore.md): the snapshot stream carries logical rows with
// no shard identity, and Restore keeps only the rows whose shard key falls
// within this core's bounds. A same-shard restore keeps everything; a restore
// from a splitting parent's snapshot keeps exactly this child's half.
type PlaygroundCore struct {
	// lowerBound/upperBound are this core's shard bounds (inclusive, 4 bytes).
	// Nil bounds mean the full keyspace.
	lowerBound []byte
	upperBound []byte

	// mu guards state: the core contract allows Read (and Snapshot) to run
	// concurrently with Update.
	mu    sync.RWMutex
	state map[uint64]string
}

var _ monstera.ApplicationCore = &PlaygroundCore{}

// NewPlaygroundCore creates a core owning the full keyspace (single-shard and
// unit-test convenience).
func NewPlaygroundCore() *PlaygroundCore {
	return NewBoundedPlaygroundCore(nil, nil)
}

// NewBoundedPlaygroundCore creates a core bound to [lowerBound, upperBound]
// of the shard keyspace, as passed by the core factory from the shard config.
func NewBoundedPlaygroundCore(lowerBound, upperBound []byte) *PlaygroundCore {
	return &PlaygroundCore{
		lowerBound: lowerBound,
		upperBound: upperBound,
		state:      make(map[uint64]string),
	}
}

func (c *PlaygroundCore) Close() {}

// ShardKeyOf computes the shard key of a logical key: the truncated hash the
// client stub routes by.
func ShardKeyOf(key uint64) []byte {
	return utils.GetTruncatedHash(utils.ConcatBytes(key), 4)
}

// ownsKey reports whether a logical key belongs to this core's bounds.
func (c *PlaygroundCore) ownsKey(key uint64) bool {
	if c.lowerBound == nil && c.upperBound == nil {
		return true
	}
	sk := ShardKeyOf(key)
	return bytes.Compare(sk, c.lowerBound) >= 0 && bytes.Compare(sk, c.upperBound) <= 0
}

func (c *PlaygroundCore) Restore(snapshots ...io.ReadCloser) error {
	// Replace semantics + bounds filter: the new state is the union of the
	// given streams (disjoint by contract), keeping only the rows this core
	// owns.
	filtered := make(map[uint64]string)
	for _, snapshot := range snapshots {
		decoded := make(map[uint64]string)
		if err := gob.NewDecoder(snapshot).Decode(&decoded); err != nil {
			return err
		}
		for k, v := range decoded {
			if c.ownsKey(k) {
				filtered[k] = v
			}
		}
	}

	c.mu.Lock()
	c.state = filtered
	c.mu.Unlock()

	return nil
}

func (c *PlaygroundCore) Read(request []byte) (*monstera.ReadResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

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
	c.mu.Lock()
	defer c.mu.Unlock()

	key := binary.BigEndian.Uint64(request[:8])
	value := string(request[8:])
	c.state[key] = value
	return &monstera.UpdateResponse{
		Data: []byte(value),
	}, nil
}

func (c *PlaygroundCore) Snapshot() monstera.ApplicationCoreSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()

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
