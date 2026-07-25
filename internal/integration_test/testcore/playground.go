// Package testcore provides application cores and client stubs shared by the
// Monstera integration tests. It is a normal package (not _test) so it can be
// imported across test packages, but nothing in production depends on it.
//
// The playground core is a uint64 -> string key/value store with one flavor
// per CoreType, all speaking the same request encoding (stub.go) and the same
// snapshot stream (a gob-encoded map), so the same tests and assertions run
// against any of them:
//
//   - InMemoryPlaygroundCore (CoreTypeInMemory): state in a RAM map.
//   - SharedPlaygroundCore (CoreTypePersistedShared): rows in a Badger store
//     shared by all cores, keyed by shard-key material only.
//   - ExclusivePlaygroundCore (CoreTypePersistedExclusive): rows in a Badger
//     store under a shard-unique prefix.
package testcore

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"io"
	"maps"
	"slices"
	"sync"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
)

// InMemoryPlaygroundCore is the CoreTypeInMemory playground flavor: state
// lives only in a RAM map, so the Raft log and snapshots are its durability.
//
// Its Restore honors the portable, bounds-filtered snapshot contract: the
// snapshot stream carries logical rows with no shard identity, and Restore
// keeps only the rows whose shard key falls within this core's bounds. A
// same-shard restore keeps everything; a restore from a splitting parent's
// snapshot keeps exactly this child's half.
type InMemoryPlaygroundCore struct {
	// lowerBound/upperBound are this core's shard bounds (inclusive, 4 bytes).
	// Nil bounds mean the full keyspace.
	lowerBound []byte
	upperBound []byte

	// mu guards state: the core contract allows Read (and Snapshot) to run
	// concurrently with Update.
	mu    sync.RWMutex
	state map[uint64]string
}

var _ monstera.ApplicationCore = &InMemoryPlaygroundCore{}

// NewInMemoryPlaygroundCore creates a core owning the full keyspace
// (single-shard and unit-test convenience).
func NewInMemoryPlaygroundCore() *InMemoryPlaygroundCore {
	return NewBoundedInMemoryPlaygroundCore(nil, nil)
}

// NewBoundedInMemoryPlaygroundCore creates a core bound to
// [lowerBound, upperBound] of the shard keyspace, as passed by the core
// factory from the shard config.
func NewBoundedInMemoryPlaygroundCore(lowerBound, upperBound []byte) *InMemoryPlaygroundCore {
	return &InMemoryPlaygroundCore{
		lowerBound: lowerBound,
		upperBound: upperBound,
		state:      make(map[uint64]string),
	}
}

func (c *InMemoryPlaygroundCore) Close() {}

// ShardKeyOf computes the shard key of a logical key: the truncated hash the
// client stub routes by.
func ShardKeyOf(key uint64) []byte {
	return utils.GetTruncatedHash(utils.ConcatBytes(key), 4)
}

// ownsPlaygroundKey reports whether a logical key belongs to the given shard
// bounds. Nil bounds mean the full keyspace.
func ownsPlaygroundKey(lowerBound, upperBound []byte, key uint64) bool {
	if lowerBound == nil && upperBound == nil {
		return true
	}
	sk := ShardKeyOf(key)
	return bytes.Compare(sk, lowerBound) >= 0 && bytes.Compare(sk, upperBound) <= 0
}

// parsePlaygroundKey decodes a read payload (see stub.go).
func parsePlaygroundKey(request []byte) uint64 {
	return binary.BigEndian.Uint64(request[:8])
}

// parsePlaygroundUpdate decodes an update payload (see stub.go).
func parsePlaygroundUpdate(request []byte) (uint64, string) {
	return binary.BigEndian.Uint64(request[:8]), string(request[8:])
}

// decodePlaygroundStreams decodes the union of the given snapshot streams,
// keeping only the rows the given bounds own (the portable, bounds-filtered
// Restore contract shared by every playground flavor).
func decodePlaygroundStreams(lowerBound, upperBound []byte, snapshots []io.ReadCloser) (map[uint64]string, error) {
	filtered := make(map[uint64]string)
	for _, snapshot := range snapshots {
		decoded := make(map[uint64]string)
		if err := gob.NewDecoder(snapshot).Decode(&decoded); err != nil {
			return nil, err
		}
		for k, v := range decoded {
			if ownsPlaygroundKey(lowerBound, upperBound, k) {
				filtered[k] = v
			}
		}
	}
	return filtered, nil
}

func (c *InMemoryPlaygroundCore) Restore(snapshots ...io.ReadCloser) error {
	// Replace semantics + bounds filter: the new state is the union of the
	// given streams (disjoint by contract), keeping only the rows this core
	// owns.
	filtered, err := decodePlaygroundStreams(c.lowerBound, c.upperBound, snapshots)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.state = filtered
	c.mu.Unlock()

	return nil
}

func (c *InMemoryPlaygroundCore) Read(request []byte) (*monstera.ReadResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	r, ok := c.state[parsePlaygroundKey(request)]
	if !ok {
		return &monstera.ReadResponse{
			Data: []byte{},
		}, nil
	}
	return &monstera.ReadResponse{
		Data: []byte(r),
	}, nil
}

func (c *InMemoryPlaygroundCore) Update(request []byte) (*monstera.UpdateResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key, value := parsePlaygroundUpdate(request)
	c.state[key] = value
	return &monstera.UpdateResponse{
		Data: []byte(value),
	}, nil
}

func (c *InMemoryPlaygroundCore) Snapshot() monstera.ApplicationCoreSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()

	clone := make(map[uint64]string)
	maps.Copy(clone, c.state)

	return &playgroundSnapshot{
		state: clone,
	}
}

// playgroundSnapshot is a snapshot over an already-materialized state map.
type playgroundSnapshot struct {
	state map[uint64]string
}

func (s *playgroundSnapshot) Write(w io.Writer) error {
	enc := gob.NewEncoder(w)
	if err := enc.Encode(s.state); err != nil {
		return err
	}
	return nil
}

func (s *playgroundSnapshot) Release() {}

// Effective full-keyspace bounds for the range-keyed store scans (persisted
// cores need concrete bounds where the in-memory core can use nil).
var (
	keyspaceLower = []byte{0x00, 0x00, 0x00, 0x00}
	keyspaceUpper = []byte{0xff, 0xff, 0xff, 0xff}
)

// SharedPlaygroundCore is the CoreTypePersistedShared playground flavor:
// every row lives in one Badger store shared by all cores of the application,
// keyed by shard-key material only (4-byte shard key + 8-byte logical key),
// so cores with overlapping bounds alias the same physical rows. A split
// child constructed with the child's bounds reads the rows its splitting
// parent wrote — the split needs no seeding at all.
//
// The store is owned by the caller (one per node, shared across replicas);
// Close does not close it.
type SharedPlaygroundCore struct {
	store      *store.BadgerStore
	lowerBound []byte
	upperBound []byte
}

var _ monstera.ApplicationCore = &SharedPlaygroundCore{}

// NewSharedPlaygroundCore creates a shared-store core bound to
// [lowerBound, upperBound] (nil bounds mean the full keyspace).
func NewSharedPlaygroundCore(s *store.BadgerStore, lowerBound, upperBound []byte) *SharedPlaygroundCore {
	if lowerBound == nil {
		lowerBound = keyspaceLower
	}
	if upperBound == nil {
		upperBound = keyspaceUpper
	}
	return &SharedPlaygroundCore{store: s, lowerBound: lowerBound, upperBound: upperBound}
}

// sharedRowKey is the range-keyed physical location of a logical key: shard
// key first, so a core's rows are exactly the store range of its bounds and
// carry no shard identity.
func sharedRowKey(key uint64) []byte {
	row := make([]byte, 4+8)
	copy(row[:4], ShardKeyOf(key))
	binary.BigEndian.PutUint64(row[4:], key)
	return row
}

func (c *SharedPlaygroundCore) Close() {}

func (c *SharedPlaygroundCore) Read(request []byte) (*monstera.ReadResponse, error) {
	txn := c.store.View()
	defer txn.Discard()

	v, err := txn.Get(sharedRowKey(parsePlaygroundKey(request)))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &monstera.ReadResponse{Data: []byte{}}, nil
		}
		return nil, err
	}
	return &monstera.ReadResponse{Data: v}, nil
}

func (c *SharedPlaygroundCore) Update(request []byte) (*monstera.UpdateResponse, error) {
	key, value := parsePlaygroundUpdate(request)
	err := c.store.BatchUpdate(func(b *store.Batch) error {
		return b.Set(sharedRowKey(key), []byte(value))
	})
	if err != nil {
		return nil, err
	}
	return &monstera.UpdateResponse{Data: []byte(value)}, nil
}

func (c *SharedPlaygroundCore) Snapshot() monstera.ApplicationCoreSnapshot {
	// The transaction pins a consistent view; Write scans it later while
	// updates continue.
	return &txnPlaygroundSnapshot{txn: c.store.View(), scan: c.scanState}
}

func (c *SharedPlaygroundCore) scanState(txn *store.Txn) (map[uint64]string, error) {
	state := make(map[uint64]string)
	err := txn.EachRange(c.lowerBound, c.upperBound, false, func(k, v []byte) (bool, error) {
		state[binary.BigEndian.Uint64(k[4:])] = string(v)
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return state, nil
}

func (c *SharedPlaygroundCore) Restore(snapshots ...io.ReadCloser) error {
	state, err := decodePlaygroundStreams(c.lowerBound, c.upperBound, snapshots)
	if err != nil {
		return err
	}

	// Replace semantics over exactly this core's bounds: clearing a wider
	// range would delete rows that are other cores' live data.
	var stale [][]byte
	txn := c.store.View()
	err = txn.EachRange(c.lowerBound, c.upperBound, false, func(k, _ []byte) (bool, error) {
		stale = append(stale, slices.Clone(k))
		return true, nil
	})
	txn.Discard()
	if err != nil {
		return err
	}

	return c.store.BatchUpdate(func(b *store.Batch) error {
		for _, k := range stale {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		for k, v := range state {
			if err := b.Set(sharedRowKey(k), []byte(v)); err != nil {
				return err
			}
		}
		return nil
	})
}

// ExclusivePlaygroundCore is the CoreTypePersistedExclusive playground
// flavor: every row lives in a Badger store (shared per node) under a
// shard-unique prefix, so no row is readable or writable by more than one
// core. A split child starts empty and is seeded by the splitter: a
// bounds-filtered Restore of the parent's snapshot plus a live Update tail.
//
// The store is owned by the caller (one per node, shared across replicas);
// Close does not close it.
type ExclusivePlaygroundCore struct {
	store      *store.BadgerStore
	prefix     []byte
	lowerBound []byte
	upperBound []byte
}

var _ monstera.ApplicationCore = &ExclusivePlaygroundCore{}

// NewExclusivePlaygroundCore creates an exclusive-store core whose rows live
// under a prefix derived from the shard id (nil bounds mean the full
// keyspace).
func NewExclusivePlaygroundCore(s *store.BadgerStore, shardId string, lowerBound, upperBound []byte) *ExclusivePlaygroundCore {
	return &ExclusivePlaygroundCore{
		store: s,
		// The trailing separator keeps one shard id from prefixing another's.
		prefix:     []byte("x/" + shardId + "/"),
		lowerBound: lowerBound,
		upperBound: upperBound,
	}
}

func (c *ExclusivePlaygroundCore) rowKey(key uint64) []byte {
	row := make([]byte, len(c.prefix)+8)
	copy(row, c.prefix)
	binary.BigEndian.PutUint64(row[len(c.prefix):], key)
	return row
}

func (c *ExclusivePlaygroundCore) Close() {}

func (c *ExclusivePlaygroundCore) Read(request []byte) (*monstera.ReadResponse, error) {
	txn := c.store.View()
	defer txn.Discard()

	v, err := txn.Get(c.rowKey(parsePlaygroundKey(request)))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &monstera.ReadResponse{Data: []byte{}}, nil
		}
		return nil, err
	}
	return &monstera.ReadResponse{Data: v}, nil
}

func (c *ExclusivePlaygroundCore) Update(request []byte) (*monstera.UpdateResponse, error) {
	key, value := parsePlaygroundUpdate(request)
	err := c.store.BatchUpdate(func(b *store.Batch) error {
		return b.Set(c.rowKey(key), []byte(value))
	})
	if err != nil {
		return nil, err
	}
	return &monstera.UpdateResponse{Data: []byte(value)}, nil
}

func (c *ExclusivePlaygroundCore) Snapshot() monstera.ApplicationCoreSnapshot {
	return &txnPlaygroundSnapshot{txn: c.store.View(), scan: c.scanState}
}

func (c *ExclusivePlaygroundCore) scanState(txn *store.Txn) (map[uint64]string, error) {
	state := make(map[uint64]string)
	err := txn.EachPrefix(c.prefix, func(k, v []byte) (bool, error) {
		state[binary.BigEndian.Uint64(k[len(c.prefix):])] = string(v)
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return state, nil
}

func (c *ExclusivePlaygroundCore) Restore(snapshots ...io.ReadCloser) error {
	state, err := decodePlaygroundStreams(c.lowerBound, c.upperBound, snapshots)
	if err != nil {
		return err
	}

	// Replace semantics: this core exclusively owns its prefix, so dropping
	// it wholesale is safe by construction.
	if err := c.store.DropPrefix(c.prefix); err != nil {
		return err
	}
	return c.store.BatchUpdate(func(b *store.Batch) error {
		for k, v := range state {
			if err := b.Set(c.rowKey(k), []byte(v)); err != nil {
				return err
			}
		}
		return nil
	})
}

// txnPlaygroundSnapshot is a snapshot over a pinned Badger view: scan
// materializes the state map at Write time from the transaction taken at
// Snapshot time, so the stream is consistent while updates continue.
type txnPlaygroundSnapshot struct {
	txn  *store.Txn
	scan func(*store.Txn) (map[uint64]string, error)
}

func (s *txnPlaygroundSnapshot) Write(w io.Writer) error {
	state, err := s.scan(s.txn)
	if err != nil {
		return err
	}
	return gob.NewEncoder(w).Encode(state)
}

func (s *txnPlaygroundSnapshot) Release() {
	s.txn.Discard()
}
