package raft

import (
	"encoding/binary"
	"errors"
	"slices"
	"sync"

	hraft "github.com/hashicorp/raft"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
)

var (
	// hashicorp/raft expects this exact error message, but this is not documented anywhere.
	// errNotFound is to preserve the message even if underlying monstera implementation
	// returns different error message for "key not found" error.
	errNotFound = errors.New("not found")
)

type HraftBadgerStore struct {
	// Shared Badger store
	store *store.BadgerStore
	// keyPrefix is a unique prefix that allows isolation of multiple raft stores on the single shared Badger store
	keyPrefix []byte

	// mu protects fields below.
	//
	// firstIndex/lastIndex are an in-memory cache of the log bounds. They are NOT
	// persisted separately: the stored log entries are the single source of truth
	// and the bounds are re-derived from them at startup (see deriveIndexBounds).
	// This is what makes StoreLogs crash-safe despite Badger's WriteBatch splitting
	// a large batch across several transactions — a partially-durable batch can
	// never leave a persisted bound pointing past the entries that survived.
	mu         sync.RWMutex
	firstIndex uint64
	lastIndex  uint64

	codec LogCodec
}

var _ hraft.LogStore = &HraftBadgerStore{}
var _ hraft.StableStore = &HraftBadgerStore{}

func NewHraftBadgerStore(badgerStore *store.BadgerStore, keyPrefix []byte, codec LogCodec) *HraftBadgerStore {
	txn := badgerStore.View()
	defer txn.Discard()

	firstIndex, lastIndex, err := deriveIndexBounds(txn, keyPrefix)
	if err != nil {
		panic(err)
	}

	return &HraftBadgerStore{
		store:      badgerStore,
		keyPrefix:  keyPrefix,
		firstIndex: firstIndex,
		lastIndex:  lastIndex,
		codec:      codec,
	}
}

// deriveIndexBounds returns the smallest and largest log index actually stored
// under keyPrefix (0, 0 if there are none). Because log keys encode the index as
// a big-endian uint64 suffix (utils.ConcatBytes), they sort in numeric order, so
// the first and last keys in the log range give the bounds directly. The stored
// entries are the sole source of truth for the bounds — nothing else is
// persisted — so this can never disagree with what durably survived a crash.
//
// Note: older stores may still hold now-unused first/last-index keys (the 0x03
// prefix); they sort above the log range and are simply ignored, so reopening
// such a store also self-corrects any previously-skewed bound.
func deriveIndexBounds(txn *store.Txn, keyPrefix []byte) (uint64, uint64, error) {
	var firstIndex, lastIndex uint64

	logPrefix := utils.ConcatBytes(keyPrefix, logStorePrefix)
	if err := txn.EachPrefixKeys(logPrefix, func(key []byte) (bool, error) {
		firstIndex = bytesToUint64(key[len(key)-8:])
		return false, nil // the first key is the smallest index
	}); err != nil {
		return 0, 0, err
	}

	// Reverse-scan the log range; the first key visited is the largest index. The
	// bounds keep every key in [lower, upper] within logPrefix, so the callback
	// only ever sees a real log key.
	lower := utils.ConcatBytes(keyPrefix, logStorePrefix, uint64(0))
	upper := utils.ConcatBytes(keyPrefix, logStorePrefix, ^uint64(0))
	if err := txn.EachRange(lower, upper, true, func(key []byte, _ []byte) (bool, error) {
		lastIndex = bytesToUint64(key[len(key)-8:])
		return false, nil
	}); err != nil {
		return 0, 0, err
	}

	return firstIndex, lastIndex, nil
}

var (
	stableStorePrefix = []byte{0x01}
	logStorePrefix    = []byte{0x02}
)

// StableStore methods

func (h *HraftBadgerStore) Set(key []byte, val []byte) error {
	return h.store.BatchUpdate(func(batch *store.Batch) error {
		fullKey := utils.ConcatBytes(h.keyPrefix, stableStorePrefix, key)
		return batch.Set(fullKey, val)
	})
}

// Get returns the value for key, or error if key was not found.
func (h *HraftBadgerStore) Get(key []byte) ([]byte, error) {
	txn := h.store.View()
	defer txn.Discard()

	fullKey := utils.ConcatBytes(h.keyPrefix, stableStorePrefix, key)
	val, err := txn.Get(fullKey)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, errNotFound
		} else {
			return nil, err
		}
	}

	return val, nil
}

func (h *HraftBadgerStore) SetUint64(key []byte, val uint64) error {
	return h.store.BatchUpdate(func(batch *store.Batch) error {
		fullKey := utils.ConcatBytes(h.keyPrefix, stableStorePrefix, key)
		return batch.Set(fullKey, uint64ToBytes(val))
	})
}

// GetUint64 returns the uint64 value for key, or error if key was not found.
func (h *HraftBadgerStore) GetUint64(key []byte) (uint64, error) {
	txn := h.store.View()
	defer txn.Discard()

	fullKey := utils.ConcatBytes(h.keyPrefix, stableStorePrefix, key)
	val, err := txn.Get(fullKey)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return 0, errNotFound
		} else {
			return 0, err
		}
	}
	return bytesToUint64(val), nil
}

// LogStore methods

// FirstIndex returns the first index written. 0 for no entries.
func (h *HraftBadgerStore) FirstIndex() (uint64, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.firstIndex, nil
}

// LastIndex returns the last index written. 0 for no entries.
func (h *HraftBadgerStore) LastIndex() (uint64, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.lastIndex, nil
}

// GetLog gets a log entry at a given index.
func (h *HraftBadgerStore) GetLog(index uint64, log *hraft.Log) error {
	txn := h.store.View()
	defer txn.Discard()

	fullKey := utils.ConcatBytes(h.keyPrefix, logStorePrefix, index)
	logBytes, err := txn.Get(fullKey)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return hraft.ErrLogNotFound
		}

		return err
	}

	if err := h.codec.Decode(logBytes, log); err != nil {
		return err
	}

	return nil
}

// StoreLog stores a log entry.
func (h *HraftBadgerStore) StoreLog(log *hraft.Log) error {
	return h.StoreLogs([]*hraft.Log{log})
}

// StoreLogs stores multiple log entries. By default, the logs stored may not be contiguous with previous logs (i.e. may
// have a gap in Index since the last log written). If an implementation can't tolerate this it may optionally implement
// `MonotonicLogStore` to indicate that this is not allowed. This changes Raft's behaviour after restoring a user
// snapshot to remove all previous logs instead of relying on a "gap" to signal the discontinuity between logs before the
// snapshot and logs after.
func (h *HraftBadgerStore) StoreLogs(logs []*hraft.Log) error {
	if len(logs) == 0 {
		return nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	// Only the log entries are persisted; the index bounds are derived from them
	// at startup. So the batch need not be atomic with any metadata: if Badger's
	// WriteBatch splits it and a crash leaves only a prefix durable, the next
	// startup simply derives the bounds from whatever survived — no bound can
	// point past a missing entry. The in-memory cache is updated only after a
	// successful Flush so a Flush failure cannot desync it from stored state.
	indexes := make([]uint64, len(logs))
	for i, l := range logs {
		indexes[i] = l.Index
	}
	newFirstIndex := h.firstIndex
	if h.firstIndex == 0 {
		newFirstIndex = slices.Min(indexes)
	}
	newLastIndex := h.lastIndex
	if highestIndex := slices.Max(indexes); h.lastIndex < highestIndex {
		newLastIndex = highestIndex
	}

	err := h.store.BatchUpdate(func(batch *store.Batch) error {
		for _, l := range logs {
			logBytes, err := h.codec.Encode(l)
			if err != nil {
				return err
			}

			fullKey := utils.ConcatBytes(h.keyPrefix, logStorePrefix, l.Index)
			if err := batch.Set(fullKey, logBytes); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	h.firstIndex = newFirstIndex
	h.lastIndex = newLastIndex

	return nil
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (h *HraftBadgerStore) DeleteRange(min uint64, max uint64) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	err := h.store.BatchUpdate(func(batch *store.Batch) error {
		for i := min; i <= max; i++ {
			fullKey := utils.ConcatBytes(h.keyPrefix, logStorePrefix, i)
			err := batch.Delete(fullKey)
			if err != nil && !errors.Is(err, store.ErrNotFound) {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Update the in-memory bounds after a successful flush. hraft only deletes a
	// contiguous range at the front (compaction) or back (truncation) of the log,
	// so shrinking the matching bound keeps the cache equal to what a fresh
	// deriveIndexBounds over the surviving entries would return.
	newFirstIndex := h.firstIndex
	newLastIndex := h.lastIndex
	if min <= newFirstIndex {
		newFirstIndex = max + 1
	}
	if max >= newLastIndex {
		newLastIndex = min - 1
	}
	if newFirstIndex > newLastIndex {
		newFirstIndex = 0
		newLastIndex = 0
	}
	h.firstIndex = newFirstIndex
	h.lastIndex = newLastIndex

	return nil
}

func uint64ToBytes(i uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, i)
	return buf
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
