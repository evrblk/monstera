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

	// mu protects fields below
	mu         sync.RWMutex
	firstIndex uint64
	lastIndex  uint64

	// Precalculated keys for performance optimization
	firstIndexFullKey []byte
	lastIndexFullKey  []byte

	codec LogCodec
}

var _ hraft.LogStore = &HraftBadgerStore{}
var _ hraft.StableStore = &HraftBadgerStore{}

func NewHraftBadgerStore(badgerStore *store.BadgerStore, keyPrefix []byte, codec LogCodec) *HraftBadgerStore {
	txn := badgerStore.View()
	defer txn.Discard()

	firstIndex := uint64(0)
	firstIndexFullKey := utils.ConcatBytes(keyPrefix, firstIndexKey)
	val, err := txn.Get(firstIndexFullKey)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			panic(err)
		}
	} else {
		firstIndex = bytesToUint64(val)
	}

	lastIndex := uint64(0)
	lastIndexFullKey := utils.ConcatBytes(keyPrefix, lastIndexKey)
	val, err = txn.Get(lastIndexFullKey)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			panic(err)
		}
	} else {
		lastIndex = bytesToUint64(val)
	}

	return &HraftBadgerStore{
		store:             badgerStore,
		keyPrefix:         keyPrefix,
		firstIndex:        firstIndex,
		lastIndex:         lastIndex,
		firstIndexFullKey: firstIndexFullKey,
		lastIndexFullKey:  lastIndexFullKey,
		codec:             codec,
	}
}

var (
	stableStorePrefix = []byte{0x01}
	logStorePrefix    = []byte{0x02}
	firstIndexKey     = []byte{0x03, 0x01}
	lastIndexKey      = []byte{0x03, 0x02}
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
	h.mu.Lock()
	defer h.mu.Unlock()

	// Compute the new index bounds locally and only commit them to the in-memory
	// cache after the batch is durably flushed, so a Flush failure does not leave
	// the cache claiming indexes that were never persisted.
	newFirstIndex := h.firstIndex
	newLastIndex := h.lastIndex

	err := h.store.BatchUpdate(func(batch *store.Batch) error {
		indexes := make([]uint64, len(logs))
		for i, l := range logs {
			indexes[i] = l.Index
		}

		if h.firstIndex == 0 {
			lowestIndex := slices.Min(indexes)
			if err := h.putFirstIndex(lowestIndex, batch); err != nil {
				return err
			}
			newFirstIndex = lowestIndex
		}

		highestIndex := slices.Max(indexes)
		if h.lastIndex < highestIndex {
			if err := h.putLastIndex(highestIndex, batch); err != nil {
				return err
			}
			newLastIndex = highestIndex
		}

		for _, l := range logs {
			logBytes, err := h.codec.Encode(l)
			if err != nil {
				return err
			}

			fullKey := utils.ConcatBytes(h.keyPrefix, logStorePrefix, l.Index)
			err = batch.Set(fullKey, logBytes)
			if err != nil {
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

	// Compute the new index bounds locally and only commit them to the in-memory
	// cache after the batch is durably flushed (see StoreLogs).
	newFirstIndex := h.firstIndex
	newLastIndex := h.lastIndex

	err := h.store.BatchUpdate(func(batch *store.Batch) error {
		if min <= h.firstIndex {
			if err := h.putFirstIndex(max+1, batch); err != nil {
				return err
			}
			newFirstIndex = max + 1
		}
		if max >= h.lastIndex {
			if err := h.putLastIndex(min-1, batch); err != nil {
				return err
			}
			newLastIndex = min - 1
		}
		if newFirstIndex > newLastIndex {
			if err := h.putFirstIndex(0, batch); err != nil {
				return err
			}
			newFirstIndex = 0
			if err := h.putLastIndex(0, batch); err != nil {
				return err
			}
			newLastIndex = 0
		}

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

	h.firstIndex = newFirstIndex
	h.lastIndex = newLastIndex

	return nil
}

// putFirstIndex writes the first index into the batch. The in-memory cache
// (h.firstIndex) must be updated by the caller only after the batch is flushed
// successfully, so a Flush failure cannot desync the cache from stored state.
func (h *HraftBadgerStore) putFirstIndex(value uint64, batch *store.Batch) error {
	return batch.Set(h.firstIndexFullKey, uint64ToBytes(value))
}

// putLastIndex writes the last index into the batch. See putFirstIndex for the
// cache-update contract.
func (h *HraftBadgerStore) putLastIndex(value uint64, batch *store.Batch) error {
	return batch.Set(h.lastIndexFullKey, uint64ToBytes(value))
}

func uint64ToBytes(i uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, i)
	return buf
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
