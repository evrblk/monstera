package monstera

import (
	"encoding/binary"
	"errors"
	"slices"
	"sync"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

var (
	// hashicorp/raft expects this exact error message, but this is not documented anywhere.
	// errNotFound is to preserve the message even if underlying monstera implementation
	// returns different error message for "key not found" error.
	errNotFound = errors.New("not found")
)

type HraftBadgerStore struct {
	// Shared Badger store
	store *BadgerStore
	// keyPrefix is a unique prefix that allows isolation of multiple raft stores on the single shared Badger store
	keyPrefix []byte

	// mu protects fields below
	mu         sync.RWMutex
	firstIndex uint64
	lastIndex  uint64

	// Precalculated keys for performance optimization
	firstIndexFullKey []byte
	lastIndexFullKey  []byte
}

var _ hraft.LogStore = &HraftBadgerStore{}
var _ hraft.StableStore = &HraftBadgerStore{}

func NewHraftBadgerStore(badgerStore *BadgerStore, keyPrefix []byte) *HraftBadgerStore {
	txn := badgerStore.View()
	defer txn.Discard()

	firstIndex := uint64(0)
	firstIndexFullKey := ConcatBytes(keyPrefix, firstIndexKey)
	val, err := txn.Get(firstIndexFullKey)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			panic(err)
		}
	} else {
		firstIndex = bytesToUint64(val)
	}

	lastIndex := uint64(0)
	lastIndexFullKey := ConcatBytes(keyPrefix, lastIndexKey)
	val, err = txn.Get(lastIndexFullKey)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
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
	return h.store.BatchUpdate(func(batch *Batch) error {
		fullKey := ConcatBytes(h.keyPrefix, stableStorePrefix, key)
		return batch.Set(fullKey, val)
	})
}

// Get returns the value for key, or error if key was not found.
func (h *HraftBadgerStore) Get(key []byte) ([]byte, error) {
	txn := h.store.View()
	defer txn.Discard()

	fullKey := ConcatBytes(h.keyPrefix, stableStorePrefix, key)
	val, err := txn.Get(fullKey)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, errNotFound
		} else {
			return nil, err
		}
	}

	return val, nil
}

func (h *HraftBadgerStore) SetUint64(key []byte, val uint64) error {
	return h.store.BatchUpdate(func(batch *Batch) error {
		fullKey := ConcatBytes(h.keyPrefix, stableStorePrefix, key)
		return batch.Set(fullKey, uint64ToBytes(val))
	})
}

// GetUint64 returns the uint64 value for key, or error if key was not found.
func (h *HraftBadgerStore) GetUint64(key []byte) (uint64, error) {
	txn := h.store.View()
	defer txn.Discard()

	fullKey := ConcatBytes(h.keyPrefix, stableStorePrefix, key)
	val, err := txn.Get(fullKey)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
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

	fullKey := ConcatBytes(h.keyPrefix, logStorePrefix, index)
	logBytes, err := txn.Get(fullKey)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return hraft.ErrLogNotFound
		} else {
			return err
		}
	}

	var message Log
	if err := proto.Unmarshal(logBytes, &message); err != nil {
		return err
	}

	*log = *decodeLog(&message)

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

	return h.store.BatchUpdate(func(batch *Batch) error {
		indexes := make([]uint64, len(logs))
		for i, l := range logs {
			indexes[i] = l.Index
		}

		if h.firstIndex == 0 {
			lowestIndex := slices.Min(indexes)
			err := h.setFirstIndex(lowestIndex, batch)
			if err != nil {
				return err
			}
		}

		highestIndex := slices.Max(indexes)
		if h.lastIndex < highestIndex {
			err := h.setLastIndex(highestIndex, batch)
			if err != nil {
				return err
			}
		}

		for _, l := range logs {
			message := encodeLog(l)
			logBytes, err := proto.Marshal(message)
			if err != nil {
				return err
			}

			fullKey := ConcatBytes(h.keyPrefix, logStorePrefix, l.Index)
			err = batch.Set(fullKey, logBytes)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (h *HraftBadgerStore) DeleteRange(min uint64, max uint64) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.store.BatchUpdate(func(batch *Batch) error {
		if min <= h.firstIndex {
			err := h.setFirstIndex(max+1, batch)
			if err != nil {
				return err
			}
		}
		if max >= h.lastIndex {
			err := h.setLastIndex(min-1, batch)
			if err != nil {
				return err
			}
		}
		if h.firstIndex > h.lastIndex {
			err := h.setFirstIndex(0, batch)
			if err != nil {
				return err
			}
			err = h.setLastIndex(0, batch)
			if err != nil {
				return err
			}
		}

		for i := min; i <= max; i++ {
			fullKey := ConcatBytes(h.keyPrefix, logStorePrefix, i)
			err := batch.Delete(fullKey)
			if err != nil && !errors.Is(err, ErrNotFound) {
				return err
			}
		}

		return nil
	})
}

// setFirstIndex needs to be called inside the locked mutex h.mu
func (h *HraftBadgerStore) setFirstIndex(value uint64, batch *Batch) error {
	// Update cached value
	h.firstIndex = value

	// Update stored value
	return batch.Set(h.firstIndexFullKey, uint64ToBytes(value))
}

// setLastIndex needs to be called inside the locked mutex h.mu
func (h *HraftBadgerStore) setLastIndex(value uint64, batch *Batch) error {
	// Update cached value
	h.lastIndex = value

	// Update stored value
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
