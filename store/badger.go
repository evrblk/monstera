package store

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"time"

	"github.com/dgraph-io/badger/v4"
)

var (
	// ErrNotFound is returned when a key does not exist in the store.
	ErrNotFound = errors.New("not found")
	// ErrConflict is returned when a transaction conflicts with a concurrent write.
	ErrConflict = errors.New("transaction conflict")
)

// BadgerStore is a persistent key-value store backed by BadgerDB.
// For disk-based stores it runs a background GC goroutine to reclaim value log space.
type BadgerStore struct {
	db       *badger.DB
	gcCancel context.CancelFunc
	gcDone   chan struct{}
}

// NewBadgerInMemoryStore creates a BadgerStore that stores all data in memory.
// Useful for testing; data is lost when the process exits.
func NewBadgerInMemoryStore() (*BadgerStore, error) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLoggingLevel(badger.ERROR))
	if err != nil {
		return nil, err
	}

	return &BadgerStore{
		db: db,
	}, nil
}

// NewBadgerStore opens (or creates) a persistent BadgerDB store at dir.
// It starts a background goroutine that runs value log GC every minute.
func NewBadgerStore(dir string) (*BadgerStore, error) {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, err
	}
	db, err := badger.Open(badger.DefaultOptions(dir).
		WithLoggingLevel(badger.ERROR).
		WithSyncWrites(false).
		WithMaxLevels(16).
		WithDetectConflicts(false))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	gcDone := make(chan struct{})
	go func() {
		defer close(gcDone)

		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for {
					err := db.RunValueLogGC(0.5)
					if err != nil {
						if !errors.Is(err, badger.ErrNoRewrite) {
							log.Printf("Error from RunValueLogGC %v", err)
						}
						break
					}
					// If no error then continue another GC cycle immediately
				}
			}
		}
	}()

	return &BadgerStore{
		db:       db,
		gcCancel: cancel,
		gcDone:   gcDone,
	}, nil
}

// Close stops the background GC goroutine (if running) and closes the database.
func (s *BadgerStore) Close() {
	if s.gcCancel != nil {
		s.gcCancel()
		<-s.gcDone
	}

	if err := s.db.Close(); err != nil {
		log.Printf("Error while closing BadgerStore %v", err)
	}
}

// View starts a read-only transaction. The caller must call Discard when done.
func (s *BadgerStore) View() *Txn {
	if s.db.IsClosed() {
		panic("db closed")
	}

	return &Txn{
		btxn: s.db.NewTransaction(false),
	}
}

// Update starts a read-write transaction. The caller must call Commit or Discard when done.
func (s *BadgerStore) Update() *Txn {
	if s.db.IsClosed() {
		panic("db closed")
	}

	return &Txn{
		btxn: s.db.NewTransaction(true),
	}
}

// Batch wraps a BadgerDB WriteBatch for bulk, non-transactional writes.
type Batch struct {
	wb *badger.WriteBatch
}

// Set writes key/value into the batch.
func (b *Batch) Set(key []byte, value []byte) error {
	return b.wb.Set(key, value)
}

// Delete removes key from the batch.
func (b *Batch) Delete(key []byte) error {
	return b.wb.Delete(key)
}

// BatchUpdate executes fn with a write batch and flushes it on success.
// The batch is cancelled if fn returns an error.
func (s *BadgerStore) BatchUpdate(fn func(batch *Batch) error) error {
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	err := fn(&Batch{wb: wb})
	if err != nil {
		return err
	}

	return wb.Flush()
}

// Flatten compacts the LSM tree using up to 4 concurrent workers.
func (s *BadgerStore) Flatten() error {
	return s.db.Flatten(4)
}

// DropPrefix deletes all keys that begin with prefix.
func (s *BadgerStore) DropPrefix(prefix []byte) error {
	return s.db.DropPrefix(prefix)
}

// Txn wraps a BadgerDB transaction, providing key-value access methods.
type Txn struct {
	btxn *badger.Txn
}

// Discard releases resources held by the transaction without committing.
func (t *Txn) Discard() {
	t.btxn.Discard()
}

// Commit persists the transaction. Returns ErrConflict if a concurrent write
// collided with this transaction.
func (t *Txn) Commit() error {
	err := t.btxn.Commit()
	if errors.Is(err, badger.ErrConflict) {
		return ErrConflict
	}
	return err
}

// PrefixExists reports whether any key with the given prefix exists.
func (t *Txn) PrefixExists(prefix []byte) (bool, error) {
	iter := t.btxn.NewIterator(badgerIteratorOptions())
	defer iter.Close()

	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		return true, nil
	}

	return false, nil
}

// EachPrefix iterates over all key/value pairs whose key begins with prefix,
// calling fn for each. Iteration stops early when fn returns false.
func (t *Txn) EachPrefix(prefix []byte, fn func(key []byte, value []byte) (bool, error)) error {
	iterOpt := badgerIteratorOptions()
	iterOpt.Prefix = prefix

	iter := t.btxn.NewIterator(iterOpt)
	defer iter.Close()

	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		item := iter.Item()
		var value []byte
		value, err := item.ValueCopy(value)
		if err != nil {
			return err
		}
		cont, err := fn(item.Key(), value)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}

	return nil
}

// EachPrefixKeys iterates over all keys that begin with prefix, calling fn for
// each key. Iteration stops early when fn returns false.
func (t *Txn) EachPrefixKeys(prefix []byte, fn func(key []byte) (bool, error)) error {
	iterOpt := badgerIteratorOptions()
	iterOpt.Prefix = prefix

	iter := t.btxn.NewIterator(iterOpt)
	defer iter.Close()

	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		cont, err := fn(iter.Item().Key())
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}

	return nil
}

// EachRange iterates over key/value pairs in [lowerBound, upperBound].
// When reverse is true the scan runs from upperBound down to lowerBound.
// Iteration stops early when fn returns false.
func (t *Txn) EachRange(lowerBound []byte, upperBound []byte, reverse bool, fn func(key []byte, value []byte) (bool, error)) error {
	iterOpt := badgerIteratorOptions()
	iterOpt.Reverse = reverse
	iter := t.btxn.NewIterator(iterOpt)
	defer iter.Close()

	if reverse {
		iter.Seek(upperBound)
	} else {
		iter.Seek(lowerBound)
	}

	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()

		if reverse {
			if bytes.Compare(key, lowerBound) < 0 {
				break
			}
		} else {
			if !bytes.HasPrefix(key, upperBound) && bytes.Compare(key, upperBound) > 0 {
				break
			}
		}

		var value []byte
		value, err := item.ValueCopy(value)
		if err != nil {
			return err
		}
		cont, err := fn(key, value)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}

	return nil
}

// Get returns the value for key. Returns ErrNotFound if the key does not exist.
func (t *Txn) Get(key []byte) ([]byte, error) {
	item, err := t.btxn.Get(key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	var value []byte
	value, err = item.ValueCopy(value)
	if err != nil {
		return nil, err
	}

	return value, nil
}

// Set writes a key/value pair within the transaction.
func (t *Txn) Set(key []byte, value []byte) error {
	return t.btxn.Set(key, value)
}

// Delete removes key from the store. It is a no-op if the key does not exist.
func (t *Txn) Delete(key []byte) error {
	err := t.btxn.Delete(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	} else {
		return err
	}
}

// badgerIteratorOptions returns iterator options tuned for forward, prefetch-enabled scans.
func badgerIteratorOptions() badger.IteratorOptions {
	iterOpt := badger.DefaultIteratorOptions
	iterOpt.PrefetchValues = true
	iterOpt.PrefetchSize = 100
	iterOpt.Reverse = false
	iterOpt.AllVersions = false

	return iterOpt
}
