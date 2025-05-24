package monstera

import (
	"bytes"
	"log"
	"os"
	"time"

	"errors"

	"github.com/dgraph-io/badger/v4"
)

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("transaction conflict")
)

type KeyRange struct {
	Lower []byte
	Upper []byte
}

type BadgerStore struct {
	db     *badger.DB
	ticker *time.Ticker
	done   chan bool
}

func NewBadgerInMemoryStore() *BadgerStore {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLoggingLevel(badger.ERROR))
	if err != nil {
		panic(err)
	}

	return &BadgerStore{
		db: db,
	}
}

func NewBadgerStore(dir string) *BadgerStore {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		panic(err)
	}
	db, err := badger.Open(badger.DefaultOptions(dir).
		WithLoggingLevel(badger.ERROR).
		WithSyncWrites(false).
		WithMaxLevels(16).
		WithDetectConflicts(false))
	if err != nil {
		panic(err)
	}

	ticker := time.NewTicker(1 * time.Minute)
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				for {
					err := db.RunValueLogGC(0.5)
					if err != nil {
						if errors.Is(err, badger.ErrNoRewrite) {
							// Completed GC
						} else {
							// Something failed
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
		db:     db,
		ticker: ticker,
		done:   done,
	}
}

func (s *BadgerStore) Close() {
	s.done <- true
	s.ticker.Stop()
	err := s.db.Close()
	if err != nil {
		log.Printf("Error while closing BadgerStore %v", err)
	}
}

func (s *BadgerStore) View() *Txn {
	if s.db.IsClosed() {
		panic("db closed")
	}

	return &Txn{
		btxn: s.db.NewTransaction(false),
	}
}

func (s *BadgerStore) Update() *Txn {
	if s.db.IsClosed() {
		panic("db closed")
	}

	return &Txn{
		btxn: s.db.NewTransaction(true),
	}
}

type Batch struct {
	wb *badger.WriteBatch
}

func (b *Batch) Set(key []byte, value []byte) error {
	return b.wb.Set(key, value)
}

func (b *Batch) Delete(key []byte) error {
	return b.wb.Delete(key)
}

func (s *BadgerStore) BatchUpdate(fn func(batch *Batch) error) error {
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	err := fn(&Batch{wb: wb})
	if err != nil {
		return err
	}

	return wb.Flush()
}

func (s *BadgerStore) Flatten() error {
	return s.db.Flatten(4)
}

func (s *BadgerStore) DropPrefix(prefix []byte) error {
	return s.db.DropPrefix(prefix)
}

type Txn struct {
	btxn *badger.Txn
}

func (t *Txn) Discard() {
	t.btxn.Discard()
}

func (t *Txn) Commit() error {
	err := t.btxn.Commit()
	if errors.Is(err, badger.ErrConflict) {
		return ErrConflict
	}
	return err
}

func (t *Txn) PrefixExists(prefix []byte) (bool, error) {
	iter := t.btxn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		return true, nil
	}

	return false, nil
}

func (t *Txn) EachPrefix(prefix []byte, fn func(key []byte, value []byte) (bool, error)) error {
	iterOpt := badger.DefaultIteratorOptions
	iterOpt.PrefetchValues = true
	iterOpt.Reverse = false
	iterOpt.AllVersions = false
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

func (t *Txn) EachPrefixKeys(prefix []byte, fn func(key []byte) (bool, error)) error {
	iterOpt := badger.DefaultIteratorOptions
	iterOpt.PrefetchValues = false
	iterOpt.Reverse = false
	iterOpt.AllVersions = false
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

func (t *Txn) EachRange(lowerBound []byte, upperBound []byte, fn func(key []byte, value []byte) (bool, error)) error {
	iterOpt := badger.DefaultIteratorOptions
	iterOpt.PrefetchValues = true
	iterOpt.PrefetchSize = 100
	iterOpt.Reverse = false
	iterOpt.AllVersions = false

	iter := t.btxn.NewIterator(iterOpt)
	defer iter.Close()

	for iter.Seek(lowerBound); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		if upperBound != nil && !bytes.HasPrefix(key, upperBound) && bytes.Compare(key, upperBound) > 0 {
			break
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
		// fn() wants to break
		if !cont {
			return nil
		}
	}

	return nil
}

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

func (t *Txn) Set(key []byte, value []byte) error {
	return t.btxn.Set(key, value)
}

func (t *Txn) Delete(key []byte) error {
	err := t.btxn.Delete(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	} else {
		return err
	}
}
