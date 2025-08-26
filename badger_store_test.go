package monstera

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBadgerStoreView(t *testing.T) {
	require := require.New(t)

	store := NewBadgerInMemoryStore()
	defer store.Close()

	// Set up some test data
	txn := store.Update()
	err := txn.Set([]byte("key1"), []byte("value1"))
	require.NoError(err)
	err = txn.Set([]byte("key2"), []byte("value2"))
	require.NoError(err)
	err = txn.Set([]byte("prefix:key1"), []byte("prefix:value1"))
	require.NoError(err)
	err = txn.Set([]byte("prefix:key2"), []byte("prefix:value2"))
	require.NoError(err)
	err = txn.Commit()
	require.NoError(err)

	// Test View transaction
	viewTxn := store.View()
	defer viewTxn.Discard()

	// Test Get method
	value, err := viewTxn.Get([]byte("key1"))
	require.NoError(err)
	require.Equal([]byte("value1"), value)

	value, err = viewTxn.Get([]byte("key2"))
	require.NoError(err)
	require.Equal([]byte("value2"), value)

	// Test Get with non-existent key
	value, err = viewTxn.Get([]byte("nonexistent"))
	require.Error(err)
	require.ErrorIs(err, ErrNotFound)
	require.Nil(value)

	// Test PrefixExists
	exists, err := viewTxn.PrefixExists([]byte("prefix:"))
	require.NoError(err)
	require.True(exists)

	exists, err = viewTxn.PrefixExists([]byte("nonexistent:"))
	require.NoError(err)
	require.False(exists)

	// Test EachPrefix
	var prefixKeys []string
	var prefixValues []string
	err = viewTxn.EachPrefix([]byte("prefix:"), func(key []byte, value []byte) (bool, error) {
		prefixKeys = append(prefixKeys, string(key))
		prefixValues = append(prefixValues, string(value))
		return true, nil
	})
	require.NoError(err)
	require.Len(prefixKeys, 2)
	require.Contains(prefixKeys, "prefix:key1")
	require.Contains(prefixKeys, "prefix:key2")
	require.Contains(prefixValues, "prefix:value1")
	require.Contains(prefixValues, "prefix:value2")

	// Test EachPrefixKeys
	var keysOnly []string
	err = viewTxn.EachPrefixKeys([]byte("prefix:"), func(key []byte) (bool, error) {
		keysOnly = append(keysOnly, string(key))
		return true, nil
	})
	require.NoError(err)
	require.Len(keysOnly, 2)
	require.Contains(keysOnly, "prefix:key1")
	require.Contains(keysOnly, "prefix:key2")

	// Test EachRange
	var rangeKeys []string
	var rangeValues []string
	err = viewTxn.EachRange([]byte("key1"), []byte("key3"), false, func(key []byte, value []byte) (bool, error) {
		rangeKeys = append(rangeKeys, string(key))
		rangeValues = append(rangeValues, string(value))
		return true, nil
	})
	require.NoError(err)
	require.Len(rangeKeys, 2)
	require.Contains(rangeKeys, "key1")
	require.Contains(rangeKeys, "key2")
	require.Contains(rangeValues, "value1")
	require.Contains(rangeValues, "value2")
}

func TestBadgerStoreUpdate(t *testing.T) {
	require := require.New(t)

	store := NewBadgerInMemoryStore()
	defer store.Close()

	// Test Update transaction
	txn := store.Update()

	// Test Set method
	err := txn.Set([]byte("key1"), []byte("value1"))
	require.NoError(err)

	err = txn.Set([]byte("key2"), []byte("value2"))
	require.NoError(err)

	// Test Get before commit
	value, err := txn.Get([]byte("key1"))
	require.NoError(err)
	require.Equal([]byte("value1"), value)

	// Test Delete method
	err = txn.Delete([]byte("key2"))
	require.NoError(err)

	// Verify key2 is deleted
	_, err = txn.Get([]byte("key2"))
	require.Error(err)
	require.ErrorIs(err, ErrNotFound)

	// Test commit
	err = txn.Commit()
	require.NoError(err)

	// Verify changes are persisted
	viewTxn := store.View()
	defer viewTxn.Discard()

	value, err = viewTxn.Get([]byte("key1"))
	require.NoError(err)
	require.Equal([]byte("value1"), value)

	_, err = viewTxn.Get([]byte("key2"))
	require.Error(err)
	require.ErrorIs(err, ErrNotFound)
}

func TestBadgerStoreUpdateConflict(t *testing.T) {
	require := require.New(t)

	store := NewBadgerInMemoryStore()
	defer store.Close()

	// Set initial data
	txn1 := store.Update()
	err := txn1.Set([]byte("key1"), []byte("value1"))
	require.NoError(err)
	err = txn1.Commit()
	require.NoError(err)

	// Start two concurrent transactions
	txn2 := store.Update()
	txn3 := store.Update()

	// Both read the same key
	value, err := txn2.Get([]byte("key1"))
	require.NoError(err)
	require.Equal([]byte("value1"), value)

	value, err = txn3.Get([]byte("key1"))
	require.NoError(err)
	require.Equal([]byte("value1"), value)

	// First transaction commits
	err = txn2.Set([]byte("key1"), []byte("value2"))
	require.NoError(err)
	err = txn2.Commit()
	require.NoError(err)

	// Second transaction should conflict
	err = txn3.Set([]byte("key1"), []byte("value3"))
	require.NoError(err)
	err = txn3.Commit()
	require.Error(err)
	require.ErrorIs(err, ErrConflict)
}

func TestBadgerStoreBatchUpdate(t *testing.T) {
	require := require.New(t)

	store := NewBadgerInMemoryStore()
	defer store.Close()

	// Test BatchUpdate
	err := store.BatchUpdate(func(batch *Batch) error {
		err := batch.Set([]byte("batch:key1"), []byte("batch:value1"))
		if err != nil {
			return err
		}

		err = batch.Set([]byte("batch:key2"), []byte("batch:value2"))
		if err != nil {
			return err
		}

		err = batch.Delete([]byte("batch:key3"))
		if err != nil {
			return err
		}

		return nil
	})
	require.NoError(err)

	// Verify batch operations were applied
	viewTxn := store.View()
	defer viewTxn.Discard()

	value, err := viewTxn.Get([]byte("batch:key1"))
	require.NoError(err)
	require.Equal([]byte("batch:value1"), value)

	value, err = viewTxn.Get([]byte("batch:key2"))
	require.NoError(err)
	require.Equal([]byte("batch:value2"), value)

	// Verify deleted key doesn't exist
	_, err = viewTxn.Get([]byte("batch:key3"))
	require.Error(err)
	require.ErrorIs(err, ErrNotFound)
}

func TestBadgerStoreBatchUpdateError(t *testing.T) {
	require := require.New(t)

	store := NewBadgerInMemoryStore()
	defer store.Close()

	// Test BatchUpdate with error
	err := store.BatchUpdate(func(batch *Batch) error {
		err := batch.Set([]byte("key1"), []byte("value1"))
		if err != nil {
			return err
		}

		// Return an error to test rollback
		return ErrNotFound
	})
	require.Error(err)
	require.ErrorIs(err, ErrNotFound)

	// Verify no changes were applied
	viewTxn := store.View()
	defer viewTxn.Discard()

	value, err := viewTxn.Get([]byte("key1"))
	require.Error(err)
	require.ErrorIs(err, ErrNotFound)
	require.Nil(value)
}

func TestBadgerStoreDropPrefix(t *testing.T) {
	require := require.New(t)

	store := NewBadgerInMemoryStore()
	defer store.Close()

	// Set up test data with different prefixes
	txn := store.Update()
	err := txn.Set([]byte("prefix1:key1"), []byte("value1"))
	require.NoError(err)
	err = txn.Set([]byte("prefix1:key2"), []byte("value2"))
	require.NoError(err)
	err = txn.Set([]byte("prefix2:key1"), []byte("value3"))
	require.NoError(err)
	err = txn.Set([]byte("prefix2:key2"), []byte("value4"))
	require.NoError(err)
	err = txn.Set([]byte("other:key1"), []byte("value5"))
	require.NoError(err)
	err = txn.Commit()
	require.NoError(err)

	// Verify all data exists
	viewTxn := store.View()
	defer viewTxn.Discard()

	value, err := viewTxn.Get([]byte("prefix1:key1"))
	require.NoError(err)
	require.Equal([]byte("value1"), value)

	value, err = viewTxn.Get([]byte("prefix2:key1"))
	require.NoError(err)
	require.Equal([]byte("value3"), value)

	value, err = viewTxn.Get([]byte("other:key1"))
	require.NoError(err)
	require.Equal([]byte("value5"), value)

	// Drop prefix1
	err = store.DropPrefix([]byte("prefix1:"))
	require.NoError(err)

	// Verify prefix1 keys are deleted
	_, err = viewTxn.Get([]byte("prefix1:key1"))
	require.Error(err)
	require.ErrorIs(err, ErrNotFound)

	_, err = viewTxn.Get([]byte("prefix1:key2"))
	require.Error(err)
	require.ErrorIs(err, ErrNotFound)

	// Verify prefix2 and other keys still exist
	value, err = viewTxn.Get([]byte("prefix2:key1"))
	require.NoError(err)
	require.Equal([]byte("value3"), value)

	value, err = viewTxn.Get([]byte("other:key1"))
	require.NoError(err)
	require.Equal([]byte("value5"), value)

	// Drop prefix2
	err = store.DropPrefix([]byte("prefix2:"))
	require.NoError(err)

	// Verify prefix2 keys are deleted
	_, err = viewTxn.Get([]byte("prefix2:key1"))
	require.Error(err)
	require.ErrorIs(err, ErrNotFound)

	_, err = viewTxn.Get([]byte("prefix2:key2"))
	require.Error(err)
	require.ErrorIs(err, ErrNotFound)

	// Verify other keys still exist
	value, err = viewTxn.Get([]byte("other:key1"))
	require.NoError(err)
	require.Equal([]byte("value5"), value)
}

func TestBadgerStoreEachPrefixEarlyReturn(t *testing.T) {
	require := require.New(t)

	store := NewBadgerInMemoryStore()
	defer store.Close()

	// Set up test data
	txn := store.Update()
	for i := 1; i <= 10; i++ {
		key := []byte("prefix:key" + string(rune(i+'0')))
		value := []byte("value" + string(rune(i+'0')))
		err := txn.Set(key, value)
		require.NoError(err)
	}
	err := txn.Commit()
	require.NoError(err)

	// Test EachPrefix with early return
	viewTxn := store.View()
	defer viewTxn.Discard()

	var count int
	err = viewTxn.EachPrefix([]byte("prefix:"), func(key []byte, value []byte) (bool, error) {
		count++
		// Return false after 3 iterations to stop early
		return count < 3, nil
	})
	require.NoError(err)
	require.Equal(3, count)
}

func TestBadgerStoreEachRangeWithBounds(t *testing.T) {
	require := require.New(t)

	store := NewBadgerInMemoryStore()
	defer store.Close()

	// Set up test data
	txn := store.Update()
	keys := []string{"a", "b", "c", "d", "e", "f", "g"}
	for _, key := range keys {
		err := txn.Set([]byte(key), []byte("value"+key))
		require.NoError(err)
	}
	err := txn.Commit()
	require.NoError(err)

	// Test EachRange with bounds
	viewTxn := store.View()
	defer viewTxn.Discard()

	var rangeKeys []string
	err = viewTxn.EachRange([]byte("b"), []byte("f"), false, func(key []byte, value []byte) (bool, error) {
		rangeKeys = append(rangeKeys, string(key))
		return true, nil
	})
	require.NoError(err)
	require.Len(rangeKeys, 5)
	require.Equal("b", rangeKeys[0])
	require.Equal("c", rangeKeys[1])
	require.Equal("d", rangeKeys[2])
	require.Equal("e", rangeKeys[3])
	require.Equal("f", rangeKeys[4])
}

func TestBadgerStoreEachRangeNoUpperBound(t *testing.T) {
	require := require.New(t)

	store := NewBadgerInMemoryStore()
	defer store.Close()

	// Set up test data
	txn := store.Update()
	keys := []string{"a", "b", "c", "d", "e"}
	for _, key := range keys {
		err := txn.Set([]byte(key), []byte("value"+key))
		require.NoError(err)
	}
	err := txn.Commit()
	require.NoError(err)

	// Test EachRange without upper bound
	viewTxn := store.View()
	defer viewTxn.Discard()

	var rangeKeys []string
	err = viewTxn.EachRange([]byte("c"), nil, false, func(key []byte, value []byte) (bool, error) {
		rangeKeys = append(rangeKeys, string(key))
		return true, nil
	})
	require.NoError(err)
	require.Len(rangeKeys, 3)
	require.Equal("c", rangeKeys[0])
	require.Equal("d", rangeKeys[1])
	require.Equal("e", rangeKeys[2])
}

func TestBadgerStoreDeleteNonExistent(t *testing.T) {
	require := require.New(t)

	store := NewBadgerInMemoryStore()
	defer store.Close()

	txn := store.Update()
	defer txn.Discard()

	// Delete non-existent key should not error
	err := txn.Delete([]byte("nonexistent"))
	require.NoError(err)

	// Commit should succeed
	err = txn.Commit()
	require.NoError(err)
}

func TestBadgerStoreFlatten(t *testing.T) {
	require := require.New(t)

	store := NewBadgerInMemoryStore()
	defer store.Close()

	// Set up some data
	txn := store.Update()
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err := txn.Set(key, value)
		require.NoError(err)
	}
	err := txn.Commit()
	require.NoError(err)

	// Test Flatten
	err = store.Flatten()
	require.NoError(err)

	// Verify data is still accessible
	viewTxn := store.View()
	defer viewTxn.Discard()

	value, err := viewTxn.Get([]byte("key0"))
	require.NoError(err)
	require.Equal([]byte("value0"), value)

	value, err = viewTxn.Get([]byte("key99"))
	require.NoError(err)
	require.Equal([]byte("value99"), value)
}
