package monstera

import (
	"fmt"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func testRaftLog(idx uint64, data string) *raft.Log {
	return &raft.Log{
		Data:  []byte(data),
		Index: idx,
	}
}

func TestHraftBadgerStoreFirstIndex(t *testing.T) {
	require := require.New(t)

	hraftStore := NewHraftBadgerStore(NewBadgerInMemoryStore(), []byte("test"))

	// Should get 0 index on empty log
	idx, err := hraftStore.FirstIndex()
	require.NoError(err)
	require.Equal(uint64(0), idx)

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	err = hraftStore.StoreLogs(logs)
	require.NoError(err)

	// Fetch the first Raft index
	idx, err = hraftStore.FirstIndex()
	require.NoError(err)
	require.Equal(uint64(1), idx)
}

func TestHraftBadgerStoreLastIndex(t *testing.T) {
	require := require.New(t)

	hraftStore := NewHraftBadgerStore(NewBadgerInMemoryStore(), []byte("test"))

	// Should get 0 index on empty log
	idx, err := hraftStore.LastIndex()
	require.NoError(err)
	require.Equal(uint64(0), idx)

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	err = hraftStore.StoreLogs(logs)
	require.NoError(err)

	// Fetch the last Raft index
	idx, err = hraftStore.LastIndex()
	require.NoError(err)
	require.Equal(uint64(3), idx)
}

func TestHraftBadgerStoreGetLog(t *testing.T) {
	require := require.New(t)

	hraftStore := NewHraftBadgerStore(NewBadgerInMemoryStore(), []byte("test"))

	log := new(raft.Log)

	// Should return an error on non-existent log
	err := hraftStore.GetLog(1, log)
	require.Error(err)
	require.ErrorIs(err, raft.ErrLogNotFound)

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	err = hraftStore.StoreLogs(logs)
	require.NoError(err)

	// Should return the proper log
	err = hraftStore.GetLog(2, log)
	require.NoError(err)
	require.Equal(log, logs[1])
}

func TestHraftBadgerStoreSetLog(t *testing.T) {
	require := require.New(t)

	hraftStore := NewHraftBadgerStore(NewBadgerInMemoryStore(), []byte("test"))

	// Create the log
	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}

	// Attempt to store the log
	err := hraftStore.StoreLog(log)
	require.NoError(err)

	// Retrieve the log again
	result := new(raft.Log)
	err = hraftStore.GetLog(1, result)
	require.NoError(err)

	// Ensure the log comes back the same
	require.Equal(log, result)
}

func TestHraftBadgerStoreSetLogs(t *testing.T) {
	require := require.New(t)

	hraftStore := NewHraftBadgerStore(NewBadgerInMemoryStore(), []byte("test"))

	// Create a set of logs
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
	}

	// Attempt to store the logs
	err := hraftStore.StoreLogs(logs)
	require.NoError(err)

	// Ensure we stored them all
	result1, result2 := new(raft.Log), new(raft.Log)
	err = hraftStore.GetLog(1, result1)
	require.NoError(err)
	require.Equal(logs[0], result1)
	err = hraftStore.GetLog(2, result2)
	require.NoError(err)
	require.Equal(logs[1], result2)
}

func TestHraftBadgerStoreDeleteRange(t *testing.T) {
	require := require.New(t)

	hraftStore := NewHraftBadgerStore(NewBadgerInMemoryStore(), []byte("test"))

	// Create a set of logs
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}

	// Attempt to store the logs
	err := hraftStore.StoreLogs(logs)
	require.NoError(err)

	// Attempt to delete a range of logs
	err = hraftStore.DeleteRange(1, 2)
	require.NoError(err)

	// Ensure the logs were deleted
	err = hraftStore.GetLog(1, new(raft.Log))
	require.Error(err)
	require.ErrorIs(err, raft.ErrLogNotFound)
	err = hraftStore.GetLog(2, new(raft.Log))
	require.Error(err)
	require.ErrorIs(err, raft.ErrLogNotFound)
}

func TestHraftBadgerStoreDeleteRangeEmptyStore(t *testing.T) {
	require := require.New(t)

	hraftStore := NewHraftBadgerStore(NewBadgerInMemoryStore(), []byte("test"))

	// Delete range on empty store should not error
	err := hraftStore.DeleteRange(1, 10)
	require.NoError(err)

	// First and last index should remain 0
	firstIdx, err := hraftStore.FirstIndex()
	require.NoError(err)
	require.Equal(uint64(0), firstIdx)

	lastIdx, err := hraftStore.LastIndex()
	require.NoError(err)
	require.Equal(uint64(0), lastIdx)
}

func TestHraftBadgerStoreDeleteRangeSingleLog(t *testing.T) {
	require := require.New(t)

	hraftStore := NewHraftBadgerStore(NewBadgerInMemoryStore(), []byte("test"))

	// Store a single log
	log := testRaftLog(5, "single_log")
	err := hraftStore.StoreLog(log)
	require.NoError(err)

	// Delete the single log
	err = hraftStore.DeleteRange(5, 5)
	require.NoError(err)

	// Verify the log is deleted
	err = hraftStore.GetLog(5, new(raft.Log))
	require.Error(err)
	require.ErrorIs(err, raft.ErrLogNotFound)

	// First and last index should be reset to 0
	firstIdx, err := hraftStore.FirstIndex()
	require.NoError(err)
	require.Equal(uint64(0), firstIdx)

	lastIdx, err := hraftStore.LastIndex()
	require.NoError(err)
	require.Equal(uint64(0), lastIdx)
}

func TestHraftBadgerStoreDeleteRangePartial(t *testing.T) {
	require := require.New(t)

	hraftStore := NewHraftBadgerStore(NewBadgerInMemoryStore(), []byte("test"))

	// Store logs from 1 to 10
	logs := make([]*raft.Log, 10)
	for i := 1; i <= 10; i++ {
		logs[i-1] = testRaftLog(uint64(i), fmt.Sprintf("log%d", i))
	}
	err := hraftStore.StoreLogs(logs)
	require.NoError(err)

	// Delete range 3-7
	err = hraftStore.DeleteRange(3, 7)
	require.NoError(err)

	// Verify deleted logs are gone
	for i := 3; i <= 7; i++ {
		err = hraftStore.GetLog(uint64(i), new(raft.Log))
		require.Error(err)
		require.ErrorIs(err, raft.ErrLogNotFound)
	}

	// Verify remaining logs are still there
	for i := 1; i <= 2; i++ {
		log := new(raft.Log)
		err = hraftStore.GetLog(uint64(i), log)
		require.NoError(err)
		require.Equal(fmt.Sprintf("log%d", i), string(log.Data))
	}

	for i := 8; i <= 10; i++ {
		log := new(raft.Log)
		err = hraftStore.GetLog(uint64(i), log)
		require.NoError(err)
		require.Equal(fmt.Sprintf("log%d", i), string(log.Data))
	}

	// First index should be updated to 1 (unchanged)
	firstIdx, err := hraftStore.FirstIndex()
	require.NoError(err)
	require.Equal(uint64(1), firstIdx)

	// Last index should be updated to 10 (unchanged)
	lastIdx, err := hraftStore.LastIndex()
	require.NoError(err)
	require.Equal(uint64(10), lastIdx)
}

func TestHraftBadgerStoreDeleteRangeFromBeginning(t *testing.T) {
	require := require.New(t)

	hraftStore := NewHraftBadgerStore(NewBadgerInMemoryStore(), []byte("test"))

	// Store logs from 1 to 10
	logs := make([]*raft.Log, 10)
	for i := 1; i <= 10; i++ {
		logs[i-1] = testRaftLog(uint64(i), fmt.Sprintf("log%d", i))
	}
	err := hraftStore.StoreLogs(logs)
	require.NoError(err)

	// Delete range 1-5 (from beginning)
	err = hraftStore.DeleteRange(1, 5)
	require.NoError(err)

	// Verify deleted logs are gone
	for i := 1; i <= 5; i++ {
		err = hraftStore.GetLog(uint64(i), new(raft.Log))
		require.Error(err)
		require.ErrorIs(err, raft.ErrLogNotFound)
	}

	// Verify remaining logs are still there
	for i := 6; i <= 10; i++ {
		log := new(raft.Log)
		err = hraftStore.GetLog(uint64(i), log)
		require.NoError(err)
		require.Equal(fmt.Sprintf("log%d", i), string(log.Data))
	}

	// First index should be updated to 6
	firstIdx, err := hraftStore.FirstIndex()
	require.NoError(err)
	require.Equal(uint64(6), firstIdx)

	// Last index should remain 10
	lastIdx, err := hraftStore.LastIndex()
	require.NoError(err)
	require.Equal(uint64(10), lastIdx)
}

func TestHraftBadgerStoreDeleteRangeToEnd(t *testing.T) {
	require := require.New(t)

	hraftStore := NewHraftBadgerStore(NewBadgerInMemoryStore(), []byte("test"))

	// Store logs from 1 to 10
	logs := make([]*raft.Log, 10)
	for i := 1; i <= 10; i++ {
		logs[i-1] = testRaftLog(uint64(i), fmt.Sprintf("log%d", i))
	}
	err := hraftStore.StoreLogs(logs)
	require.NoError(err)

	// Delete range 6-10 (to end)
	err = hraftStore.DeleteRange(6, 10)
	require.NoError(err)

	// Verify deleted logs are gone
	for i := 6; i <= 10; i++ {
		err = hraftStore.GetLog(uint64(i), new(raft.Log))
		require.Error(err)
		require.ErrorIs(err, raft.ErrLogNotFound)
	}

	// Verify remaining logs are still there
	for i := 1; i <= 5; i++ {
		log := new(raft.Log)
		err = hraftStore.GetLog(uint64(i), log)
		require.NoError(err)
		require.Equal(fmt.Sprintf("log%d", i), string(log.Data))
	}

	// First index should remain 1
	firstIdx, err := hraftStore.FirstIndex()
	require.NoError(err)
	require.Equal(uint64(1), firstIdx)

	// Last index should be updated to 5
	lastIdx, err := hraftStore.LastIndex()
	require.NoError(err)
	require.Equal(uint64(5), lastIdx)
}

func TestHraftBadgerStoreDeleteRangeEntireStore(t *testing.T) {
	require := require.New(t)

	hraftStore := NewHraftBadgerStore(NewBadgerInMemoryStore(), []byte("test"))

	// Store logs from 1 to 10
	logs := make([]*raft.Log, 10)
	for i := 1; i <= 10; i++ {
		logs[i-1] = testRaftLog(uint64(i), fmt.Sprintf("log%d", i))
	}
	err := hraftStore.StoreLogs(logs)
	require.NoError(err)

	// Delete entire range
	err = hraftStore.DeleteRange(1, 10)
	require.NoError(err)

	// Verify all logs are deleted
	for i := 1; i <= 10; i++ {
		err = hraftStore.GetLog(uint64(i), new(raft.Log))
		require.Error(err)
		require.ErrorIs(err, raft.ErrLogNotFound)
	}

	// First and last index should be reset to 0
	firstIdx, err := hraftStore.FirstIndex()
	require.NoError(err)
	require.Equal(uint64(0), firstIdx)

	lastIdx, err := hraftStore.LastIndex()
	require.NoError(err)
	require.Equal(uint64(0), lastIdx)
}

func TestHraftBadgerStoreDeleteRangeNonExistent(t *testing.T) {
	require := require.New(t)

	hraftStore := NewHraftBadgerStore(NewBadgerInMemoryStore(), []byte("test"))

	// Store logs 1, 3, 5 (with gaps)
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(3, "log3"),
		testRaftLog(5, "log5"),
	}
	err := hraftStore.StoreLogs(logs)
	require.NoError(err)

	// Delete range that includes non-existent logs (2, 4)
	err = hraftStore.DeleteRange(2, 4)
	require.NoError(err)

	// Verify logs 2 and 4 are still not found (they never existed)
	err = hraftStore.GetLog(2, new(raft.Log))
	require.Error(err)
	require.ErrorIs(err, raft.ErrLogNotFound)

	err = hraftStore.GetLog(4, new(raft.Log))
	require.Error(err)
	require.ErrorIs(err, raft.ErrLogNotFound)

	// Verify existing logs are still there
	for _, idx := range []uint64{1, 5} {
		log := new(raft.Log)
		err = hraftStore.GetLog(idx, log)
		require.NoError(err)
		require.Equal(fmt.Sprintf("log%d", idx), string(log.Data))
	}

	// First and last index should remain unchanged
	firstIdx, err := hraftStore.FirstIndex()
	require.NoError(err)
	require.Equal(uint64(1), firstIdx)

	lastIdx, err := hraftStore.LastIndex()
	require.NoError(err)
	require.Equal(uint64(5), lastIdx)
}

func TestHraftBadgerStoreDeleteRangeInvalidRange(t *testing.T) {
	require := require.New(t)

	hraftStore := NewHraftBadgerStore(NewBadgerInMemoryStore(), []byte("test"))

	// Store some logs
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	err := hraftStore.StoreLogs(logs)
	require.NoError(err)

	// Delete range where min > max (invalid range)
	err = hraftStore.DeleteRange(5, 3)
	require.NoError(err) // Implementation doesn't validate range order

	// Verify no logs were affected
	for i := 1; i <= 3; i++ {
		log := new(raft.Log)
		err = hraftStore.GetLog(uint64(i), log)
		require.NoError(err)
		require.Equal(fmt.Sprintf("log%d", i), string(log.Data))
	}
}

func TestHraftBadgerStoreSetAndGet(t *testing.T) {
	require := require.New(t)

	hraftStore := NewHraftBadgerStore(NewBadgerInMemoryStore(), []byte("test"))

	// Returns error on non-existent key
	_, err := hraftStore.Get([]byte("bad"))
	require.Error(err)
	require.Equal("not found", err.Error())

	k, v := []byte("hello"), []byte("world")

	// Try to set a k/v pair
	err = hraftStore.Set(k, v)
	require.NoError(err)

	// Try to read it back
	val, err := hraftStore.Get(k)
	require.NoError(err)
	require.Equal(v, val)
}

func TestHraftBadgerStoreSetUint64AndGetUint64(t *testing.T) {
	require := require.New(t)

	hraftStore := NewHraftBadgerStore(NewBadgerInMemoryStore(), []byte("test"))

	// Returns error on non-existent key
	_, err := hraftStore.GetUint64([]byte("bad"))
	require.Error(err)
	require.Equal("not found", err.Error())

	k, v := []byte("abc"), uint64(123)

	// Attempt to set the k/v pair
	err = hraftStore.SetUint64(k, v)
	require.NoError(err)

	// Read back the value
	val, err := hraftStore.GetUint64(k)
	require.NoError(err)
	require.Equal(v, val)
}

func TestHraftBadgerStoreNewWithExistingData(t *testing.T) {
	require := require.New(t)

	// Create a Badger store
	badgerStore := NewBadgerInMemoryStore()
	keyPrefix := []byte("test")

	// Create first HraftBadgerStore and populate it with data
	hraftStore1 := NewHraftBadgerStore(badgerStore, keyPrefix)

	// Store some logs
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
		testRaftLog(4, "log4"),
		testRaftLog(5, "log5"),
	}
	err := hraftStore1.StoreLogs(logs)
	require.NoError(err)

	// Store some stable store data
	err = hraftStore1.Set([]byte("key1"), []byte("value1"))
	require.NoError(err)
	err = hraftStore1.SetUint64([]byte("counter"), uint64(42))
	require.NoError(err)

	// Verify the data is stored correctly
	firstIdx, err := hraftStore1.FirstIndex()
	require.NoError(err)
	require.Equal(uint64(1), firstIdx)

	lastIdx, err := hraftStore1.LastIndex()
	require.NoError(err)
	require.Equal(uint64(5), lastIdx)

	// Create a new HraftBadgerStore with the same Badger store and key prefix
	hraftStore2 := NewHraftBadgerStore(badgerStore, keyPrefix)

	// Verify the new store correctly reads the existing first and last index
	firstIdx, err = hraftStore2.FirstIndex()
	require.NoError(err)
	require.Equal(uint64(1), firstIdx)

	lastIdx, err = hraftStore2.LastIndex()
	require.NoError(err)
	require.Equal(uint64(5), lastIdx)

	// Verify all logs can be retrieved
	for i := 1; i <= 5; i++ {
		log := new(raft.Log)
		err = hraftStore2.GetLog(uint64(i), log)
		require.NoError(err)
		require.Equal(fmt.Sprintf("log%d", i), string(log.Data))
		require.Equal(uint64(i), log.Index)
	}

	// Verify stable store data is preserved
	val, err := hraftStore2.Get([]byte("key1"))
	require.NoError(err)
	require.Equal([]byte("value1"), val)

	counter, err := hraftStore2.GetUint64([]byte("counter"))
	require.NoError(err)
	require.Equal(uint64(42), counter)

	// Verify that non-existent data still returns errors
	_, err = hraftStore2.Get([]byte("nonexistent"))
	require.Error(err)
	require.Equal("not found", err.Error())

	err = hraftStore2.GetLog(10, new(raft.Log))
	require.Error(err)
	require.ErrorIs(err, raft.ErrLogNotFound)
}

func TestHraftBadgerStoreNewWithDifferentKeyPrefix(t *testing.T) {
	require := require.New(t)

	// Create a Badger store
	badgerStore := NewBadgerInMemoryStore()

	// Create first HraftBadgerStore with one key prefix and populate it
	hraftStore1 := NewHraftBadgerStore(badgerStore, []byte("prefix1"))

	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
	}
	err := hraftStore1.StoreLogs(logs)
	require.NoError(err)

	err = hraftStore1.Set([]byte("key1"), []byte("value1"))
	require.NoError(err)

	// Create second HraftBadgerStore with different key prefix
	hraftStore2 := NewHraftBadgerStore(badgerStore, []byte("prefix2"))

	// Verify the second store starts with empty state (different prefix)
	firstIdx, err := hraftStore2.FirstIndex()
	require.NoError(err)
	require.Equal(uint64(0), firstIdx)

	lastIdx, err := hraftStore2.LastIndex()
	require.NoError(err)
	require.Equal(uint64(0), lastIdx)

	// Verify stable store data is isolated
	_, err = hraftStore2.Get([]byte("key1"))
	require.Error(err)
	require.Equal("not found", err.Error())

	// Store data in the second store
	logs2 := []*raft.Log{
		testRaftLog(10, "log10"),
		testRaftLog(11, "log11"),
	}
	err = hraftStore2.StoreLogs(logs2)
	require.NoError(err)

	err = hraftStore2.Set([]byte("key2"), []byte("value2"))
	require.NoError(err)

	// Verify both stores maintain their separate data
	firstIdx, err = hraftStore1.FirstIndex()
	require.NoError(err)
	require.Equal(uint64(1), firstIdx)

	lastIdx, err = hraftStore1.LastIndex()
	require.NoError(err)
	require.Equal(uint64(2), lastIdx)

	val, err := hraftStore1.Get([]byte("key1"))
	require.NoError(err)
	require.Equal([]byte("value1"), val)

	// Verify second store data
	firstIdx, err = hraftStore2.FirstIndex()
	require.NoError(err)
	require.Equal(uint64(10), firstIdx)

	lastIdx, err = hraftStore2.LastIndex()
	require.NoError(err)
	require.Equal(uint64(11), lastIdx)

	val, err = hraftStore2.Get([]byte("key2"))
	require.NoError(err)
	require.Equal([]byte("value2"), val)
}

func TestHraftBadgerStoreNewWithEmptyStore(t *testing.T) {
	require := require.New(t)

	// Create a Badger store
	badgerStore := NewBadgerInMemoryStore()
	keyPrefix := []byte("test")

	// Create HraftBadgerStore on empty store
	hraftStore := NewHraftBadgerStore(badgerStore, keyPrefix)

	// Verify it starts with empty state
	firstIdx, err := hraftStore.FirstIndex()
	require.NoError(err)
	require.Equal(uint64(0), firstIdx)

	lastIdx, err := hraftStore.LastIndex()
	require.NoError(err)
	require.Equal(uint64(0), lastIdx)

	// Verify stable store operations work on empty store
	_, err = hraftStore.Get([]byte("nonexistent"))
	require.Error(err)
	require.Equal("not found", err.Error())

	_, err = hraftStore.GetUint64([]byte("nonexistent"))
	require.Error(err)
	require.Equal("not found", err.Error())

	// Verify log operations work on empty store
	err = hraftStore.GetLog(1, new(raft.Log))
	require.Error(err)
	require.ErrorIs(err, raft.ErrLogNotFound)
}
