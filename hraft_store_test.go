package monstera

import (
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
