package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"testing"

	"github.com/evrblk/monstera/store"
	hraft "github.com/hashicorp/raft"
	raftbench "github.com/hashicorp/raft/bench"
	"github.com/stretchr/testify/require"
)

// gobLogCodec is a test-only LogCodec that uses encoding/gob.
type gobLogCodec struct{}

func (c *gobLogCodec) Encode(log *hraft.Log) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(log); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *gobLogCodec) Decode(data []byte, out *hraft.Log) error {
	dec := gob.NewDecoder(bytes.NewReader(data))
	return dec.Decode(out)
}

func newTestStore(t testing.TB, prefix string) *HraftBadgerStore {
	t.Helper()
	return NewHraftBadgerStore(store.NewBadgerInMemoryStore(), []byte(prefix), &gobLogCodec{})
}

func testRaftLog(idx uint64, data string) *hraft.Log {
	return &hraft.Log{
		Data:  []byte(data),
		Index: idx,
	}
}

func TestHraftBadgerStoreFirstIndex(t *testing.T) {
	hraftStore := newTestStore(t, "test")

	idx, err := hraftStore.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), idx)

	logs := []*hraft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	err = hraftStore.StoreLogs(logs)
	require.NoError(t, err)

	idx, err = hraftStore.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), idx)
}

func TestHraftBadgerStoreLastIndex(t *testing.T) {
	hraftStore := newTestStore(t, "test")

	idx, err := hraftStore.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), idx)

	logs := []*hraft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	err = hraftStore.StoreLogs(logs)
	require.NoError(t, err)

	idx, err = hraftStore.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(3), idx)
}

func TestHraftBadgerStoreGetLog(t *testing.T) {
	hraftStore := newTestStore(t, "test")

	log := new(hraft.Log)

	err := hraftStore.GetLog(1, log)
	require.Error(t, err)
	require.ErrorIs(t, err, hraft.ErrLogNotFound)

	logs := []*hraft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	err = hraftStore.StoreLogs(logs)
	require.NoError(t, err)

	err = hraftStore.GetLog(2, log)
	require.NoError(t, err)
	require.Equal(t, log, logs[1])
}

func TestHraftBadgerStoreSetLog(t *testing.T) {
	hraftStore := newTestStore(t, "test")

	log := &hraft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}

	err := hraftStore.StoreLog(log)
	require.NoError(t, err)

	result := new(hraft.Log)
	err = hraftStore.GetLog(1, result)
	require.NoError(t, err)
	require.Equal(t, log, result)
}

func TestHraftBadgerStoreSetLogs(t *testing.T) {
	hraftStore := newTestStore(t, "test")

	logs := []*hraft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
	}

	err := hraftStore.StoreLogs(logs)
	require.NoError(t, err)

	result1, result2 := new(hraft.Log), new(hraft.Log)
	err = hraftStore.GetLog(1, result1)
	require.NoError(t, err)
	require.Equal(t, logs[0], result1)
	err = hraftStore.GetLog(2, result2)
	require.NoError(t, err)
	require.Equal(t, logs[1], result2)
}

func TestHraftBadgerStoreDeleteRange(t *testing.T) {
	hraftStore := newTestStore(t, "test")

	logs := []*hraft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}

	err := hraftStore.StoreLogs(logs)
	require.NoError(t, err)

	err = hraftStore.DeleteRange(1, 2)
	require.NoError(t, err)

	err = hraftStore.GetLog(1, new(hraft.Log))
	require.Error(t, err)
	require.ErrorIs(t, err, hraft.ErrLogNotFound)
	err = hraftStore.GetLog(2, new(hraft.Log))
	require.Error(t, err)
	require.ErrorIs(t, err, hraft.ErrLogNotFound)
}

func TestHraftBadgerStoreDeleteRangeEmptyStore(t *testing.T) {
	hraftStore := newTestStore(t, "test")

	err := hraftStore.DeleteRange(1, 10)
	require.NoError(t, err)

	firstIdx, err := hraftStore.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), firstIdx)

	lastIdx, err := hraftStore.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastIdx)
}

func TestHraftBadgerStoreDeleteRangeSingleLog(t *testing.T) {
	hraftStore := newTestStore(t, "test")

	log := testRaftLog(5, "single_log")
	err := hraftStore.StoreLog(log)
	require.NoError(t, err)

	err = hraftStore.DeleteRange(5, 5)
	require.NoError(t, err)

	err = hraftStore.GetLog(5, new(hraft.Log))
	require.Error(t, err)
	require.ErrorIs(t, err, hraft.ErrLogNotFound)

	firstIdx, err := hraftStore.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), firstIdx)

	lastIdx, err := hraftStore.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastIdx)
}

func TestHraftBadgerStoreDeleteRangePartial(t *testing.T) {
	hraftStore := newTestStore(t, "test")

	logs := make([]*hraft.Log, 10)
	for i := 1; i <= 10; i++ {
		logs[i-1] = testRaftLog(uint64(i), fmt.Sprintf("log%d", i))
	}
	err := hraftStore.StoreLogs(logs)
	require.NoError(t, err)

	err = hraftStore.DeleteRange(3, 7)
	require.NoError(t, err)

	for i := 3; i <= 7; i++ {
		err = hraftStore.GetLog(uint64(i), new(hraft.Log))
		require.Error(t, err)
		require.ErrorIs(t, err, hraft.ErrLogNotFound)
	}

	for i := 1; i <= 2; i++ {
		log := new(hraft.Log)
		err = hraftStore.GetLog(uint64(i), log)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("log%d", i), string(log.Data))
	}

	for i := 8; i <= 10; i++ {
		log := new(hraft.Log)
		err = hraftStore.GetLog(uint64(i), log)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("log%d", i), string(log.Data))
	}

	firstIdx, err := hraftStore.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), firstIdx)

	lastIdx, err := hraftStore.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(10), lastIdx)
}

func TestHraftBadgerStoreDeleteRangeFromBeginning(t *testing.T) {
	hraftStore := newTestStore(t, "test")

	logs := make([]*hraft.Log, 10)
	for i := 1; i <= 10; i++ {
		logs[i-1] = testRaftLog(uint64(i), fmt.Sprintf("log%d", i))
	}
	err := hraftStore.StoreLogs(logs)
	require.NoError(t, err)

	err = hraftStore.DeleteRange(1, 5)
	require.NoError(t, err)

	for i := 1; i <= 5; i++ {
		err = hraftStore.GetLog(uint64(i), new(hraft.Log))
		require.Error(t, err)
		require.ErrorIs(t, err, hraft.ErrLogNotFound)
	}

	for i := 6; i <= 10; i++ {
		log := new(hraft.Log)
		err = hraftStore.GetLog(uint64(i), log)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("log%d", i), string(log.Data))
	}

	firstIdx, err := hraftStore.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(6), firstIdx)

	lastIdx, err := hraftStore.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(10), lastIdx)
}

func TestHraftBadgerStoreDeleteRangeToEnd(t *testing.T) {
	hraftStore := newTestStore(t, "test")

	logs := make([]*hraft.Log, 10)
	for i := 1; i <= 10; i++ {
		logs[i-1] = testRaftLog(uint64(i), fmt.Sprintf("log%d", i))
	}
	err := hraftStore.StoreLogs(logs)
	require.NoError(t, err)

	err = hraftStore.DeleteRange(6, 10)
	require.NoError(t, err)

	for i := 6; i <= 10; i++ {
		err = hraftStore.GetLog(uint64(i), new(hraft.Log))
		require.Error(t, err)
		require.ErrorIs(t, err, hraft.ErrLogNotFound)
	}

	for i := 1; i <= 5; i++ {
		log := new(hraft.Log)
		err = hraftStore.GetLog(uint64(i), log)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("log%d", i), string(log.Data))
	}

	firstIdx, err := hraftStore.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), firstIdx)

	lastIdx, err := hraftStore.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(5), lastIdx)
}

func TestHraftBadgerStoreDeleteRangeEntireStore(t *testing.T) {
	hraftStore := newTestStore(t, "test")

	logs := make([]*hraft.Log, 10)
	for i := 1; i <= 10; i++ {
		logs[i-1] = testRaftLog(uint64(i), fmt.Sprintf("log%d", i))
	}
	err := hraftStore.StoreLogs(logs)
	require.NoError(t, err)

	err = hraftStore.DeleteRange(1, 10)
	require.NoError(t, err)

	for i := 1; i <= 10; i++ {
		err = hraftStore.GetLog(uint64(i), new(hraft.Log))
		require.Error(t, err)
		require.ErrorIs(t, err, hraft.ErrLogNotFound)
	}

	firstIdx, err := hraftStore.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), firstIdx)

	lastIdx, err := hraftStore.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastIdx)
}

func TestHraftBadgerStoreDeleteRangeNonExistent(t *testing.T) {
	hraftStore := newTestStore(t, "test")

	logs := []*hraft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(3, "log3"),
		testRaftLog(5, "log5"),
	}
	err := hraftStore.StoreLogs(logs)
	require.NoError(t, err)

	err = hraftStore.DeleteRange(2, 4)
	require.NoError(t, err)

	err = hraftStore.GetLog(2, new(hraft.Log))
	require.Error(t, err)
	require.ErrorIs(t, err, hraft.ErrLogNotFound)

	err = hraftStore.GetLog(4, new(hraft.Log))
	require.Error(t, err)
	require.ErrorIs(t, err, hraft.ErrLogNotFound)

	for _, idx := range []uint64{1, 5} {
		log := new(hraft.Log)
		err = hraftStore.GetLog(idx, log)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("log%d", idx), string(log.Data))
	}

	firstIdx, err := hraftStore.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), firstIdx)

	lastIdx, err := hraftStore.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(5), lastIdx)
}

func TestHraftBadgerStoreDeleteRangeInvalidRange(t *testing.T) {
	hraftStore := newTestStore(t, "test")

	logs := []*hraft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	err := hraftStore.StoreLogs(logs)
	require.NoError(t, err)

	err = hraftStore.DeleteRange(5, 3)
	require.NoError(t, err)

	for i := 1; i <= 3; i++ {
		log := new(hraft.Log)
		err = hraftStore.GetLog(uint64(i), log)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("log%d", i), string(log.Data))
	}
}

func TestHraftBadgerStoreSetAndGet(t *testing.T) {
	hraftStore := newTestStore(t, "test")

	_, err := hraftStore.Get([]byte("bad"))
	require.Error(t, err)
	require.Equal(t, "not found", err.Error())

	k, v := []byte("hello"), []byte("world")

	err = hraftStore.Set(k, v)
	require.NoError(t, err)

	val, err := hraftStore.Get(k)
	require.NoError(t, err)
	require.Equal(t, v, val)
}

func TestHraftBadgerStoreSetUint64AndGetUint64(t *testing.T) {
	hraftStore := newTestStore(t, "test")

	_, err := hraftStore.GetUint64([]byte("bad"))
	require.Error(t, err)
	require.Equal(t, "not found", err.Error())

	k, v := []byte("abc"), uint64(123)

	err = hraftStore.SetUint64(k, v)
	require.NoError(t, err)

	val, err := hraftStore.GetUint64(k)
	require.NoError(t, err)
	require.Equal(t, v, val)
}

func TestHraftBadgerStoreNewWithExistingData(t *testing.T) {
	badgerStore := store.NewBadgerInMemoryStore()
	codec := &gobLogCodec{}
	keyPrefix := []byte("test")

	hraftStore1 := NewHraftBadgerStore(badgerStore, keyPrefix, codec)

	logs := []*hraft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
		testRaftLog(4, "log4"),
		testRaftLog(5, "log5"),
	}
	err := hraftStore1.StoreLogs(logs)
	require.NoError(t, err)

	err = hraftStore1.Set([]byte("key1"), []byte("value1"))
	require.NoError(t, err)
	err = hraftStore1.SetUint64([]byte("counter"), uint64(42))
	require.NoError(t, err)

	firstIdx, err := hraftStore1.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), firstIdx)

	lastIdx, err := hraftStore1.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(5), lastIdx)

	hraftStore2 := NewHraftBadgerStore(badgerStore, keyPrefix, codec)

	firstIdx, err = hraftStore2.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), firstIdx)

	lastIdx, err = hraftStore2.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(5), lastIdx)

	for i := 1; i <= 5; i++ {
		log := new(hraft.Log)
		err = hraftStore2.GetLog(uint64(i), log)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("log%d", i), string(log.Data))
		require.Equal(t, uint64(i), log.Index)
	}

	val, err := hraftStore2.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val)

	counter, err := hraftStore2.GetUint64([]byte("counter"))
	require.NoError(t, err)
	require.Equal(t, uint64(42), counter)

	_, err = hraftStore2.Get([]byte("nonexistent"))
	require.Error(t, err)
	require.Equal(t, "not found", err.Error())

	err = hraftStore2.GetLog(10, new(hraft.Log))
	require.Error(t, err)
	require.ErrorIs(t, err, hraft.ErrLogNotFound)
}

func TestHraftBadgerStoreNewWithDifferentKeyPrefix(t *testing.T) {
	badgerStore := store.NewBadgerInMemoryStore()
	codec := &gobLogCodec{}

	hraftStore1 := NewHraftBadgerStore(badgerStore, []byte("prefix1"), codec)

	logs := []*hraft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
	}
	err := hraftStore1.StoreLogs(logs)
	require.NoError(t, err)

	err = hraftStore1.Set([]byte("key1"), []byte("value1"))
	require.NoError(t, err)

	hraftStore2 := NewHraftBadgerStore(badgerStore, []byte("prefix2"), codec)

	firstIdx, err := hraftStore2.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), firstIdx)

	lastIdx, err := hraftStore2.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastIdx)

	_, err = hraftStore2.Get([]byte("key1"))
	require.Error(t, err)
	require.Equal(t, "not found", err.Error())

	logs2 := []*hraft.Log{
		testRaftLog(10, "log10"),
		testRaftLog(11, "log11"),
	}
	err = hraftStore2.StoreLogs(logs2)
	require.NoError(t, err)

	err = hraftStore2.Set([]byte("key2"), []byte("value2"))
	require.NoError(t, err)

	firstIdx, err = hraftStore1.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), firstIdx)

	lastIdx, err = hraftStore1.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(2), lastIdx)

	val, err := hraftStore1.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val)

	firstIdx, err = hraftStore2.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(10), firstIdx)

	lastIdx, err = hraftStore2.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(11), lastIdx)

	val, err = hraftStore2.Get([]byte("key2"))
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), val)
}

func TestHraftBadgerStoreNewWithEmptyStore(t *testing.T) {
	badgerStore := store.NewBadgerInMemoryStore()
	keyPrefix := []byte("test")

	hraftStore := NewHraftBadgerStore(badgerStore, keyPrefix, &gobLogCodec{})

	firstIdx, err := hraftStore.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), firstIdx)

	lastIdx, err := hraftStore.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastIdx)

	_, err = hraftStore.Get([]byte("nonexistent"))
	require.Error(t, err)
	require.Equal(t, "not found", err.Error())

	_, err = hraftStore.GetUint64([]byte("nonexistent"))
	require.Error(t, err)
	require.Equal(t, "not found", err.Error())

	err = hraftStore.GetLog(1, new(hraft.Log))
	require.Error(t, err)
	require.ErrorIs(t, err, hraft.ErrLogNotFound)
}

// Benchmarks

func BenchmarkHraftBadgerStoreFirstIndex(b *testing.B) {
	dir, err := os.MkdirTemp("", "hraft-badger-store")
	if err != nil {
		b.Error(err)
	}
	defer os.RemoveAll(dir)
	badgerStore := store.NewBadgerStore(dir)
	defer badgerStore.Close()
	hraftStore := NewHraftBadgerStore(badgerStore, []byte("test"), &gobLogCodec{})

	raftbench.FirstIndex(b, hraftStore)
}

func BenchmarkHraftBadgerStoreLastIndex(b *testing.B) {
	dir, err := os.MkdirTemp("", "hraft-badger-store")
	if err != nil {
		b.Error(err)
	}
	defer os.RemoveAll(dir)
	badgerStore := store.NewBadgerStore(dir)
	defer badgerStore.Close()
	hraftStore := NewHraftBadgerStore(badgerStore, []byte("test"), &gobLogCodec{})

	raftbench.LastIndex(b, hraftStore)
}

func BenchmarkHraftBadgerStoreGetLog(b *testing.B) {
	dir, err := os.MkdirTemp("", "hraft-badger-store")
	if err != nil {
		b.Error(err)
	}
	defer os.RemoveAll(dir)
	badgerStore := store.NewBadgerStore(dir)
	defer badgerStore.Close()
	hraftStore := NewHraftBadgerStore(badgerStore, []byte("test"), &gobLogCodec{})

	raftbench.GetLog(b, hraftStore)
}

func BenchmarkHraftBadgerStoreStoreLog(b *testing.B) {
	dir, err := os.MkdirTemp("", "hraft-badger-store")
	if err != nil {
		b.Error(err)
	}
	defer os.RemoveAll(dir)
	badgerStore := store.NewBadgerStore(dir)
	defer badgerStore.Close()
	hraftStore := NewHraftBadgerStore(badgerStore, []byte("test"), &gobLogCodec{})

	raftbench.StoreLog(b, hraftStore)
}

func BenchmarkHraftBadgerStoreStoreLogs(b *testing.B) {
	dir, err := os.MkdirTemp("", "hraft-badger-store")
	if err != nil {
		b.Error(err)
	}
	defer os.RemoveAll(dir)
	badgerStore := store.NewBadgerStore(dir)
	defer badgerStore.Close()
	hraftStore := NewHraftBadgerStore(badgerStore, []byte("test"), &gobLogCodec{})

	raftbench.StoreLogs(b, hraftStore)
}

func BenchmarkHraftBadgerStoreDeleteRange(b *testing.B) {
	dir, err := os.MkdirTemp("", "hraft-badger-store")
	if err != nil {
		b.Error(err)
	}
	defer os.RemoveAll(dir)
	badgerStore := store.NewBadgerStore(dir)
	defer badgerStore.Close()
	hraftStore := NewHraftBadgerStore(badgerStore, []byte("test"), &gobLogCodec{})

	raftbench.DeleteRange(b, hraftStore)
}

func BenchmarkHraftBadgerStoreGet(b *testing.B) {
	dir, err := os.MkdirTemp("", "hraft-badger-store")
	if err != nil {
		b.Error(err)
	}
	defer os.RemoveAll(dir)
	badgerStore := store.NewBadgerStore(dir)
	defer badgerStore.Close()
	hraftStore := NewHraftBadgerStore(badgerStore, []byte("test"), &gobLogCodec{})

	raftbench.Get(b, hraftStore)
}

func BenchmarkHraftBadgerStoreSet(b *testing.B) {
	dir, err := os.MkdirTemp("", "hraft-badger-store")
	if err != nil {
		b.Error(err)
	}
	defer os.RemoveAll(dir)
	badgerStore := store.NewBadgerStore(dir)
	defer badgerStore.Close()
	hraftStore := NewHraftBadgerStore(badgerStore, []byte("test"), &gobLogCodec{})

	raftbench.Set(b, hraftStore)
}

func BenchmarkHraftBadgerStoreSetUint64(b *testing.B) {
	dir, err := os.MkdirTemp("", "hraft-badger-store")
	if err != nil {
		b.Error(err)
	}
	defer os.RemoveAll(dir)
	badgerStore := store.NewBadgerStore(dir)
	defer badgerStore.Close()
	hraftStore := NewHraftBadgerStore(badgerStore, []byte("test"), &gobLogCodec{})

	raftbench.SetUint64(b, hraftStore)
}

func BenchmarkHraftBadgerStoreGetUint64(b *testing.B) {
	dir, err := os.MkdirTemp("", "hraft-badger-store")
	if err != nil {
		b.Error(err)
	}
	defer os.RemoveAll(dir)
	badgerStore := store.NewBadgerStore(dir)
	defer badgerStore.Close()
	hraftStore := NewHraftBadgerStore(badgerStore, []byte("test"), &gobLogCodec{})

	raftbench.GetUint64(b, hraftStore)
}
