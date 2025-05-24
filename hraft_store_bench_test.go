package monstera

import (
	"os"
	"testing"

	raftbench "github.com/hashicorp/raft/bench"
)

func BenchmarkHraftBadgerStoreFirstIndex(b *testing.B) {
	dir, err := os.MkdirTemp("", "hraft-badger-store")
	if err != nil {
		b.Error(err)
	}
	defer os.RemoveAll(dir)
	badgerStore := NewBadgerStore(dir)
	defer badgerStore.Close()
	hraftStore := NewHraftBadgerStore(badgerStore, []byte("test"))

	raftbench.FirstIndex(b, hraftStore)
}

func BenchmarkHraftBadgerStoreLastIndex(b *testing.B) {
	dir, err := os.MkdirTemp("", "hraft-badger-store")
	if err != nil {
		b.Error(err)
	}
	defer os.RemoveAll(dir)
	badgerStore := NewBadgerStore(dir)
	defer badgerStore.Close()
	hraftStore := NewHraftBadgerStore(badgerStore, []byte("test"))

	raftbench.LastIndex(b, hraftStore)
}

func BenchmarkHraftBadgerStoreGetLog(b *testing.B) {
	dir, err := os.MkdirTemp("", "hraft-badger-store")
	if err != nil {
		b.Error(err)
	}
	defer os.RemoveAll(dir)
	badgerStore := NewBadgerStore(dir)
	defer badgerStore.Close()
	hraftStore := NewHraftBadgerStore(badgerStore, []byte("test"))

	raftbench.GetLog(b, hraftStore)
}

func BenchmarkHraftBadgerStoreStoreLog(b *testing.B) {
	dir, err := os.MkdirTemp("", "hraft-badger-store")
	if err != nil {
		b.Error(err)
	}
	defer os.RemoveAll(dir)
	badgerStore := NewBadgerStore(dir)
	defer badgerStore.Close()
	hraftStore := NewHraftBadgerStore(badgerStore, []byte("test"))

	raftbench.StoreLog(b, hraftStore)
}

func BenchmarkHraftBadgerStoreStoreLogs(b *testing.B) {
	dir, err := os.MkdirTemp("", "hraft-badger-store")
	if err != nil {
		b.Error(err)
	}
	defer os.RemoveAll(dir)
	badgerStore := NewBadgerStore(dir)
	defer badgerStore.Close()
	hraftStore := NewHraftBadgerStore(badgerStore, []byte("test"))

	raftbench.StoreLogs(b, hraftStore)
}

func BenchmarkHraftBadgerStoreDeleteRange(b *testing.B) {
	dir, err := os.MkdirTemp("", "hraft-badger-store")
	if err != nil {
		b.Error(err)
	}
	defer os.RemoveAll(dir)
	badgerStore := NewBadgerStore(dir)
	defer badgerStore.Close()
	hraftStore := NewHraftBadgerStore(badgerStore, []byte("test"))

	raftbench.DeleteRange(b, hraftStore)
}

func BenchmarkHraftBadgerStoreSet(b *testing.B) {
	dir, err := os.MkdirTemp("", "hraft-badger-store")
	if err != nil {
		b.Error(err)
	}
	defer os.RemoveAll(dir)
	badgerStore := NewBadgerStore(dir)
	defer badgerStore.Close()
	hraftStore := NewHraftBadgerStore(badgerStore, []byte("test"))

	raftbench.Set(b, hraftStore)
}

func BenchmarkHraftBadgerStoreGet(b *testing.B) {
	dir, err := os.MkdirTemp("", "hraft-badger-store")
	if err != nil {
		b.Error(err)
	}
	defer os.RemoveAll(dir)
	badgerStore := NewBadgerStore(dir)
	defer badgerStore.Close()
	hraftStore := NewHraftBadgerStore(badgerStore, []byte("test"))

	raftbench.Get(b, hraftStore)
}

func BenchmarkHraftBadgerStoreSetUint64(b *testing.B) {
	dir, err := os.MkdirTemp("", "hraft-badger-store")
	if err != nil {
		b.Error(err)
	}
	defer os.RemoveAll(dir)
	badgerStore := NewBadgerStore(dir)
	defer badgerStore.Close()
	hraftStore := NewHraftBadgerStore(badgerStore, []byte("test"))

	raftbench.SetUint64(b, hraftStore)
}

func BenchmarkHraftBadgerStoreGetUint64(b *testing.B) {
	dir, err := os.MkdirTemp("", "hraft-badger-store")
	if err != nil {
		b.Error(err)
	}
	defer os.RemoveAll(dir)
	badgerStore := NewBadgerStore(dir)
	defer badgerStore.Close()
	hraftStore := NewHraftBadgerStore(badgerStore, []byte("test"))

	raftbench.GetUint64(b, hraftStore)
}
