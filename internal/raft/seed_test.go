package raft

import (
	"bytes"
	"fmt"
	"io"
	"maps"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera/store"
)

// TestSeederClonesRaftGroup is the Phase-1 validation from
// notes/shard-split-design.md: clone a raft group locally through the seeding
// API and prove the pre-baked replica starts as an established member.
//
// Flow (mirroring in-memory-core split seeding): run a source replica, apply
// some commands, take+open a user snapshot at K, apply more commands up to M,
// seed a dormant replica with the snapshot copied verbatim plus the log tail
// (K, M], finalize, then construct a regular Raft over the seeded state — no
// Bootstrap — and verify it elects itself (membership from the base snapshot
// metadata) and reaches the exact source FSM state.
func TestSeederClonesRaftGroup(t *testing.T) {
	raftStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	t.Cleanup(raftStore.Close)
	baseDir := t.TempDir()

	// Source replica: single-voter group with a kv FSM.
	srcCore := newKvAppCore()
	src := NewRaft(baseDir, "node_1", "Core", "s1", "src", srcCore, &nopTransport{}, raftStore, true, 5*time.Second)
	t.Cleanup(func() { _ = src.Close() })
	require.NoError(t, src.Bootstrap([]RaftServer{{ReplicaId: "src", NodeId: "node_1"}}))
	require.Eventually(t, func() bool { return src.GetRaftState() == Leader },
		15*time.Second, 100*time.Millisecond, "source never became leader")

	apply := func(cmd string) {
		_, err := src.Update([]byte(cmd))
		require.NoError(t, err)
	}

	apply("set a 1")
	apply("set b 2")
	apply("set c 3")

	// Base snapshot at K.
	meta, rc, err := src.TakeAndOpenSnapshot()
	require.NoError(t, err)
	content, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	k := meta.Index
	require.Greater(t, k, uint64(0))

	// More commands after K — the tail the seeder must copy.
	apply("set d 4")
	apply("set b 22")
	m := src.GetRaftStats().AppliedIndex
	require.Greater(t, m, k)

	// Seed the clone: base snapshot (parent content, rewritten metadata with
	// the clone's own single-voter membership) + the log tail (K, M].
	seeder := NewSeeder(baseDir, "clone", raftStore)
	require.NoError(t, seeder.SeedBaseSnapshot(k, []RaftServer{{ReplicaId: "clone", NodeId: "node_1"}}, bytes.NewReader(content)))
	for i := k + 1; i <= m; i++ {
		entry, err := src.GetLogEntry(i)
		require.NoError(t, err)
		data := entry.Data
		if !entry.IsCommand {
			// Raft-internal entries are copied as filler the FSM ignores
			// (monstera-level seeding uses framework NOOP commands).
			data = []byte("noop")
		}
		require.NoError(t, seeder.AppendEntries([]SeedEntry{{Index: i, Data: data}}))
	}
	last, err := seeder.LastSeededIndex()
	require.NoError(t, err)
	require.Equal(t, m, last)
	require.NoError(t, seeder.Finalize())

	// Promote in place: a regular Raft over the seeded stores. No Bootstrap.
	cloneCore := newKvAppCore()
	clone := NewRaft(baseDir, "node_1", "Core", "s1", "clone", cloneCore, &nopTransport{}, raftStore, true, 5*time.Second)
	require.Eventually(t, func() bool { return clone.GetRaftState() == Leader },
		15*time.Second, 100*time.Millisecond, "seeded clone never became leader")

	// Membership came from the base snapshot metadata.
	cfg, err := clone.GetConfiguration()
	require.NoError(t, err)
	require.Equal(t, []RaftServer{{ReplicaId: "clone", NodeId: "node_1"}}, cfg)

	// FSM state: base snapshot restored + tail replayed = exact source state.
	require.Eventually(t, func() bool { return clone.GetRaftStats().AppliedIndex >= m },
		15*time.Second, 100*time.Millisecond, "clone never applied the seeded tail")
	require.Equal(t, srcCore.snapshotOf(), cloneCore.snapshotOf())
	require.Equal(t, map[string]string{"a": "1", "b": "22", "c": "3", "d": "4"}, cloneCore.snapshotOf())

	// The clone keeps working as a normal replica.
	_, err = clone.Update([]byte("set e 5"))
	require.NoError(t, err)
	require.Equal(t, "5", cloneCore.get("e"))

	// And survives a restart over the same durable state.
	require.NoError(t, clone.Close())
	cloneCore2 := newKvAppCore()
	clone2 := NewRaft(baseDir, "node_1", "Core", "s1", "clone", cloneCore2, &nopTransport{}, raftStore, true, 5*time.Second)
	t.Cleanup(func() { _ = clone2.Close() })
	require.Eventually(t, func() bool { return clone2.GetRaftState() == Leader },
		15*time.Second, 100*time.Millisecond, "restarted clone never became leader")
	require.Eventually(t, func() bool { return cloneCore2.get("e") == "5" },
		15*time.Second, 100*time.Millisecond, "restarted clone never recovered its state")
}

// TestSeederMetadataOnlyBase is the persisted-core construction: a
// metadata-only base snapshot at index M with an empty log. The promoted
// replica must start with lastApplied = M, replay nothing into the FSM, and
// accept new commands from M+1.
func TestSeederMetadataOnlyBase(t *testing.T) {
	raftStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	t.Cleanup(raftStore.Close)
	baseDir := t.TempDir()

	const m = uint64(42)

	seeder := NewSeeder(baseDir, "child", raftStore)
	require.NoError(t, seeder.SeedBaseSnapshot(m, []RaftServer{{ReplicaId: "child", NodeId: "node_1"}}, nil))
	require.NoError(t, seeder.Finalize())

	core := newKvAppCore()
	// restoreSnapshotOnStart=false: the persisted-core mode; the snapshot
	// content (none) is never restored, only its metadata is read.
	r := NewRaft(baseDir, "node_1", "Core", "s1", "child", core, &nopTransport{}, raftStore, false, 5*time.Second)
	t.Cleanup(func() { _ = r.Close() })

	require.Eventually(t, func() bool { return r.GetRaftState() == Leader },
		15*time.Second, 100*time.Millisecond, "seeded replica never became leader")

	stats := r.GetRaftStats()
	require.Equal(t, m, stats.LastSnapshotIndex)
	require.GreaterOrEqual(t, stats.AppliedIndex, m)
	require.EqualValues(t, 0, core.applyCount(), "no entries may be replayed into a metadata-only-seeded FSM")
	require.EqualValues(t, 0, core.restoreCount(), "metadata-only snapshot content must never be restored")

	// New commands land after M.
	_, err = r.Update([]byte("set x 9"))
	require.NoError(t, err)
	require.EqualValues(t, 1, core.applyCount())
	require.Equal(t, "9", core.get("x"))
	require.Greater(t, r.GetRaftStats().AppliedIndex, m)
}

func TestSeederAppendEntriesContiguity(t *testing.T) {
	raftStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	t.Cleanup(raftStore.Close)

	seeder := NewSeeder(t.TempDir(), "r1", raftStore)

	// Internal gap.
	err = seeder.AppendEntries([]SeedEntry{{Index: 5, Data: []byte("a")}, {Index: 7, Data: []byte("b")}})
	require.ErrorIs(t, err, ErrSeedGap)

	// First batch can start anywhere (the caller aligns it with the base).
	require.NoError(t, seeder.AppendEntries([]SeedEntry{{Index: 5, Data: []byte("a")}, {Index: 6, Data: []byte("b")}}))

	// Gap against the already-seeded log.
	err = seeder.AppendEntries([]SeedEntry{{Index: 8, Data: []byte("c")}})
	require.ErrorIs(t, err, ErrSeedGap)

	// Contiguous append and idempotent re-append.
	require.NoError(t, seeder.AppendEntries([]SeedEntry{{Index: 7, Data: []byte("c")}}))
	require.NoError(t, seeder.AppendEntries([]SeedEntry{{Index: 7, Data: []byte("c")}}))

	last, err := seeder.LastSeededIndex()
	require.NoError(t, err)
	require.EqualValues(t, 7, last)

	// Empty batch is a no-op.
	require.NoError(t, seeder.AppendEntries(nil))
}

func TestSeederCatchUpIndex(t *testing.T) {
	raftStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	t.Cleanup(raftStore.Close)

	seeder := NewSeeder(t.TempDir(), "r1", raftStore)

	v, err := seeder.CatchUpIndex()
	require.NoError(t, err)
	require.EqualValues(t, 0, v)

	require.NoError(t, seeder.SetCatchUpIndex(123))
	v, err = seeder.CatchUpIndex()
	require.NoError(t, err)
	require.EqualValues(t, 123, v)
}

func TestSeederBaseSnapshotValidation(t *testing.T) {
	raftStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	t.Cleanup(raftStore.Close)

	seeder := NewSeeder(t.TempDir(), "r1", raftStore)
	require.Error(t, seeder.SeedBaseSnapshot(0, []RaftServer{{ReplicaId: "r1", NodeId: "node_1"}}, nil))
	require.Error(t, seeder.SeedBaseSnapshot(10, nil, nil))
}

// kvAppCore is a minimal in-memory kv FSM for seeding tests. Commands are
// "set <key> <value>"; anything else (e.g. "noop" filler) is ignored.
// Snapshots are sorted "key=value" lines.
type kvAppCore struct {
	mu       sync.Mutex
	data     map[string]string
	applies  int
	restores int
}

func newKvAppCore() *kvAppCore {
	return &kvAppCore{data: make(map[string]string)}
}

func (c *kvAppCore) Apply(index uint64, request []byte) any {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.applies++
	parts := strings.Fields(string(request))
	if len(parts) == 3 && parts[0] == "set" {
		c.data[parts[1]] = parts[2]
	}
	return nil
}

func (c *kvAppCore) Snapshot() AppCoreSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Copy under lock: Write runs concurrently with subsequent applies.
	return &kvAppSnapshot{data: maps.Clone(c.data)}
}

func (c *kvAppCore) Restore(reader io.ReadCloser) error {
	defer reader.Close()

	content, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.restores++
	c.data = make(map[string]string)
	for _, line := range strings.Split(string(content), "\n") {
		if k, v, ok := strings.Cut(line, "="); ok {
			c.data[k] = v
		}
	}
	return nil
}

func (c *kvAppCore) get(key string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data[key]
}

func (c *kvAppCore) snapshotOf() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.data)
}

func (c *kvAppCore) applyCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.applies
}

func (c *kvAppCore) restoreCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.restores
}

type kvAppSnapshot struct {
	data map[string]string
}

func (s *kvAppSnapshot) Write(w io.Writer) error {
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if _, err := fmt.Fprintf(w, "%s=%s\n", k, s.data[k]); err != nil {
			return err
		}
	}
	return nil
}

func (s *kvAppSnapshot) Release() {}
