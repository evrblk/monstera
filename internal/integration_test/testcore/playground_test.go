package testcore

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/store"
)

func TestNewInMemoryPlaygroundCore(t *testing.T) {
	core := NewInMemoryPlaygroundCore()

	require.NotNil(t, core, "NewInMemoryPlaygroundCore returned nil")
	require.NotNil(t, core.state, "NewInMemoryPlaygroundCore state map is nil")
	require.Empty(t, core.state, "NewInMemoryPlaygroundCore should start with empty state")
}

func TestInMemoryPlaygroundCore_Read(t *testing.T) {
	core := NewInMemoryPlaygroundCore()

	// Test reading nonexistent key
	key := uint64(123)
	keyBytes := createKeyBytes(key)

	result, err := core.Read(keyBytes)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Empty(t, result.Data, "Expected empty result for nonexistent key")

	// Test reading existing key
	value := "test value"
	core.state[key] = value

	result, err = core.Read(keyBytes)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, value, string(result.Data), "Expected %s, got %s", value, string(result.Data))
}

func TestInMemoryPlaygroundCore_Update(t *testing.T) {
	core := NewInMemoryPlaygroundCore()

	// Test updating with new key-value pair
	key := uint64(456)
	value := "new value"

	request := createRequestBytes(key, value)

	result, err := core.Update(request)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Check return value
	require.Equal(t, value, string(result.Data), "Expected return value %s, got %s", value, string(result.Data))

	// Check if state was updated
	require.Equal(t, value, core.state[key], "Expected state value %s, got %s", value, core.state[key])

	// Test updating existing key
	newValue := "updated value"
	request = createRequestBytes(key, newValue)

	result, err = core.Update(request)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Equal(t, newValue, string(result.Data), "Expected return value %s, got %s", newValue, string(result.Data))
	require.Equal(t, newValue, core.state[key], "Expected state value %s, got %s", newValue, core.state[key])
}

func TestInMemoryPlaygroundCore_Snapshot(t *testing.T) {
	core := NewInMemoryPlaygroundCore()

	// Add some test data
	core.state[1] = "value1"
	core.state[2] = "value2"
	core.state[3] = "value3"

	snapshot := core.Snapshot()

	require.NotNil(t, snapshot, "Snapshot returned nil")

	// Test that snapshot is independent of original state
	core.state[1] = "modified"

	// Write snapshot to buffer
	var buf bytes.Buffer
	err := snapshot.Write(&buf)
	require.NoError(t, err, "Failed to write snapshot")

	// Create new core and restore from snapshot
	newCore := NewInMemoryPlaygroundCore()
	reader := io.NopCloser(&buf)
	err = newCore.Restore(reader)
	require.NoError(t, err, "Failed to restore snapshot")

	// Verify restored state has original values
	require.Equal(t, "value1", newCore.state[1], "Expected value1, got %s", newCore.state[1])
	require.Equal(t, "value2", newCore.state[2], "Expected value2, got %s", newCore.state[2])
	require.Equal(t, "value3", newCore.state[3], "Expected value3, got %s", newCore.state[3])

	// Verify original core has modified value
	require.Equal(t, "modified", core.state[1], "Expected modified, got %s", core.state[1])
}

func TestInMemoryPlaygroundCore_Restore(t *testing.T) {
	core := NewInMemoryPlaygroundCore()

	// Add initial state
	core.state[1] = "initial"

	// Create snapshot with different data
	snapshotCore := NewInMemoryPlaygroundCore()
	snapshotCore.state[1] = "restored1"
	snapshotCore.state[2] = "restored2"

	snapshot := snapshotCore.Snapshot()

	// Write snapshot to buffer
	var buf bytes.Buffer
	err := snapshot.Write(&buf)
	require.NoError(t, err, "Failed to write snapshot")

	// Restore from snapshot
	reader := io.NopCloser(&buf)
	err = core.Restore(reader)
	require.NoError(t, err, "Failed to restore snapshot")

	// Verify state was replaced
	require.Equal(t, "restored1", core.state[1], "Expected restored1, got %s", core.state[1])
	require.Equal(t, "restored2", core.state[2], "Expected restored2, got %s", core.state[2])

	// Verify old state is gone
	_, exists := core.state[999]
	require.False(t, exists, "Old state should be completely replaced")
}

func TestInMemoryPlaygroundCore_Close(t *testing.T) {
	core := NewInMemoryPlaygroundCore()

	// Add some data
	core.state[1] = "test"

	// Close should not panic
	defer func() {
		if r := recover(); r != nil {
			require.Fail(t, "Close() panicked")
		}
	}()

	core.Close()

	// Close should be idempotent
	core.Close()
}

func TestInMemoryPlaygroundCore_Integration(t *testing.T) {
	core := NewInMemoryPlaygroundCore()

	// Test full workflow: update, read, snapshot, restore
	key := uint64(789)
	value := "integration test value"

	// Update
	request := createRequestBytes(key, value)

	result, err := core.Update(request)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, value, string(result.Data), "Update failed: expected %s, got %s", value, string(result.Data))

	// Read
	keyBytes := createKeyBytes(key)

	result, err = core.Update(request)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, value, string(result.Data), "Read failed: expected %s, got %s", value, string(result.Data))

	// Snapshot
	snapshot := core.Snapshot()

	// Modify state
	core.state[key] = "modified"

	// Write and restore snapshot
	var buf bytes.Buffer
	err = snapshot.Write(&buf)
	require.NoError(t, err, "Failed to write snapshot")

	newCore := NewInMemoryPlaygroundCore()
	reader := io.NopCloser(&buf)
	err = newCore.Restore(reader)
	require.NoError(t, err, "Failed to restore snapshot")

	// Verify restored state
	result2, err := newCore.Read(keyBytes)
	require.NoError(t, err)
	require.NotNil(t, result2)
	require.Equal(t, value, string(result2.Data), "Restored read failed: expected %s, got %s", value, string(result2.Data))
}

func TestInMemoryPlaygroundCore_EmptyState(t *testing.T) {
	core := NewInMemoryPlaygroundCore()

	// Test snapshot of empty state
	snapshot := core.Snapshot()

	var buf bytes.Buffer
	err := snapshot.Write(&buf)
	require.NoError(t, err, "Failed to write empty snapshot")

	// Restore empty state
	newCore := NewInMemoryPlaygroundCore()
	newCore.state[1] = "should be cleared"

	reader := io.NopCloser(&buf)
	err = newCore.Restore(reader)
	require.NoError(t, err, "Failed to restore empty snapshot")

	// Verify state is empty
	require.Empty(t, newCore.state, "Expected empty state after restore, got %d items", len(newCore.state))
}

func TestInMemoryPlaygroundCore_MultipleUpdates(t *testing.T) {
	core := NewInMemoryPlaygroundCore()

	// Test multiple updates
	testData := map[uint64]string{
		1: "first",
		2: "second",
		3: "third",
	}

	for key, value := range testData {
		request := createRequestBytes(key, value)

		result, err := core.Update(request)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, value, string(result.Data), "Update failed for key %d: expected %s, got %s", key, value, string(result.Data))
	}

	// Verify all updates
	for key, expectedValue := range testData {
		keyBytes := createKeyBytes(key)

		result, err := core.Read(keyBytes)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, expectedValue, string(result.Data), "Read failed for key %d: expected %s, got %s", key, expectedValue, string(result.Data))
	}
}

var (
	fullLower  = []byte{0x00, 0x00, 0x00, 0x00}
	fullUpper  = []byte{0xff, 0xff, 0xff, 0xff}
	halfUpper  = []byte{0x7f, 0xff, 0xff, 0xff}
	halfLower  = []byte{0x80, 0x00, 0x00, 0x00}
	coreShards = []string{"parent", "child1", "child2"}
)

// newPlaygroundCore builds one playground core of the given type over the
// given store (ignored for the in-memory flavor). shard picks the exclusive
// prefix; bounds pick the owned key range.
func newPlaygroundCore(coreType monstera.CoreType, s *store.BadgerStore, shard string, lower, upper []byte) monstera.ApplicationCore {
	switch coreType {
	case monstera.CoreTypeInMemory:
		return NewBoundedInMemoryPlaygroundCore(lower, upper)
	case monstera.CoreTypePersistedShared:
		return NewSharedPlaygroundCore(s, lower, upper)
	case monstera.CoreTypePersistedExclusive:
		return NewExclusivePlaygroundCore(s, shard, lower, upper)
	}
	panic("unknown core type")
}

func snapshotBytes(t *testing.T, core monstera.ApplicationCore) []byte {
	t.Helper()
	snap := core.Snapshot()
	defer snap.Release()
	var buf bytes.Buffer
	require.NoError(t, snap.Write(&buf))
	return buf.Bytes()
}

func readValue(t *testing.T, core monstera.ApplicationCore, key uint64) string {
	t.Helper()
	resp, err := core.Read(createKeyBytes(key))
	require.NoError(t, err)
	return string(resp.Data)
}

func updateValue(t *testing.T, core monstera.ApplicationCore, key uint64, value string) {
	t.Helper()
	resp, err := core.Update(createRequestBytes(key, value))
	require.NoError(t, err)
	require.Equal(t, value, string(resp.Data))
}

// TestPlaygroundCores_UpdateReadSnapshotRestore exercises the basic core
// contract — update, read, consistent snapshot, replace-semantics restore —
// uniformly across the three playground flavors.
func TestPlaygroundCores_UpdateReadSnapshotRestore(t *testing.T) {
	for _, coreType := range []monstera.CoreType{
		monstera.CoreTypeInMemory,
		monstera.CoreTypePersistedShared,
		monstera.CoreTypePersistedExclusive,
	} {
		t.Run(coreType.String(), func(t *testing.T) {
			s, err := store.NewBadgerInMemoryStore()
			require.NoError(t, err)
			defer s.Close()

			core := newPlaygroundCore(coreType, s, "parent", nil, nil)

			require.Empty(t, readValue(t, core, 1), "missing key must read empty")
			updateValue(t, core, 1, "one")
			updateValue(t, core, 2, "two")
			require.Equal(t, "one", readValue(t, core, 1))

			// A snapshot is a consistent view: updates after Snapshot() must
			// not leak into its stream.
			snap := core.Snapshot()
			updateValue(t, core, 1, "one-modified")
			updateValue(t, core, 3, "three")
			var buf bytes.Buffer
			require.NoError(t, snap.Write(&buf))
			snap.Release()

			// Restore replaces the state wholesale with the stream.
			require.NoError(t, core.Restore(io.NopCloser(&buf)))
			require.Equal(t, "one", readValue(t, core, 1), "restore must roll back to the snapshot view")
			require.Equal(t, "two", readValue(t, core, 2))
			require.Empty(t, readValue(t, core, 3), "key written after the snapshot must be gone")
		})
	}
}

// TestPlaygroundCores_SplitPartitionOnRestore proves the portable,
// bounds-filtered Restore contract for every flavor: a full-range parent's
// snapshot restored into two half-range children keeps exactly each child's
// half, and the halves partition the parent's data.
func TestPlaygroundCores_SplitPartitionOnRestore(t *testing.T) {
	for _, coreType := range []monstera.CoreType{
		monstera.CoreTypeInMemory,
		monstera.CoreTypePersistedShared,
		monstera.CoreTypePersistedExclusive,
	} {
		t.Run(coreType.String(), func(t *testing.T) {
			s, err := store.NewBadgerInMemoryStore()
			require.NoError(t, err)
			defer s.Close()

			parent := newPlaygroundCore(coreType, s, coreShards[0], fullLower, fullUpper)
			const n = uint64(200)
			for i := uint64(1); i <= n; i++ {
				updateValue(t, parent, i, "v")
			}
			parentSnapshot := snapshotBytes(t, parent)

			// One core per half, over the SAME store for the persisted
			// flavors (the split runs children next to the live parent).
			child1 := newPlaygroundCore(coreType, s, coreShards[1], fullLower, halfUpper)
			child2 := newPlaygroundCore(coreType, s, coreShards[2], halfLower, fullUpper)
			require.NoError(t, child1.Restore(io.NopCloser(bytes.NewReader(parentSnapshot))))
			require.NoError(t, child2.Restore(io.NopCloser(bytes.NewReader(parentSnapshot))))

			// Every key is served by exactly the child owning its shard key.
			c1, c2 := 0, 0
			for i := uint64(1); i <= n; i++ {
				owned1 := ownsPlaygroundKey(fullLower, halfUpper, i)
				v1 := readValue(t, child1, i)
				v2 := readValue(t, child2, i)
				if owned1 {
					require.Equalf(t, "v", v1, "child1 must own key %d", i)
					c1++
				} else {
					require.Equalf(t, "v", v2, "child2 must own key %d", i)
					c2++
				}
				if coreType != monstera.CoreTypePersistedShared {
					// Exclusive/in-memory children hold ONLY their half. (A
					// shared child aliases all range-keyed rows by design, so
					// this assertion does not apply.)
					if owned1 {
						require.Emptyf(t, v2, "child2 must not hold key %d", i)
					} else {
						require.Emptyf(t, v1, "child1 must not hold key %d", i)
					}
				}
			}
			require.Equal(t, int(n), c1+c2, "children must partition the parent's keys")
			require.Positive(t, c1)
			require.Positive(t, c2)

			// The parent still serves everything (its rows are untouched).
			for i := uint64(1); i <= n; i++ {
				require.Equal(t, "v", readValue(t, parent, i))
			}
		})
	}
}
