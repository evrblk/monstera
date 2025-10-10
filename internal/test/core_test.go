package test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewPlaygroundCore(t *testing.T) {
	core := NewPlaygroundCore()

	require.NotNil(t, core, "NewPlaygroundCore returned nil")
	require.NotNil(t, core.state, "NewPlaygroundCore state map is nil")
	require.Empty(t, core.state, "NewPlaygroundCore should start with empty state")
}

func TestPlaygroundCore_Read(t *testing.T) {
	core := NewPlaygroundCore()

	// Test reading non-existent key
	key := uint64(123)
	keyBytes := createKeyBytes(key)

	result := core.Read(keyBytes)
	require.Empty(t, result, "Expected empty result for non-existent key")

	// Test reading existing key
	value := "test value"
	core.state[key] = value

	result = core.Read(keyBytes)
	require.Equal(t, value, string(result), "Expected %s, got %s", value, string(result))
}

func TestPlaygroundCore_Update(t *testing.T) {
	core := NewPlaygroundCore()

	// Test updating with new key-value pair
	key := uint64(456)
	value := "new value"

	request := createRequestBytes(key, value)

	result := core.Update(request)

	// Check return value
	require.Equal(t, value, string(result), "Expected return value %s, got %s", value, string(result))

	// Check if state was updated
	require.Equal(t, value, core.state[key], "Expected state value %s, got %s", value, core.state[key])

	// Test updating existing key
	newValue := "updated value"
	request = createRequestBytes(key, newValue)

	result = core.Update(request)

	require.Equal(t, newValue, string(result), "Expected return value %s, got %s", newValue, string(result))
	require.Equal(t, newValue, core.state[key], "Expected state value %s, got %s", newValue, core.state[key])
}

func TestPlaygroundCore_Snapshot(t *testing.T) {
	core := NewPlaygroundCore()

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
	newCore := NewPlaygroundCore()
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

func TestPlaygroundCore_Restore(t *testing.T) {
	core := NewPlaygroundCore()

	// Add initial state
	core.state[1] = "initial"

	// Create snapshot with different data
	snapshotCore := NewPlaygroundCore()
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

func TestPlaygroundCore_Close(t *testing.T) {
	core := NewPlaygroundCore()

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

func TestPlaygroundCore_Integration(t *testing.T) {
	core := NewPlaygroundCore()

	// Test full workflow: update, read, snapshot, restore
	key := uint64(789)
	value := "integration test value"

	// Update
	request := createRequestBytes(key, value)

	result := core.Update(request)
	require.Equal(t, value, string(result), "Update failed: expected %s, got %s", value, string(result))

	// Read
	keyBytes := createKeyBytes(key)

	result = core.Read(keyBytes)
	require.Equal(t, value, string(result), "Read failed: expected %s, got %s", value, string(result))

	// Snapshot
	snapshot := core.Snapshot()

	// Modify state
	core.state[key] = "modified"

	// Write and restore snapshot
	var buf bytes.Buffer
	err := snapshot.Write(&buf)
	require.NoError(t, err, "Failed to write snapshot")

	newCore := NewPlaygroundCore()
	reader := io.NopCloser(&buf)
	err = newCore.Restore(reader)
	require.NoError(t, err, "Failed to restore snapshot")

	// Verify restored state
	result = newCore.Read(keyBytes)
	require.Equal(t, value, string(result), "Restored read failed: expected %s, got %s", value, string(result))
}

func TestPlaygroundCore_EmptyState(t *testing.T) {
	core := NewPlaygroundCore()

	// Test snapshot of empty state
	snapshot := core.Snapshot()

	var buf bytes.Buffer
	err := snapshot.Write(&buf)
	require.NoError(t, err, "Failed to write empty snapshot")

	// Restore empty state
	newCore := NewPlaygroundCore()
	newCore.state[1] = "should be cleared"

	reader := io.NopCloser(&buf)
	err = newCore.Restore(reader)
	require.NoError(t, err, "Failed to restore empty snapshot")

	// Verify state is empty
	require.Empty(t, newCore.state, "Expected empty state after restore, got %d items", len(newCore.state))
}

func TestPlaygroundCore_MultipleUpdates(t *testing.T) {
	core := NewPlaygroundCore()

	// Test multiple updates
	testData := map[uint64]string{
		1: "first",
		2: "second",
		3: "third",
	}

	for key, value := range testData {
		request := createRequestBytes(key, value)

		result := core.Update(request)
		require.Equal(t, value, string(result), "Update failed for key %d: expected %s, got %s", key, value, string(result))
	}

	// Verify all updates
	for key, expectedValue := range testData {
		keyBytes := createKeyBytes(key)

		result := core.Read(keyBytes)
		require.Equal(t, expectedValue, string(result), "Read failed for key %d: expected %s, got %s", key, expectedValue, string(result))
	}
}
