package monsterax

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
)

// testMessage is a simple type that implements BinaryMarshaler/BinaryUnmarshaler for testing
type testMessage struct {
	Data string
}

func (m *testMessage) MarshalBinary() ([]byte, error) {
	return []byte(m.Data), nil
}

func (m *testMessage) UnmarshalBinary(data []byte) error {
	m.Data = string(data)
	return nil
}

// setupTestStore creates an in-memory store for testing
func setupTestStore(t *testing.T) *store.BadgerStore {
	s, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	return s
}

// =============================================================================
// BinaryTable Tests
// =============================================================================

func TestBinaryTable_Get_Set_Delete(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("BINARY1")
	table := NewBinaryTable[*testMessage, testMessage](tableId, []byte{0x10}, []byte{0x20})

	txn := s.Update()
	defer txn.Discard()

	// Test Set and Get
	key := []byte{0x15, 0x01}
	msg := &testMessage{Data: "test-message"}
	err := table.Set(txn, key, msg)
	require.NoError(t, err)

	retrieved, err := table.Get(txn, key)
	require.NoError(t, err)
	require.Equal(t, msg.Data, retrieved.Data)

	// Test Delete
	err = table.Delete(txn, key)
	require.NoError(t, err)

	_, err = table.Get(txn, key)
	require.ErrorIs(t, err, store.ErrNotFound)
}

func TestBinaryTable_ListAll(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("BINARY1")
	table := NewBinaryTable[*testMessage, testMessage](tableId, []byte{0x10}, []byte{0x20})

	txn := s.Update()
	defer txn.Discard()

	// Set multiple values with a common prefix
	prefix := []byte{0x15}
	for i := 0; i < 10; i++ {
		key := utils.ConcatBytes(prefix, uint64(i))
		msg := &testMessage{Data: fmt.Sprintf("msg-%d", i)}
		err := table.Set(txn, key, msg)
		require.NoError(t, err)
	}

	// ListAll with prefix
	messages, err := table.ListAll(txn, prefix)
	require.NoError(t, err)
	require.Len(t, messages, 10)

	// Verify messages and check for corruption
	for i, msg := range messages {
		expected := fmt.Sprintf("msg-%d", i)
		require.Equal(t, expected, msg.Data)
	}
}

func TestBinaryTable_ListInRange(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("BINARY1")
	table := NewBinaryTable[*testMessage, testMessage](tableId, []byte{0x10}, []byte{0x20})

	txn := s.Update()
	defer txn.Discard()

	// Set values with sequential keys
	for i := 0; i < 10; i++ {
		key := utils.ConcatBytes([]byte{0x15}, uint64(i*10))
		msg := &testMessage{Data: fmt.Sprintf("msg-%d", i)}
		err := table.Set(txn, key, msg)
		require.NoError(t, err)
	}

	// List in range [15,20] to [15,60]
	lowerBound := utils.ConcatBytes([]byte{0x15}, uint64(20))
	upperBound := utils.ConcatBytes([]byte{0x15}, uint64(60))

	var collected []*testMessage
	err := table.ListInRange(txn, lowerBound, upperBound, false, func(msg *testMessage) (bool, error) {
		collected = append(collected, msg)
		return true, nil
	})
	require.NoError(t, err)

	// Should get indices 2,3,4,5,6 (keys 20,30,40,50,60)
	expected := []string{"msg-2", "msg-3", "msg-4", "msg-5", "msg-6"}
	require.Len(t, collected, len(expected))

	for i, msg := range collected {
		require.Equal(t, expected[i], msg.Data)
	}
}

func TestBinaryTable_ListPaginated(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("BINARY1")
	table := NewBinaryTable[*testMessage, testMessage](tableId, []byte{0x10}, []byte{0x20})

	txn := s.Update()
	defer txn.Discard()

	// Set values
	prefix := []byte{0x15}
	numItems := 25
	for i := 0; i < numItems; i++ {
		key := utils.ConcatBytes(prefix, uint64(i))
		msg := &testMessage{Data: fmt.Sprintf("msg-%d", i)}
		err := table.Set(txn, key, msg)
		require.NoError(t, err)
	}

	// Paginate through all items
	pageSize := 10
	allMessages := []*testMessage{}
	var token *PaginationToken

	for {
		result, err := table.ListPaginated(txn, prefix, token, pageSize)
		require.NoError(t, err)

		allMessages = append(allMessages, result.Items...)

		if result.NextPaginationToken == nil {
			break
		}
		token = result.NextPaginationToken
	}

	require.Len(t, allMessages, numItems)

	// Verify no corruption
	for i, msg := range allMessages {
		expected := fmt.Sprintf("msg-%d", i)
		require.Equal(t, expected, msg.Data)
	}
}

func TestBinaryTable_PrefixTrimming(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("BINARY1")
	table := NewBinaryTable[*testMessage, testMessage](tableId, []byte{0x10}, []byte{0x20})

	txn := s.Update()
	defer txn.Discard()

	key := []byte{0x15, 0x01, 0x02}
	msg := &testMessage{Data: "test"}
	err := table.Set(txn, key, msg)
	require.NoError(t, err)

	// Use internal eachPrefix to verify keys don't contain tableId
	prefix := []byte{0x15}
	err = table.table.eachPrefix(txn, prefix, func(returnedKey []byte, value []byte) (bool, error) {
		require.NotContains(t, string(returnedKey), string(tableId), "Key should not contain table prefix")
		require.True(t, len(returnedKey) >= len(key) && string(returnedKey[:len(key)]) == string(key))
		return true, nil
	})
	require.NoError(t, err)
}

func TestBinaryTable_OutOfRange(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("BINARY1")
	table := NewBinaryTable[*testMessage, testMessage](tableId, []byte{0x10}, []byte{0x20})

	txn := s.Update()
	defer txn.Discard()

	msg := &testMessage{Data: "test"}

	// Test key below range - should panic
	require.Panics(t, func() {
		_ = table.Set(txn, []byte{0x05}, msg)
	})

	// Test key above range - should panic
	require.Panics(t, func() {
		_ = table.Set(txn, []byte{0x25}, msg)
	})

	// Test key within range - should not panic
	err := table.Set(txn, []byte{0x15}, msg)
	require.NoError(t, err)
}

// =============================================================================
// StringTable Tests
// =============================================================================

func TestStringTable_Get_Set_Delete(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("STRING1")
	table := NewStringTable(tableId, []byte{0x30}, []byte{0x40})

	txn := s.Update()
	defer txn.Discard()

	// Test Set and Get
	key := []byte{0x35, 0x01}
	value := "test-value"
	err := table.Set(txn, key, value)
	require.NoError(t, err)

	retrieved, err := table.Get(txn, key)
	require.NoError(t, err)
	require.Equal(t, value, retrieved)

	// Test Delete
	err = table.Delete(txn, key)
	require.NoError(t, err)

	_, err = table.Get(txn, key)
	require.ErrorIs(t, err, store.ErrNotFound)
}

func TestStringTable_ListAll(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("STRING1")
	table := NewStringTable(tableId, []byte{0x30}, []byte{0x40})

	txn := s.Update()
	defer txn.Discard()

	prefix := []byte{0x35}
	for i := 0; i < 10; i++ {
		key := utils.ConcatBytes(prefix, uint64(i))
		value := fmt.Sprintf("value-%d", i)
		err := table.Set(txn, key, value)
		require.NoError(t, err)
	}

	values, err := table.ListAll(txn, prefix)
	require.NoError(t, err)
	require.Len(t, values, 10)

	for i, val := range values {
		expected := fmt.Sprintf("value-%d", i)
		require.Equal(t, expected, val)
	}
}

func TestStringTable_ListInRange(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("STRING1")
	table := NewStringTable(tableId, []byte{0x30}, []byte{0x40})

	txn := s.Update()
	defer txn.Discard()

	for i := 0; i < 10; i++ {
		key := utils.ConcatBytes([]byte{0x35}, uint64(i*10))
		value := fmt.Sprintf("value-%d", i)
		err := table.Set(txn, key, value)
		require.NoError(t, err)
	}

	lowerBound := utils.ConcatBytes([]byte{0x35}, uint64(20))
	upperBound := utils.ConcatBytes([]byte{0x35}, uint64(60))

	var collected []string
	err := table.ListInRange(txn, lowerBound, upperBound, false, func(value string) (bool, error) {
		collected = append(collected, value)
		return true, nil
	})
	require.NoError(t, err)

	expected := []string{"value-2", "value-3", "value-4", "value-5", "value-6"}
	require.Equal(t, expected, collected)
}

func TestStringTable_ListPaginated(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("STRING1")
	table := NewStringTable(tableId, []byte{0x30}, []byte{0x40})

	txn := s.Update()
	defer txn.Discard()

	prefix := []byte{0x35}
	numItems := 25
	for i := 0; i < numItems; i++ {
		key := utils.ConcatBytes(prefix, uint64(i))
		value := fmt.Sprintf("value-%d", i)
		err := table.Set(txn, key, value)
		require.NoError(t, err)
	}

	// Test forward pagination
	result, err := table.ListPaginated(txn, prefix, nil, 10)
	require.NoError(t, err)
	require.Len(t, result.Items, 10)
	require.NotNil(t, result.NextPaginationToken)
	require.Nil(t, result.PreviousPaginationToken)

	// Test second page
	result2, err := table.ListPaginated(txn, prefix, result.NextPaginationToken, 10)
	require.NoError(t, err)
	require.Len(t, result2.Items, 10)
	require.NotNil(t, result2.PreviousPaginationToken)

	// Verify reverse pagination
	resultBack, err := table.ListPaginated(txn, prefix, result2.PreviousPaginationToken, 10)
	require.NoError(t, err)
	require.Len(t, resultBack.Items, 10)
}

func TestStringTable_ByteSliceCorruption(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("STRING1")
	table := NewStringTable(tableId, []byte{0x30}, []byte{0x40})

	txn := s.Update()
	defer txn.Discard()

	prefix := []byte{0x35}
	for i := 0; i < 20; i++ {
		key := utils.ConcatBytes(prefix, uint64(i))
		value := fmt.Sprintf("value-%d", i)
		err := table.Set(txn, key, value)
		require.NoError(t, err)
	}

	// Get first page
	result1, err := table.ListPaginated(txn, prefix, nil, 10)
	require.NoError(t, err)

	firstPageItems := make([]string, len(result1.Items))
	copy(firstPageItems, result1.Items)

	// Get second page
	_, err = table.ListPaginated(txn, prefix, result1.NextPaginationToken, 10)
	require.NoError(t, err)

	// Verify first page data wasn't corrupted
	for i, item := range firstPageItems {
		expected := fmt.Sprintf("value-%d", i)
		require.Equal(t, expected, item)
	}
}

func TestStringTable_OutOfRange(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("STRING1")
	table := NewStringTable(tableId, []byte{0x30}, []byte{0x40})

	txn := s.Update()
	defer txn.Discard()

	// Key below range
	require.Panics(t, func() {
		_ = table.Set(txn, []byte{0x20}, "value")
	})

	// Key above range
	require.Panics(t, func() {
		_ = table.Set(txn, []byte{0x50}, "value")
	})

	// Key within range
	err := table.Set(txn, []byte{0x35}, "value")
	require.NoError(t, err)
}

// =============================================================================
// Uint64Table Tests
// =============================================================================

func TestUint64Table_Get_Set_Delete(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("UINT64")
	table := NewUint64Table(tableId, []byte{0x50}, []byte{0x60})

	txn := s.Update()
	defer txn.Discard()

	key := []byte{0x55, 0x01}
	value := uint64(123456789)

	err := table.Set(txn, key, value)
	require.NoError(t, err)

	retrieved, err := table.Get(txn, key)
	require.NoError(t, err)
	require.Equal(t, value, retrieved)

	err = table.Delete(txn, key)
	require.NoError(t, err)

	_, err = table.Get(txn, key)
	require.ErrorIs(t, err, store.ErrNotFound)
}

func TestUint64Table_ListAll(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("UINT64")
	table := NewUint64Table(tableId, []byte{0x50}, []byte{0x60})

	txn := s.Update()
	defer txn.Discard()

	prefix := []byte{0x55}
	for i := 0; i < 10; i++ {
		key := utils.ConcatBytes(prefix, uint64(i))
		value := uint64(i * 100)
		err := table.Set(txn, key, value)
		require.NoError(t, err)
	}

	values, err := table.ListAll(txn, prefix)
	require.NoError(t, err)
	require.Len(t, values, 10)

	for i, val := range values {
		expected := uint64(i * 100)
		require.Equal(t, expected, val)
	}
}

func TestUint64Table_ListPaginated(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("UINT64")
	table := NewUint64Table(tableId, []byte{0x50}, []byte{0x60})

	txn := s.Update()
	defer txn.Discard()

	prefix := []byte{0x55}
	numItems := 25
	for i := 0; i < numItems; i++ {
		key := utils.ConcatBytes(prefix, uint64(i))
		value := uint64(i * 1000)
		err := table.Set(txn, key, value)
		require.NoError(t, err)
	}

	allValues := []uint64{}
	var token *PaginationToken

	for {
		result, err := table.ListPaginated(txn, prefix, token, 10)
		require.NoError(t, err)

		allValues = append(allValues, result.Items...)

		if result.NextPaginationToken == nil {
			break
		}
		token = result.NextPaginationToken
	}

	require.Len(t, allValues, numItems)

	for i, val := range allValues {
		expected := uint64(i * 1000)
		require.Equal(t, expected, val)
	}
}

func TestUint64Table_OutOfRange(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("UINT64")
	table := NewUint64Table(tableId, []byte{0x50}, []byte{0x60})

	txn := s.Update()
	defer txn.Discard()

	// Key below range
	require.Panics(t, func() {
		_ = table.Set(txn, []byte{0x40}, 123)
	})

	// Key above range
	require.Panics(t, func() {
		_ = table.Set(txn, []byte{0x70}, 123)
	})
}

// =============================================================================
// Uint32Table Tests
// =============================================================================

func TestUint32Table_Get_Set_Delete(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("UINT32")
	table := NewUint32Table(tableId, []byte{0x70}, []byte{0x80})

	txn := s.Update()
	defer txn.Discard()

	key := []byte{0x75, 0x01}
	value := uint32(54321)

	err := table.Set(txn, key, value)
	require.NoError(t, err)

	retrieved, err := table.Get(txn, key)
	require.NoError(t, err)
	require.Equal(t, value, retrieved)

	err = table.Delete(txn, key)
	require.NoError(t, err)

	_, err = table.Get(txn, key)
	require.ErrorIs(t, err, store.ErrNotFound)
}

func TestUint32Table_ListAll(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("UINT32")
	table := NewUint32Table(tableId, []byte{0x70}, []byte{0x80})

	txn := s.Update()
	defer txn.Discard()

	prefix := []byte{0x75}
	for i := 0; i < 10; i++ {
		key := utils.ConcatBytes(prefix, uint32(i))
		value := uint32(i * 50)
		err := table.Set(txn, key, value)
		require.NoError(t, err)
	}

	values, err := table.ListAll(txn, prefix)
	require.NoError(t, err)
	require.Len(t, values, 10)

	for i, val := range values {
		expected := uint32(i * 50)
		require.Equal(t, expected, val)
	}
}

func TestUint32Table_ListPaginated(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("UINT32")
	table := NewUint32Table(tableId, []byte{0x70}, []byte{0x80})

	txn := s.Update()
	defer txn.Discard()

	prefix := []byte{0x75}
	numItems := 20
	for i := 0; i < numItems; i++ {
		key := utils.ConcatBytes(prefix, uint32(i))
		value := uint32(i * 2)
		err := table.Set(txn, key, value)
		require.NoError(t, err)
	}

	result, err := table.ListPaginated(txn, prefix, nil, 10)
	require.NoError(t, err)
	require.Len(t, result.Items, 10)

	for i, val := range result.Items {
		expected := uint32(i * 2)
		require.Equal(t, expected, val)
	}
}

func TestUint32Table_OutOfRange(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("UINT32")
	table := NewUint32Table(tableId, []byte{0x70}, []byte{0x80})

	txn := s.Update()
	defer txn.Discard()

	// Key below range
	require.Panics(t, func() {
		_ = table.Set(txn, []byte{0x60}, 123)
	})

	// Key above range
	require.Panics(t, func() {
		_ = table.Set(txn, []byte{0x90}, 123)
	})
}

// =============================================================================
// OneToManyUint64Index Tests
// =============================================================================

func TestOneToManyUint64Index_Add_List_Delete(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("IDX64")
	index := NewOneToManyUint64Index(tableId, []byte{0x90}, []byte{0xa0})

	txn := s.Update()
	defer txn.Discard()

	pk := []byte{0x95, 0x01}

	// Add items
	for i := 0; i < 10; i++ {
		err := index.Add(txn, pk, uint64(i))
		require.NoError(t, err)
	}

	// List items
	var collected []uint64
	err := index.List(txn, pk, func(item uint64) (bool, error) {
		collected = append(collected, item)
		return true, nil
	})
	require.NoError(t, err)
	require.Len(t, collected, 10)

	for i, item := range collected {
		require.Equal(t, uint64(i), item)
	}

	// Delete an item
	err = index.Delete(txn, pk, uint64(5))
	require.NoError(t, err)

	// List again
	collected = []uint64{}
	err = index.List(txn, pk, func(item uint64) (bool, error) {
		collected = append(collected, item)
		return true, nil
	})
	require.NoError(t, err)
	require.Len(t, collected, 9)
}

func TestOneToManyUint64Index_NotEmpty(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("IDX64")
	index := NewOneToManyUint64Index(tableId, []byte{0x90}, []byte{0xa0})

	txn := s.Update()
	defer txn.Discard()

	pk1 := []byte{0x95, 0x01}
	pk2 := []byte{0x95, 0x02}

	// pk1 should be empty initially
	notEmpty, err := index.NotEmpty(txn, pk1)
	require.NoError(t, err)
	require.False(t, notEmpty)

	// Add item to pk1
	err = index.Add(txn, pk1, 123)
	require.NoError(t, err)

	// pk1 should not be empty now
	notEmpty, err = index.NotEmpty(txn, pk1)
	require.NoError(t, err)
	require.True(t, notEmpty)

	// pk2 should still be empty
	notEmpty, err = index.NotEmpty(txn, pk2)
	require.NoError(t, err)
	require.False(t, notEmpty)
}

func TestOneToManyUint64Index_ListPaginated(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("IDX64")
	index := NewOneToManyUint64Index(tableId, []byte{0x90}, []byte{0xa0})

	txn := s.Update()
	defer txn.Discard()

	prefix := []byte{0x95}
	numItems := 30
	for i := 0; i < numItems; i++ {
		pk := utils.ConcatBytes(prefix, uint64(i))
		err := index.Add(txn, pk, uint64(i*10))
		require.NoError(t, err)
	}

	// Paginate
	result, err := index.ListPaginated(txn, prefix, nil, 15)
	require.NoError(t, err)
	require.Len(t, result.Items, 15)
	require.NotNil(t, result.NextPaginationToken)

	// Second page
	result2, err := index.ListPaginated(txn, prefix, result.NextPaginationToken, 15)
	require.NoError(t, err)
	require.Len(t, result2.Items, 15)
}

func TestOneToManyUint64Index_ByteSliceCorruption(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("IDX64")
	index := NewOneToManyUint64Index(tableId, []byte{0x90}, []byte{0xa0})

	txn := s.Update()
	defer txn.Discard()

	pk := []byte{0x95, 0x01}
	numItems := 50
	for i := 0; i < numItems; i++ {
		err := index.Add(txn, pk, uint64(i))
		require.NoError(t, err)
	}

	// Collect all items
	var collected []uint64
	err := index.List(txn, pk, func(item uint64) (bool, error) {
		collected = append(collected, item)
		return true, nil
	})
	require.NoError(t, err)

	// Verify no corruption
	for i, item := range collected {
		require.Equal(t, uint64(i), item)
	}
}

func TestOneToManyUint64Index_OutOfRange(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("IDX64")
	index := NewOneToManyUint64Index(tableId, []byte{0x90}, []byte{0xa0})

	txn := s.Update()
	defer txn.Discard()

	// Key below range
	require.Panics(t, func() {
		_ = index.Add(txn, []byte{0x80}, 123)
	})

	// Key above range
	require.Panics(t, func() {
		_ = index.Add(txn, []byte{0xb0}, 123)
	})
}

// =============================================================================
// OneToManySortedIndex Tests
// =============================================================================

func TestOneToManySortedIndex_Add_ListAll_Delete(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("IDXSRT")
	index := NewOneToManySortedIndex(tableId, []byte{0xb0}, []byte{0xc0})

	txn := s.Update()
	defer txn.Discard()

	pk := []byte{0xb5, 0x01}

	// Add items
	items := [][]byte{
		[]byte("item-003"),
		[]byte("item-001"),
		[]byte("item-005"),
		[]byte("item-002"),
		[]byte("item-004"),
	}
	for _, item := range items {
		err := index.Add(txn, pk, item)
		require.NoError(t, err)
	}

	// ListAll - should be sorted
	var collected [][]byte
	err := index.ListAll(txn, pk, func(item []byte) (bool, error) {
		collected = append(collected, item)
		return true, nil
	})
	require.NoError(t, err)
	require.Len(t, collected, 5)

	// Verify sorted order
	expected := [][]byte{
		[]byte("item-001"),
		[]byte("item-002"),
		[]byte("item-003"),
		[]byte("item-004"),
		[]byte("item-005"),
	}
	for i, item := range collected {
		require.Equal(t, expected[i], item)
	}

	// Delete an item
	err = index.Delete(txn, pk, []byte("item-003"))
	require.NoError(t, err)

	// List again
	collected = [][]byte{}
	err = index.ListAll(txn, pk, func(item []byte) (bool, error) {
		collected = append(collected, item)
		return true, nil
	})
	require.NoError(t, err)
	require.Len(t, collected, 4)
}

func TestOneToManySortedIndex_ListInRange(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("IDXSRT")
	index := NewOneToManySortedIndex(tableId, []byte{0xb0}, []byte{0xc0})

	txn := s.Update()
	defer txn.Discard()

	pk := []byte{0xb5, 0x01}

	// Add items
	for i := 0; i < 10; i++ {
		item := []byte(fmt.Sprintf("item-%03d", i))
		err := index.Add(txn, pk, item)
		require.NoError(t, err)
	}

	// List in range
	lowerBound := []byte("item-003")
	upperBound := []byte("item-007")

	var collected [][]byte
	err := index.ListInRange(txn, pk, lowerBound, upperBound, func(key []byte) (bool, error) {
		// Note: ListInRange returns the full key (pk+item), so we need to strip the pk prefix
		require.True(t, len(key) >= len(pk))
		item := key[len(pk):]
		collected = append(collected, item)
		return true, nil
	})
	require.NoError(t, err)

	// Should get items 3,4,5,6,7
	require.Len(t, collected, 5)

	for i, item := range collected {
		expected := []byte(fmt.Sprintf("item-%03d", i+3))
		require.Equal(t, expected, item)
	}
}

func TestOneToManySortedIndex_NotEmpty(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("IDXSRT")
	index := NewOneToManySortedIndex(tableId, []byte{0xb0}, []byte{0xc0})

	txn := s.Update()
	defer txn.Discard()

	pk := []byte{0xb5, 0x01}

	// Should be empty initially
	notEmpty, err := index.NotEmpty(txn, pk)
	require.NoError(t, err)
	require.False(t, notEmpty)

	// Add item
	err = index.Add(txn, pk, []byte("item"))
	require.NoError(t, err)

	// Should not be empty now
	notEmpty, err = index.NotEmpty(txn, pk)
	require.NoError(t, err)
	require.True(t, notEmpty)
}

func TestOneToManySortedIndex_ListPaginated(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("IDXSRT")
	index := NewOneToManySortedIndex(tableId, []byte{0xb0}, []byte{0xc0})

	txn := s.Update()
	defer txn.Discard()

	pk := []byte{0xb5, 0x01}
	numItems := 25
	for i := 0; i < numItems; i++ {
		item := []byte(fmt.Sprintf("item-%03d", i))
		err := index.Add(txn, pk, item)
		require.NoError(t, err)
	}

	result, err := index.ListPaginated(txn, pk, nil, 10)
	require.NoError(t, err)
	require.Len(t, result.Items, 10)

	// Verify items are correctly extracted (prefix stripped)
	for i, item := range result.Items {
		expected := []byte(fmt.Sprintf("item-%03d", i))
		require.Equal(t, expected, item)
	}
}

func TestOneToManySortedIndex_OutOfRange(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("IDXSRT")
	index := NewOneToManySortedIndex(tableId, []byte{0xb0}, []byte{0xc0})

	txn := s.Update()
	defer txn.Discard()

	// Key below range
	require.Panics(t, func() {
		_ = index.Add(txn, []byte{0xa0}, []byte("item"))
	})

	// Key above range
	require.Panics(t, func() {
		_ = index.Add(txn, []byte{0xd0}, []byte("item"))
	})
}

// =============================================================================
// SortedIndex Tests
// =============================================================================

func TestSortedIndex_Add_ListInRange_Delete(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("SORTED")
	index := NewSortedIndex(tableId, []byte{0xd0}, []byte{0xe0})

	txn := s.Update()
	defer txn.Discard()

	// Add items (no PK, global index)
	items := [][]byte{
		{0xd5, 0x03},
		{0xd5, 0x01},
		{0xd5, 0x05},
		{0xd5, 0x02},
		{0xd5, 0x04},
	}
	for _, item := range items {
		err := index.Add(txn, item)
		require.NoError(t, err)
	}

	// List in range
	lowerBound := []byte{0xd5, 0x02}
	upperBound := []byte{0xd5, 0x04}

	var collected [][]byte
	err := index.ListInRange(txn, lowerBound, upperBound, func(item []byte) (bool, error) {
		collected = append(collected, item)
		return true, nil
	})
	require.NoError(t, err)

	// Should get items with values 02, 03, 04
	expected := [][]byte{
		{0xd5, 0x02},
		{0xd5, 0x03},
		{0xd5, 0x04},
	}
	require.Equal(t, expected, collected)

	// Delete an item
	err = index.Delete(txn, []byte{0xd5, 0x03})
	require.NoError(t, err)

	// List again
	collected = [][]byte{}
	err = index.ListInRange(txn, lowerBound, upperBound, func(item []byte) (bool, error) {
		collected = append(collected, item)
		return true, nil
	})
	require.NoError(t, err)
	require.Len(t, collected, 2)
}

func TestSortedIndex_NotEmpty(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("SORTED")
	index := NewSortedIndex(tableId, []byte{0xd0}, []byte{0xe0})

	txn := s.Update()
	defer txn.Discard()

	prefix := []byte{0xd5}

	// Should be empty initially
	notEmpty, err := index.NotEmpty(txn, prefix)
	require.NoError(t, err)
	require.False(t, notEmpty)

	// Add item
	err = index.Add(txn, []byte{0xd5, 0x01})
	require.NoError(t, err)

	// Should not be empty now
	notEmpty, err = index.NotEmpty(txn, prefix)
	require.NoError(t, err)
	require.True(t, notEmpty)
}

func TestSortedIndex_GetTableKeyRange(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("SORTED")
	lowerBound := []byte{0xd0}
	upperBound := []byte{0xe0}
	index := NewSortedIndex(tableId, lowerBound, upperBound)

	keyRange := index.GetTableKeyRange()

	// Key range should include tableId prefix
	expectedLower := utils.ConcatBytes(tableId, lowerBound)
	expectedUpper := utils.ConcatBytes(tableId, upperBound)

	require.Equal(t, expectedLower, keyRange.Lower)
	require.Equal(t, expectedUpper, keyRange.Upper)
}

func TestSortedIndex_OutOfRange(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	tableId := []byte("SORTED")
	index := NewSortedIndex(tableId, []byte{0xd0}, []byte{0xe0})

	txn := s.Update()
	defer txn.Discard()

	// Key below range
	require.Panics(t, func() {
		_ = index.Add(txn, []byte{0xc0})
	})

	// Key above range
	require.Panics(t, func() {
		_ = index.Add(txn, []byte{0xf0})
	})

	// Key within range
	err := index.Add(txn, []byte{0xd5})
	require.NoError(t, err)
}

// =============================================================================
// Cross-Table Tests
// =============================================================================

func TestMultipleTableIsolation(t *testing.T) {
	s := setupTestStore(t)
	defer s.Close()

	table1 := NewStringTable([]byte("TBL1"), []byte{0x00}, []byte{0xff})
	table2 := NewStringTable([]byte("TBL2"), []byte{0x00}, []byte{0xff})

	txn := s.Update()
	defer txn.Discard()

	// Set same key in both tables
	key := []byte("shared-key")
	value1 := "value-from-table1"
	value2 := "value-from-table2"

	err := table1.Set(txn, key, value1)
	require.NoError(t, err)

	err = table2.Set(txn, key, value2)
	require.NoError(t, err)

	// Verify both tables return their own values
	retrieved1, err := table1.Get(txn, key)
	require.NoError(t, err)
	require.Equal(t, value1, retrieved1)

	retrieved2, err := table2.Get(txn, key)
	require.NoError(t, err)
	require.Equal(t, value2, retrieved2)

	// Delete from table1 shouldn't affect table2
	err = table1.Delete(txn, key)
	require.NoError(t, err)

	_, err = table1.Get(txn, key)
	require.ErrorIs(t, err, store.ErrNotFound)

	retrieved2Again, err := table2.Get(txn, key)
	require.NoError(t, err)
	require.Equal(t, value2, retrieved2Again)
}

func TestEmptyTableId_Panics(t *testing.T) {
	require.Panics(t, func() {
		_ = NewStringTable([]byte{}, []byte{0x00}, []byte{0xff})
	})
}
