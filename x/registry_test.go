package monsterax

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// =============================================================================
// TablePrefix Tests
// =============================================================================

func TestTablePrefix_Bytes(t *testing.T) {
	prefix := []byte{0x01, 0x02, 0x03}
	tp := TablePrefix{value: prefix}

	result := tp.Bytes()

	require.Equal(t, prefix, result)
}

func TestTablePrefix_BytesEmpty(t *testing.T) {
	prefix := []byte{}
	tp := TablePrefix{value: prefix}

	result := tp.Bytes()

	require.Len(t, result, 0)
}

// =============================================================================
// BaseTableRegistry Tests
// =============================================================================

func TestBaseTableRegistry_NewBaseTableRegistry(t *testing.T) {
	registry := NewBaseTableRegistry(4)

	require.NotNil(t, registry)
	require.Equal(t, 4, registry.prefixLength)
	require.NotNil(t, registry.registry)
	require.Len(t, registry.registry, 0)
}

func TestBaseTableRegistry_RegisterPrefix_Valid(t *testing.T) {
	registry := NewBaseTableRegistry(4)

	prefix := []byte{0x01, 0x02, 0x03, 0x04}
	tablePrefix := registry.RegisterPrefix(prefix)

	require.Equal(t, prefix, tablePrefix.Bytes())

	// Verify it was registered
	allPrefixes := registry.GetAllPrefixes()
	require.Len(t, allPrefixes, 1)

	key := string(prefix)
	registered, exists := allPrefixes[key]
	require.True(t, exists)
	require.Equal(t, prefix, registered)
}

func TestBaseTableRegistry_RegisterPrefix_Multiple(t *testing.T) {
	registry := NewBaseTableRegistry(2)

	prefixes := [][]byte{
		{0x01, 0x02},
		{0x03, 0x04},
		{0x05, 0x06},
		{0xaa, 0xbb},
	}

	for _, prefix := range prefixes {
		tablePrefix := registry.RegisterPrefix(prefix)
		require.Equal(t, prefix, tablePrefix.Bytes())
	}

	// Verify all were registered
	allPrefixes := registry.GetAllPrefixes()
	require.Len(t, allPrefixes, len(prefixes))

	for _, prefix := range prefixes {
		key := string(prefix)
		registered, exists := allPrefixes[key]
		require.True(t, exists, "Prefix %v not found in registry", prefix)
		require.Equal(t, prefix, registered)
	}
}

func TestBaseTableRegistry_RegisterPrefix_WrongLength_TooShort(t *testing.T) {
	registry := NewBaseTableRegistry(4)

	prefix := []byte{0x01, 0x02} // Only 2 bytes, expected 4
	require.Panics(t, func() {
		registry.RegisterPrefix(prefix)
	})
}

func TestBaseTableRegistry_RegisterPrefix_WrongLength_TooLong(t *testing.T) {
	registry := NewBaseTableRegistry(2)

	prefix := []byte{0x01, 0x02, 0x03, 0x04} // 4 bytes, expected 2
	require.Panics(t, func() {
		registry.RegisterPrefix(prefix)
	})
}

func TestBaseTableRegistry_RegisterPrefix_EmptyPrefix(t *testing.T) {
	registry := NewBaseTableRegistry(0)

	// Should work with empty prefix when prefixLength is 0
	prefix := []byte{}
	tablePrefix := registry.RegisterPrefix(prefix)

	require.Len(t, tablePrefix.Bytes(), 0)
}

func TestBaseTableRegistry_RegisterPrefix_Duplicate(t *testing.T) {
	registry := NewBaseTableRegistry(3)

	prefix := []byte{0xaa, 0xbb, 0xcc}
	registry.RegisterPrefix(prefix)

	// Try to register the same prefix again
	require.Panics(t, func() {
		registry.RegisterPrefix(prefix)
	})
}

func TestBaseTableRegistry_RegisterPrefix_DuplicateAfterMultiple(t *testing.T) {
	registry := NewBaseTableRegistry(2)

	// Register several prefixes
	registry.RegisterPrefix([]byte{0x01, 0x02})
	registry.RegisterPrefix([]byte{0x03, 0x04})
	registry.RegisterPrefix([]byte{0x05, 0x06})

	// Try to register a duplicate
	require.Panics(t, func() {
		registry.RegisterPrefix([]byte{0x03, 0x04})
	})
}

func TestBaseTableRegistry_GetAllPrefixes_Empty(t *testing.T) {
	registry := NewBaseTableRegistry(4)

	allPrefixes := registry.GetAllPrefixes()

	require.Len(t, allPrefixes, 0)
}

func TestBaseTableRegistry_GetAllPrefixes_ReturnsIndependentCopy(t *testing.T) {
	registry := NewBaseTableRegistry(2)

	prefix := []byte{0x01, 0x02}
	registry.RegisterPrefix(prefix)

	// Get all prefixes
	allPrefixes1 := registry.GetAllPrefixes()

	// Modify the returned map
	allPrefixes1["new-key"] = []byte{0xff, 0xff}

	// Get all prefixes again
	allPrefixes2 := registry.GetAllPrefixes()

	// Verify the internal registry wasn't affected
	require.Len(t, allPrefixes2, 1)
	_, exists := allPrefixes2["new-key"]
	require.False(t, exists, "Registry should not contain the key we added to the returned map")
}

func TestBaseTableRegistry_Interface(t *testing.T) {
	// Verify BaseTableRegistry implements TableRegistry interface
	var _ TableRegistry = (*BaseTableRegistry)(nil)

	registry := NewBaseTableRegistry(4)
	var iface TableRegistry = registry

	prefix := []byte{0x01, 0x02, 0x03, 0x04}
	tablePrefix := iface.RegisterPrefix(prefix)

	require.Equal(t, prefix, tablePrefix.Bytes())
}

// =============================================================================
// PrefixedTableRegistry Tests
// =============================================================================

func TestPrefixedTableRegistry_NewPrefixedTableRegistry(t *testing.T) {
	baseRegistry := NewBaseTableRegistry(4)
	prefix := []byte{0xaa, 0xbb}

	prefixedRegistry := NewPrefixedTableRegistry(prefix, baseRegistry)

	require.NotNil(t, prefixedRegistry)
	require.Equal(t, prefix, prefixedRegistry.prefix)
	require.Equal(t, baseRegistry, prefixedRegistry.inner)
}

func TestPrefixedTableRegistry_RegisterPrefix_AddsPrefix(t *testing.T) {
	baseRegistry := NewBaseTableRegistry(4)
	prefix := []byte{0xaa, 0xbb}

	prefixedRegistry := NewPrefixedTableRegistry(prefix, baseRegistry)

	value := []byte{0x01, 0x02}
	tablePrefix := prefixedRegistry.RegisterPrefix(value)

	// The returned prefix should be prefix + value
	expected := []byte{0xaa, 0xbb, 0x01, 0x02}
	require.Equal(t, expected, tablePrefix.Bytes())

	// Verify it was registered in the base registry with the full prefix
	allPrefixes := baseRegistry.GetAllPrefixes()
	require.Len(t, allPrefixes, 1)

	key := string(expected)
	registered, exists := allPrefixes[key]
	require.True(t, exists, "Full prefix not found in base registry")
	require.Equal(t, expected, registered)
}

func TestPrefixedTableRegistry_RegisterPrefix_Multiple(t *testing.T) {
	baseRegistry := NewBaseTableRegistry(5)
	prefix := []byte{0xff}

	prefixedRegistry := NewPrefixedTableRegistry(prefix, baseRegistry)

	values := [][]byte{
		{0x01, 0x02, 0x03, 0x04},
		{0x11, 0x12, 0x13, 0x14},
		{0x21, 0x22, 0x23, 0x24},
	}

	for _, value := range values {
		tablePrefix := prefixedRegistry.RegisterPrefix(value)

		expected := append([]byte{0xff}, value...)
		require.Equal(t, expected, tablePrefix.Bytes())
	}

	// Verify all were registered in base registry
	allPrefixes := baseRegistry.GetAllPrefixes()
	require.Len(t, allPrefixes, len(values))
}

func TestPrefixedTableRegistry_RegisterPrefix_EmptyPrefix(t *testing.T) {
	baseRegistry := NewBaseTableRegistry(3)
	emptyPrefix := []byte{}

	prefixedRegistry := NewPrefixedTableRegistry(emptyPrefix, baseRegistry)

	value := []byte{0x01, 0x02, 0x03}
	tablePrefix := prefixedRegistry.RegisterPrefix(value)

	// With empty prefix, should just be the value
	require.Equal(t, value, tablePrefix.Bytes())
}

func TestPrefixedTableRegistry_RegisterPrefix_EmptyValue(t *testing.T) {
	baseRegistry := NewBaseTableRegistry(2)
	prefix := []byte{0xaa, 0xbb}

	prefixedRegistry := NewPrefixedTableRegistry(prefix, baseRegistry)

	value := []byte{}
	tablePrefix := prefixedRegistry.RegisterPrefix(value)

	// With empty value, should just be the prefix
	require.Equal(t, prefix, tablePrefix.Bytes())
}

func TestPrefixedTableRegistry_RegisterPrefix_Duplicate(t *testing.T) {
	baseRegistry := NewBaseTableRegistry(4)
	prefix := []byte{0xaa, 0xbb}

	prefixedRegistry := NewPrefixedTableRegistry(prefix, baseRegistry)

	value := []byte{0x01, 0x02}
	prefixedRegistry.RegisterPrefix(value)

	// Try to register the same value again
	require.Panics(t, func() {
		prefixedRegistry.RegisterPrefix(value)
	})
}

func TestPrefixedTableRegistry_Nested(t *testing.T) {
	baseRegistry := NewBaseTableRegistry(6)

	// Create nested prefixed registries
	prefix1 := []byte{0xaa, 0xbb}
	prefixedRegistry1 := NewPrefixedTableRegistry(prefix1, baseRegistry)

	prefix2 := []byte{0xcc, 0xdd}
	prefixedRegistry2 := NewPrefixedTableRegistry(prefix2, prefixedRegistry1)

	// Register through the nested registry
	value := []byte{0x01, 0x02}
	tablePrefix := prefixedRegistry2.RegisterPrefix(value)

	// The order is: innermost prefix (prefix1) + outer prefix (prefix2) + value
	expected := []byte{0xaa, 0xbb, 0xcc, 0xdd, 0x01, 0x02}
	require.Equal(t, expected, tablePrefix.Bytes())

	// Verify it's in the base registry
	allPrefixes := baseRegistry.GetAllPrefixes()
	require.Len(t, allPrefixes, 1)

	key := string(expected)
	registered, exists := allPrefixes[key]
	require.True(t, exists, "Nested prefix not found in base registry")
	require.Equal(t, expected, registered)
}

func TestPrefixedTableRegistry_NestedMultipleLevels(t *testing.T) {
	baseRegistry := NewBaseTableRegistry(10)

	// Create multiple levels of nesting
	level1 := NewPrefixedTableRegistry([]byte{0x01, 0x02}, baseRegistry)
	level2 := NewPrefixedTableRegistry([]byte{0x03, 0x04}, level1)
	level3 := NewPrefixedTableRegistry([]byte{0x05, 0x06}, level2)

	value := []byte{0xaa, 0xbb, 0xcc, 0xdd}
	tablePrefix := level3.RegisterPrefix(value)

	// Order is innermost first: level1 + level2 + level3 + value
	expected := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0xaa, 0xbb, 0xcc, 0xdd}
	require.Equal(t, expected, tablePrefix.Bytes())
}

func TestPrefixedTableRegistry_DuplicateDetectionAcrossPrefixes(t *testing.T) {
	baseRegistry := NewBaseTableRegistry(4)

	prefix1 := []byte{0xaa}
	prefixedRegistry1 := NewPrefixedTableRegistry(prefix1, baseRegistry)

	prefix2 := []byte{0xaa}
	prefixedRegistry2 := NewPrefixedTableRegistry(prefix2, baseRegistry)

	// Register through first prefixed registry
	value := []byte{0x01, 0x02, 0x03}
	prefixedRegistry1.RegisterPrefix(value)

	// Try to register the same full prefix through second prefixed registry
	// This should fail because both produce the same full prefix: 0xaa,0x01,0x02,0x03
	require.Panics(t, func() {
		prefixedRegistry2.RegisterPrefix(value)
	})
}

func TestPrefixedTableRegistry_NoDuplicateWithDifferentPrefixes(t *testing.T) {
	baseRegistry := NewBaseTableRegistry(4)

	prefix1 := []byte{0xaa}
	prefixedRegistry1 := NewPrefixedTableRegistry(prefix1, baseRegistry)

	prefix2 := []byte{0xbb}
	prefixedRegistry2 := NewPrefixedTableRegistry(prefix2, baseRegistry)

	// Register the same value through both prefixed registries
	// This should work because the full prefixes are different
	value := []byte{0x01, 0x02, 0x03}

	tablePrefix1 := prefixedRegistry1.RegisterPrefix(value)
	tablePrefix2 := prefixedRegistry2.RegisterPrefix(value)

	expected1 := []byte{0xaa, 0x01, 0x02, 0x03}
	expected2 := []byte{0xbb, 0x01, 0x02, 0x03}

	require.Equal(t, expected1, tablePrefix1.Bytes())
	require.Equal(t, expected2, tablePrefix2.Bytes())

	// Verify both are in the base registry
	allPrefixes := baseRegistry.GetAllPrefixes()
	require.Len(t, allPrefixes, 2)
}

func TestPrefixedTableRegistry_Interface(t *testing.T) {
	// Verify PrefixedTableRegistry implements TableRegistry interface
	var _ TableRegistry = (*PrefixedTableRegistry)(nil)

	baseRegistry := NewBaseTableRegistry(4)
	prefixedRegistry := NewPrefixedTableRegistry([]byte{0xaa, 0xbb}, baseRegistry)

	var iface TableRegistry = prefixedRegistry

	value := []byte{0x01, 0x02}
	tablePrefix := iface.RegisterPrefix(value)

	expected := []byte{0xaa, 0xbb, 0x01, 0x02}
	require.Equal(t, expected, tablePrefix.Bytes())
}

// =============================================================================
// Integration Tests
// =============================================================================

func TestIntegration_MixedRegistrations(t *testing.T) {
	baseRegistry := NewBaseTableRegistry(5)

	// Register directly to base
	baseRegistry.RegisterPrefix([]byte{0x00, 0x01, 0x02, 0x03, 0x04})

	// Register through prefixed registry
	prefixed1 := NewPrefixedTableRegistry([]byte{0x10}, baseRegistry)
	prefixed1.RegisterPrefix([]byte{0x11, 0x12, 0x13, 0x14})

	// Register through another prefixed registry
	prefixed2 := NewPrefixedTableRegistry([]byte{0x20}, baseRegistry)
	prefixed2.RegisterPrefix([]byte{0x21, 0x22, 0x23, 0x24})

	// Register through nested prefixed registry
	nested := NewPrefixedTableRegistry([]byte{0x30}, prefixed1)
	nested.RegisterPrefix([]byte{0x31, 0x32, 0x33})

	// Verify all 4 registrations
	allPrefixes := baseRegistry.GetAllPrefixes()
	require.Len(t, allPrefixes, 4)

	expectedPrefixes := [][]byte{
		{0x00, 0x01, 0x02, 0x03, 0x04},
		{0x10, 0x11, 0x12, 0x13, 0x14},
		{0x20, 0x21, 0x22, 0x23, 0x24},
		{0x10, 0x30, 0x31, 0x32, 0x33}, // nested: prefixed1 prefix + nested prefix + value
	}

	for _, expected := range expectedPrefixes {
		key := string(expected)
		registered, exists := allPrefixes[key]
		require.True(t, exists, "Prefix %v not found in registry", expected)
		require.Equal(t, expected, registered)
	}
}

func TestIntegration_RegistryIsolation(t *testing.T) {
	// Create two separate base registries
	registry1 := NewBaseTableRegistry(3)
	registry2 := NewBaseTableRegistry(3)

	prefix := []byte{0x01, 0x02, 0x03}

	// Register same prefix in both registries - should not conflict
	registry1.RegisterPrefix(prefix)
	registry2.RegisterPrefix(prefix)

	// Verify each has 1 prefix
	require.Len(t, registry1.GetAllPrefixes(), 1)
	require.Len(t, registry2.GetAllPrefixes(), 1)

	// Register another prefix in registry1 - should not affect registry2
	registry1.RegisterPrefix([]byte{0x04, 0x05, 0x06})

	require.Len(t, registry1.GetAllPrefixes(), 2)
	require.Len(t, registry2.GetAllPrefixes(), 1)
}

func TestIntegration_ByteSliceStoredByReference(t *testing.T) {
	registry := NewBaseTableRegistry(4)

	original := []byte{0x01, 0x02, 0x03, 0x04}
	originalKey := string(original)
	tablePrefix := registry.RegisterPrefix(original)

	// Verify the slice is stored by reference (not copied)
	// Modifying the original should affect the registered prefix
	original[0] = 0xff
	original[1] = 0xff

	// The registered prefix should be affected by the modification
	expected := []byte{0xff, 0xff, 0x03, 0x04}
	require.Equal(t, expected, tablePrefix.Bytes())

	// The registry map is keyed by the original string, so we need to look it up with the original key
	// This demonstrates an important side effect: modifying a registered slice causes inconsistency
	// between the map key and the stored value
	allPrefixes := registry.GetAllPrefixes()
	registered, exists := allPrefixes[originalKey]
	require.True(t, exists, "Original prefix not found in registry")

	// The stored slice has been modified
	require.Equal(t, expected, registered)

	// The registry is now in an inconsistent state:
	// - The map key is based on the original value
	// - But the stored slice has been modified
	// This is why callers should NEVER modify slices after registering them.

	// Note: This test documents that the implementation stores slices by reference.
	// Callers must not modify slices after registering them, or the registry becomes inconsistent.
}
