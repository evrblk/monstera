package monsterax

import (
	"fmt"
)

// TablePrefix is a type-safe wrapper for table prefix byte slices.
// It ensures that table prefixes are globally unique across all Everblack products
// that share a Badger storage.
type TablePrefix struct {
	value []byte
}

// Bytes returns the underlying byte slice for the table prefix.
func (t TablePrefix) Bytes() []byte {
	return t.value
}

// TableRegistry tracks all registered prefixes to ensure global uniqueness.
type TableRegistry interface {
	RegisterPrefix(value []byte) TablePrefix
}

// BaseTableRegistry is the base implementation of TableRegistry.
type BaseTableRegistry struct {
	prefixLength int
	registry     map[string][]byte
}

var _ TableRegistry = (*BaseTableRegistry)(nil)

func NewBaseTableRegistry(prefixLength int) *BaseTableRegistry {
	return &BaseTableRegistry{
		prefixLength: prefixLength,
		registry:     make(map[string][]byte),
	}
}

// RegisterPrefix registers a table prefix and panics if a duplicate is found.
func (r *BaseTableRegistry) RegisterPrefix(value []byte) TablePrefix {
	if len(value) != r.prefixLength {
		panic(fmt.Sprintf("table prefix must be %d bytes, got %d", r.prefixLength, len(value)))
	}

	key := string(value)
	if existing, exists := r.registry[key]; exists {
		panic(fmt.Sprintf("duplicate table prefix detected: %v", existing))
	}
	r.registry[key] = value

	return TablePrefix{value: value}
}

// GetAllPrefixes returns all registered table prefixes for inspection.
func (r *BaseTableRegistry) GetAllPrefixes() map[string][]byte {
	result := make(map[string][]byte, len(r.registry))
	for k, v := range r.registry {
		result[k] = v
	}
	return result
}

// PrefixedTableRegistry wraps another TableRegistry and adds a prefix to all registered table prefixes.
type PrefixedTableRegistry struct {
	prefix []byte
	inner  TableRegistry
}

var _ TableRegistry = (*PrefixedTableRegistry)(nil)

func NewPrefixedTableRegistry(prefix []byte, inner TableRegistry) *PrefixedTableRegistry {
	return &PrefixedTableRegistry{
		prefix: prefix,
		inner:  inner,
	}
}

// RegisterPrefix registers a table prefix and panics if a duplicate is found.
func (r *PrefixedTableRegistry) RegisterPrefix(value []byte) TablePrefix {
	newValue := make([]byte, len(r.prefix)+len(value))
	copy(newValue, r.prefix)
	copy(newValue[len(r.prefix):], value)

	return r.inner.RegisterPrefix(newValue)
}
