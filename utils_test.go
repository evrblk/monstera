package monstera

import (
	"encoding/binary"
	"testing"
)

func TestConcatBytes(t *testing.T) {
	tests := []struct {
		name     string
		items    []interface{}
		expected []byte
		panics   bool
	}{
		{
			name:     "empty items",
			items:    []interface{}{},
			expected: []byte{},
			panics:   false,
		},
		{
			name:     "single uint64",
			items:    []interface{}{uint64(1234567890)},
			expected: []byte{0, 0, 0, 0, 73, 150, 2, 210},
			panics:   false,
		},
		{
			name:     "single int64",
			items:    []interface{}{int64(-1234567890)},
			expected: []byte{255, 255, 255, 255, 182, 105, 253, 46},
			panics:   false,
		},
		{
			name:     "single uint32",
			items:    []interface{}{uint32(1234567890)},
			expected: []byte{73, 150, 2, 210},
			panics:   false,
		},
		{
			name:     "single int32",
			items:    []interface{}{int32(-1234567890)},
			expected: []byte{182, 105, 253, 46},
			panics:   false,
		},
		{
			name:     "single byte slice",
			items:    []interface{}{[]byte{1, 2, 3, 4}},
			expected: []byte{1, 2, 3, 4},
			panics:   false,
		},
		{
			name:     "single string",
			items:    []interface{}{"hello"},
			expected: []byte("hello"),
			panics:   false,
		},
		{
			name: "mixed types",
			items: []interface{}{
				uint64(1234567890),
				[]byte{1, 2, 3},
				"world",
				uint32(987654321),
			},
			expected: []byte{
				0, 0, 0, 0, 73, 150, 2, 210, // uint64(1234567890)
				1, 2, 3, // []byte{1, 2, 3}
				119, 111, 114, 108, 100, // "world"
				58, 222, 104, 177, // uint32(987654321)
			},
			panics: false,
		},
		{
			name:     "nil item",
			items:    []interface{}{uint64(1), nil, []byte{1}},
			expected: nil,
			panics:   true,
		},
		{
			name:     "unsupported type",
			items:    []interface{}{uint64(1), "hello", 42}, // int is not supported
			expected: nil,
			panics:   true,
		},
		{
			name:     "unsupported type float",
			items:    []interface{}{uint64(1), 3.14}, // float64 is not supported
			expected: nil,
			panics:   true,
		},
		{
			name:     "unsupported type bool",
			items:    []interface{}{uint64(1), true}, // bool is not supported
			expected: nil,
			panics:   true,
		},
		{
			name: "large values",
			items: []interface{}{
				uint64(0xFFFFFFFFFFFFFFFF),
				int64(-0x7FFFFFFFFFFFFFFF),
				uint32(0xFFFFFFFF),
				int32(-0x7FFFFFFF),
			},
			expected: []byte{
				255, 255, 255, 255, 255, 255, 255, 255, // uint64 max
				128, 0, 0, 0, 0, 0, 0, 1, // int64 min + 1
				255, 255, 255, 255, // uint32 max
				128, 0, 0, 1, // int32 min + 1
			},
			panics: false,
		},
		{
			name:     "empty string",
			items:    []interface{}{""},
			expected: []byte{},
			panics:   false,
		},
		{
			name:     "empty byte slice",
			items:    []interface{}{[]byte{}},
			expected: []byte{},
			panics:   false,
		},
		{
			name: "zero values",
			items: []interface{}{
				uint64(0),
				int64(0),
				uint32(0),
				int32(0),
				[]byte{},
				"",
			},
			expected: []byte{
				0, 0, 0, 0, 0, 0, 0, 0, // uint64(0)
				0, 0, 0, 0, 0, 0, 0, 0, // int64(0)
				0, 0, 0, 0, // uint32(0)
				0, 0, 0, 0, // int32(0)
				// empty []byte and string add nothing
			},
			panics: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if tt.panics && r == nil {
					t.Errorf("expected panic but got none")
				}
				if !tt.panics && r != nil {
					t.Errorf("unexpected panic: %v", r)
				}
			}()

			result := ConcatBytes(tt.items...)
			if !tt.panics {
				if len(result) != len(tt.expected) {
					t.Errorf("length mismatch: got %d, want %d", len(result), len(tt.expected))
					return
				}
				for i, b := range result {
					if b != tt.expected[i] {
						t.Errorf("byte at index %d: got %d, want %d", i, b, tt.expected[i])
					}
				}
			}
		})
	}
}

func TestConcatBytesManualVerification(t *testing.T) {
	// Test with known values to manually verify binary encoding
	items := []interface{}{
		uint64(0x1234567890ABCDEF),
		uint32(0x12345678),
		[]byte{0xAA, 0xBB, 0xCC},
		"test",
	}

	result := ConcatBytes(items...)

	// Verify uint64 encoding
	expectedUint64 := make([]byte, 8)
	binary.BigEndian.PutUint64(expectedUint64, 0x1234567890ABCDEF)
	for i := 0; i < 8; i++ {
		if result[i] != expectedUint64[i] {
			t.Errorf("uint64 encoding mismatch at index %d: got %d, want %d", i, result[i], expectedUint64[i])
		}
	}

	// Verify uint32 encoding
	expectedUint32 := make([]byte, 4)
	binary.BigEndian.PutUint32(expectedUint32, 0x12345678)
	for i := 0; i < 4; i++ {
		if result[i+8] != expectedUint32[i] {
			t.Errorf("uint32 encoding mismatch at index %d: got %d, want %d", i+8, result[i+8], expectedUint32[i])
		}
	}

	// Verify byte slice
	expectedBytes := []byte{0xAA, 0xBB, 0xCC}
	for i := 0; i < 3; i++ {
		if result[i+12] != expectedBytes[i] {
			t.Errorf("byte slice mismatch at index %d: got %d, want %d", i+12, result[i+12], expectedBytes[i])
		}
	}

	// Verify string
	expectedString := []byte("test")
	for i := 0; i < 4; i++ {
		if result[i+15] != expectedString[i] {
			t.Errorf("string mismatch at index %d: got %d, want %d", i+15, result[i+15], expectedString[i])
		}
	}
}

func BenchmarkConcatBytes(b *testing.B) {
	items := []interface{}{
		uint64(1234567890),
		[]byte{1, 2, 3, 4, 5},
		"hello world",
		uint32(987654321),
		int64(-1234567890),
		"another string",
		[]byte{6, 7, 8, 9, 10},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ConcatBytes(items...)
	}
}
