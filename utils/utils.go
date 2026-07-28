// Package utils holds small helpers for building the keys that address data in
// a Monstera cluster: deriving shard keys from entity ids, concatenating typed
// values into a byte key, and fixed-width big-endian integer conversions.
package utils

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/evrblk/monstera/cluster"
)

// GetTruncatedHash returns the first size bytes of the SHA-256 hash of data.
// size must not exceed 32 (the SHA-256 digest length), or the slice expression
// panics. Useful for deriving a short, well-distributed key from arbitrary bytes.
func GetTruncatedHash(data []byte, size int) []byte {
	h := sha256.Sum256(data)
	return h[:size]
}

// GetShardKey derives a shard key from arbitrary bytes: the first 4 bytes of
// the SHA-256 hash of data, big-endian. Use it with ConcatBytes to build a
// shard key from an entity id, e.g. GetShardKey(ConcatBytes(accountId)).
func GetShardKey(data []byte) cluster.ShardKey {
	h := sha256.Sum256(data)
	return cluster.ShardKey(binary.BigEndian.Uint32(h[:4]))
}

// ConcatBytes concatenates the given items into a single big-endian byte slice,
// used to build a composite key from several fields (e.g. an account id followed
// by an entity id). Integers are encoded fixed-width (uint64/int64 as 8 bytes,
// uint32/int32 as 4), and []byte and string are appended as-is. It panics on a
// nil item or an unsupported type, since key layout is a programming decision and
// a malformed key must never be silently produced.
//
// It walks items twice: first to size the output buffer exactly, then to fill it,
// avoiding reallocations.
func ConcatBytes(items ...any) []byte {
	total := 0
	for _, item := range items {
		if item == nil {
			panic("nil item")
		}

		switch i := item.(type) {
		case uint64:
			total = total + 8
		case int64:
			total = total + 8
		case uint32:
			total = total + 4
		case int32:
			total = total + 4
		case []byte:
			total = total + len(i)
		case string:
			total = total + len(i)
		default:
			panic("wrong item type")
		}
	}

	key := make([]byte, total)

	c := 0
	for _, item := range items {
		if item == nil {
			panic("nil item")
		}

		switch i := item.(type) {
		case uint64:
			binary.BigEndian.PutUint64(key[c:], i)
			c = c + 8
		case int64:
			binary.BigEndian.PutUint64(key[c:], uint64(i))
			c = c + 8
		case uint32:
			binary.BigEndian.PutUint32(key[c:], i)
			c = c + 4
		case int32:
			binary.BigEndian.PutUint32(key[c:], uint32(i))
			c = c + 4
		case []byte:
			copy(key[c:], i)
			c = c + len(i)
		case string:
			copy(key[c:], i)
			c = c + len(i)
		}
	}

	return key
}

// Uint64ToBytes encodes i as 8 big-endian bytes. It is the inverse of BytesToUint64.
func Uint64ToBytes(i uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, i)
	return buf
}

// BytesToUint64 decodes the first 8 bytes of b as a big-endian uint64. It panics
// if b is shorter than 8 bytes.
func BytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Uint32ToBytes encodes i as 4 big-endian bytes. It is the inverse of BytesToUint32.
func Uint32ToBytes(i uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, i)
	return buf
}

// BytesToUint32 decodes the first 4 bytes of b as a big-endian uint32. It panics
// if b is shorter than 4 bytes.
func BytesToUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}
