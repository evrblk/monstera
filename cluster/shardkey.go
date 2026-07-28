package cluster

import (
	"encoding/binary"
	"fmt"
)

// ShardKey identifies a point in an application's keyspace. Routing is a pure
// function of it: every key belongs to exactly one routable shard, the one
// whose [LowerKey, UpperKey] range contains it. Applications derive a shard
// key from a request however they like (typically a truncated hash of the
// entity id, see utils.GetShardKey); the framework never interprets it beyond
// range comparisons.
//
// A ShardKey is a plain uint32 — the keyspace is exactly [0, 2^32) (see
// KeyspacePerApplication) — so every value is valid and comparisons are
// ordinary integer comparisons. On the wire and in shard bounds it is encoded
// as 4 big-endian bytes, which sort the same way as the integers.
type ShardKey uint32

// Bytes returns the key's canonical 4-byte big-endian encoding (the form used
// by shard bounds and derived shard ids).
func (k ShardKey) Bytes() []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(k))
	return b
}

// String formats the key as 8 hex characters, matching the bounds notation in
// cluster configs (e.g. "80000000").
func (k ShardKey) String() string {
	return fmt.Sprintf("%08x", uint32(k))
}

// ShardKeyFromBytes decodes a shard key from its canonical 4-byte big-endian
// encoding.
func ShardKeyFromBytes(b []byte) (ShardKey, error) {
	if len(b) != 4 {
		return 0, fmt.Errorf("invalid shard key length %d: must be exactly 4 bytes", len(b))
	}
	return ShardKey(binary.BigEndian.Uint32(b)), nil
}

// LowerKey returns the shard's lower bound as a ShardKey.
func (s *Shard) LowerKey() ShardKey {
	return ShardKey(s.LowerBound)
}

// UpperKey returns the shard's (inclusive) upper bound as a ShardKey.
func (s *Shard) UpperKey() ShardKey {
	return ShardKey(s.UpperBound)
}

// ContainsKey reports whether the shard's [LowerKey, UpperKey] range contains k.
func (s *Shard) ContainsKey(k ShardKey) bool {
	return k >= s.LowerKey() && k <= s.UpperKey()
}
