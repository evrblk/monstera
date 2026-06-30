package rpc

import "encoding"

// Event is emitted to the Monstera Pub/Sub bus to notify subscribers about
// changes that happened in the given transaction.
type Event struct {
}

// response is the payload of any RPC response: it must round-trip through binary
// (de)serialization for transport and storage.
type response interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

// request is the payload of a sharded RPC. In addition to being binary
// serializable, it exposes a ShardKey used to route the request to the owning
// shard.
type request interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler

	ShardKey() []byte
}

// unshardedRequest is the payload of an RPC that is not routed by shard (e.g. a
// cluster-wide or fan-out operation), so it carries no ShardKey.
type unshardedRequest interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

// UpdateResponse is the result of a mutating (update) RPC. ApplicationError, when
// set, is the deterministic domain error to return to the caller (see
// ErrorToGRPC); Events holds the events to emit to the Monstera Pub/Sub bus for
// the transaction.
type UpdateResponse[T response] struct {
	Payload          T
	ApplicationError *Error
	Events           []Event
}

// ReadResponse is the result of a read-only RPC. ApplicationError, when set, is
// the deterministic domain error to return to the caller (see ErrorToGRPC).
type ReadResponse[T response] struct {
	Payload          T
	ApplicationError *Error
}

// UpdateRequest wraps the payload of a sharded mutating RPC. Now is the request
// timestamp in Unix nanoseconds, carried on the wire by Request.now; the
// application core should read it instead of calling time.Now() at apply time so
// the update applies deterministically on every replica.
type UpdateRequest[T request] struct {
	Payload T
	Now     int64
}

// ReadRequest wraps the payload of a sharded read-only RPC. Now is the request
// timestamp in Unix nanoseconds (Request.now) used to evaluate effective state
// (e.g. expirations) as of that instant.
type ReadRequest[T request] struct {
	Payload T
	Now     int64
}

// UpdateUnshardedRequest wraps the payload of an unsharded mutating RPC. See
// UpdateRequest for the meaning of Now.
type UpdateUnshardedRequest[T unshardedRequest] struct {
	Payload T
	Now     int64
}

// ReadUnshardedRequest wraps the payload of an unsharded read-only RPC. See
// ReadRequest for the meaning of Now.
type ReadUnshardedRequest[T unshardedRequest] struct {
	Payload T
	Now     int64
}
