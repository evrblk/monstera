package monsterax

import "encoding"

type Event struct {
}

type response interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

type request interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler

	ShardKey() []byte
}

type unshardedRequest interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

type UpdateResponse[T response] struct {
	Payload          T
	ApplicationError *Error
	Events           []Event
}

type ReadResponse[T response] struct {
	Payload          T
	ApplicationError *Error
}

type UpdateRequest[T request] struct {
	Payload T
}

type ReadRequest[T request] struct {
	Payload T
}

type UpdateUnshardedRequest[T unshardedRequest] struct {
	Payload T
}

type ReadUnshardedRequest[T unshardedRequest] struct {
	Payload T
}
