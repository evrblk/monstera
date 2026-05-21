package raft

import (
	hraft "github.com/hashicorp/raft"

	"github.com/evrblk/monstera/internal/raft/raftpb"
)

// LogCodec abstracts serialization of Raft log entries to/from bytes.
// This allows the raft store to remain independent of the specific
// serialization format (e.g. protobuf, msgpack).
type LogCodec interface {
	Encode(log *hraft.Log) ([]byte, error)
	Decode(data []byte, out *hraft.Log) error
}

// protoLogCodec encodes/decodes Raft log entries as protobuf messages.
type protoLogCodec struct{}

func (c *protoLogCodec) Encode(log *hraft.Log) ([]byte, error) {
	return encodeLog(log).MarshalVT()
}

func (c *protoLogCodec) Decode(data []byte, out *hraft.Log) error {
	var msg raftpb.Log
	if err := msg.UnmarshalVT(data); err != nil {
		return err
	}
	*out = *decodeLog(&msg)
	return nil
}
