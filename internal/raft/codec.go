package raft

import (
	"github.com/evrblk/monstera/internal/raft/raftpb"
	hraft "github.com/hashicorp/raft"
)

// LogCodec abstracts serialization of Raft log entries to/from bytes.
// This allows the raft store to remain independent of the specific
// serialization format (e.g. protobuf, msgpack).
type LogCodec interface {
	Encode(log *hraft.Log) ([]byte, error)
	Decode(data []byte, out *hraft.Log) error
}

// CommandCodec abstracts serialization of application commands to/from bytes.
// This allows the FSM to remain independent of the specific serialization format.
type CommandCodec interface {
	Encode(payload []byte) ([]byte, error)
	Decode(data []byte) ([]byte, error)
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

// protoCommandCodec encodes/decodes application commands as MonsteraCommand
// protobuf messages.
type protoCommandCodec struct{}

func (c *protoCommandCodec) Encode(payload []byte) ([]byte, error) {
	cmd := &raftpb.MonsteraCommand{Payload: payload}
	return cmd.MarshalVT()
}

func (c *protoCommandCodec) Decode(data []byte) ([]byte, error) {
	var cmd raftpb.MonsteraCommand
	if err := cmd.UnmarshalVT(data); err != nil {
		return nil, err
	}
	return cmd.Payload, nil
}
