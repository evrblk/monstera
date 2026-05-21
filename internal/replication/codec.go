package replication

import (
	"github.com/evrblk/monstera/internal/replication/replicationpb"
)

// CommandCodec abstracts serialization of application commands to/from bytes.
// This allows the FSM to remain independent of the specific serialization format.
type CommandCodec interface {
	Encode(cmd *replicationpb.MonsteraCommand) ([]byte, error)
	Decode(data []byte) (*replicationpb.MonsteraCommand, error)
}

// ProtoCommandCodec encodes/decodes application commands as MonsteraCommand
// protobuf messages.
type ProtoCommandCodec struct{}

var _ CommandCodec = (*ProtoCommandCodec)(nil)

func (c *ProtoCommandCodec) Encode(cmd *replicationpb.MonsteraCommand) ([]byte, error) {
	return cmd.MarshalVT()
}

func (c *ProtoCommandCodec) Decode(data []byte) (*replicationpb.MonsteraCommand, error) {
	var cmd replicationpb.MonsteraCommand
	if err := cmd.UnmarshalVT(data); err != nil {
		return nil, err
	}
	return &cmd, nil
}
