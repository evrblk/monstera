package grpc

import (
	"fmt"

	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

// codecName is the name of the default gRPC codec for the application/grpc+proto
// content subtype. Registering under this name replaces the codec for every gRPC
// client and server in the process, not just for monstera's own connections.
const codecName = "proto"

type vtprotoMessage interface {
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
}

// vtprotoCodec marshals messages with their vtprotobuf generated helpers when they
// have them, and falls back to the standard protobuf runtime when they do not.
//
// The fallback is what makes global registration safe. Anything importing this
// package also serves its own gRPC services, health checks and reflection through
// this codec, and those messages are generated without vtprotobuf helpers. A codec
// that errored on them would silently break every other gRPC service in the process.
type vtprotoCodec struct{}

func (vtprotoCodec) Marshal(v any) ([]byte, error) {
	if vt, ok := v.(vtprotoMessage); ok {
		return vt.MarshalVT()
	}

	m := messageV2Of(v)
	if m == nil {
		return nil, fmt.Errorf("failed to marshal, message is %T, want proto.Message", v)
	}
	return proto.Marshal(m)
}

func (vtprotoCodec) Unmarshal(data []byte, v any) error {
	if vt, ok := v.(vtprotoMessage); ok {
		return vt.UnmarshalVT(data)
	}

	m := messageV2Of(v)
	if m == nil {
		return fmt.Errorf("failed to unmarshal, message is %T, want proto.Message", v)
	}
	return proto.Unmarshal(data, m)
}

func (vtprotoCodec) Name() string {
	return codecName
}

// messageV2Of returns v as a protobuf v2 message, adapting v1 messages, or nil if v
// is not a protobuf message at all.
func messageV2Of(v any) proto.Message {
	switch v := v.(type) {
	case protoadapt.MessageV1:
		return protoadapt.MessageV2Of(v)
	case protoadapt.MessageV2:
		return v
	}
	return nil
}

func init() {
	encoding.RegisterCodec(vtprotoCodec{})
}
