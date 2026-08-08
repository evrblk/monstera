package grpc

import (
	"testing"

	"github.com/evrblk/monstera/transport/grpc/monsterapb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/encoding"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// vtSpy is a proto message whose vtprotobuf helpers record that they ran, so tests can
// tell which of the codec's two paths was taken. A generated message cannot show this:
// both paths produce identical bytes.
type vtSpy struct {
	*wrapperspb.StringValue
	marshalVTCalled   bool
	unmarshalVTCalled bool
}

func (s *vtSpy) MarshalVT() ([]byte, error) {
	s.marshalVTCalled = true
	return proto.Marshal(s.StringValue)
}

func (s *vtSpy) UnmarshalVT(data []byte) error {
	s.unmarshalVTCalled = true
	return proto.Unmarshal(data, s.StringValue)
}

func TestCodecIsRegisteredAsProto(t *testing.T) {
	require.IsType(t, vtprotoCodec{}, encoding.GetCodec(codecName))
}

func TestCodecPrefersVtprotoHelpers(t *testing.T) {
	require := require.New(t)

	codec := vtprotoCodec{}

	msg := &vtSpy{StringValue: wrapperspb.String("hello")}
	data, err := codec.Marshal(msg)
	require.NoError(err)
	require.True(msg.marshalVTCalled)

	got := &vtSpy{StringValue: &wrapperspb.StringValue{}}
	require.NoError(codec.Unmarshal(data, got))
	require.True(got.unmarshalVTCalled)
	require.Equal("hello", got.Value)
}

func TestCodecRoundTripsVtprotoMessage(t *testing.T) {
	require := require.New(t)

	codec := vtprotoCodec{}
	msg := &monsterapb.ReadRequest{
		Payload:         []byte("payload"),
		ApplicationName: "test",
		ShardId:         "shard-1",
		Hops:            2,
	}

	data, err := codec.Marshal(msg)
	require.NoError(err)

	got := &monsterapb.ReadRequest{}
	require.NoError(codec.Unmarshal(data, got))
	require.True(proto.Equal(msg, got))
}

// Messages generated without vtprotobuf helpers — everything else the importing
// process serves or calls over gRPC, including health checks and reflection — must
// still go through.
func TestCodecFallsBackToProtoForMessagesWithoutVtprotoHelpers(t *testing.T) {
	require := require.New(t)

	msg := wrapperspb.String("no vtprotobuf helpers here")
	require.NotImplements((*vtprotoMessage)(nil), msg)

	codec := vtprotoCodec{}
	data, err := codec.Marshal(msg)
	require.NoError(err)

	expected, err := proto.Marshal(msg)
	require.NoError(err)
	require.Equal(expected, data)

	got := &wrapperspb.StringValue{}
	require.NoError(codec.Unmarshal(data, got))
	require.True(proto.Equal(msg, got))
}

func TestCodecRejectsNonProtoMessages(t *testing.T) {
	require := require.New(t)

	codec := vtprotoCodec{}

	_, err := codec.Marshal("not a proto message")
	require.ErrorContains(err, "want proto.Message")

	require.ErrorContains(codec.Unmarshal(nil, &struct{}{}), "want proto.Message")
}
