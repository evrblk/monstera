package rpc

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestErrorToGRPC_Nil(t *testing.T) {
	assert.Nil(t, ErrorToGRPC(nil))
}

func TestErrorToGRPC_OK(t *testing.T) {
	assert.Nil(t, ErrorToGRPC(NewError(OK, "fine")))
}

func TestErrorToGRPC_CodeMapping(t *testing.T) {
	cases := []struct {
		code ErrorCode
		want codes.Code
	}{
		{InvalidRequest, codes.InvalidArgument},
		{DeadlineExceeded, codes.DeadlineExceeded},
		{NotFound, codes.NotFound},
		{AlreadyExists, codes.AlreadyExists},
		{ResourceExhausted, codes.ResourceExhausted},
		{Unimplemented, codes.Unimplemented},
		{Internal, codes.Internal},
		{IDCollision, codes.Internal}, // unmapped -> internal
	}

	for _, c := range cases {
		t.Run(c.code.String(), func(t *testing.T) {
			err := ErrorToGRPC(NewError(c.code, "boom"))
			assert.Equal(t, c.want, status.Code(err))
		})
	}
}

func TestErrorToGRPC_NonMonsteraError(t *testing.T) {
	err := ErrorToGRPC(errors.New("some infra failure"))
	assert.Equal(t, codes.Internal, status.Code(err))
}

func TestErrorToGRPC_AttachesContextAsErrorInfo(t *testing.T) {
	berr := NewErrorWithContext(NotFound, "namespace not found", map[string]string{
		"namespace_name": "foo",
	})

	err := ErrorToGRPC(berr)

	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.NotFound, st.Code())

	// Context is appended to the human-readable message...
	assert.Contains(t, st.Message(), "namespace_name: foo")

	// ...and attached as structured ErrorInfo metadata.
	var info *errdetails.ErrorInfo
	for _, d := range st.Details() {
		if ei, ok := d.(*errdetails.ErrorInfo); ok {
			info = ei
		}
	}
	require.NotNil(t, info)
	assert.Equal(t, "NOT_FOUND", info.GetReason())
	assert.Equal(t, errorInfoDomain, info.GetDomain())
	assert.Equal(t, map[string]string{"namespace_name": "foo"}, info.GetMetadata())
}

func TestErrorToGRPC_NoContextNoDetails(t *testing.T) {
	err := ErrorToGRPC(NewError(NotFound, "namespace not found"))

	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Empty(t, st.Details())
}

// Internal errors must not leak context to the client, neither in the message
// nor as structured details.
func TestErrorToGRPC_InternalSuppressesContext(t *testing.T) {
	berr := NewErrorWithContext(Internal, "boom", map[string]string{
		"secret": "do-not-leak",
	})

	err := ErrorToGRPC(berr)

	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Internal, st.Code())
	assert.Empty(t, st.Details())
	assert.NotContains(t, st.Message(), "do-not-leak")
}
