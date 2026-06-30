package rpc

import (
	"errors"
	"fmt"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errorInfoDomain identifies the source of structured error details attached to
// gRPC statuses (errdetails.ErrorInfo.Domain).
const errorInfoDomain = "evrblk.com"

// Short aliases for the generated ErrorCode enum values, so callers can write
// rpc.NotFound instead of rpc.ErrorCode_NOT_FOUND.
const (
	OK                = ErrorCode_OK
	InvalidRequest    = ErrorCode_INVALID_REQUEST
	DeadlineExceeded  = ErrorCode_DEADLINE_EXCEEDED
	NotFound          = ErrorCode_NOT_FOUND
	AlreadyExists     = ErrorCode_ALREADY_EXISTS
	ResourceExhausted = ErrorCode_RESOURCE_EXHAUSTED
	Unimplemented     = ErrorCode_UNIMPLEMENTED
	Internal          = ErrorCode_INTERNAL
	IDCollision       = ErrorCode_ID_COLLISION
)

var _ error = &Error{}

// Error implements the error interface, rendering the code and message.
func (e *Error) Error() string {
	return fmt.Sprintf("%v %s", e.Code, e.Message)
}

// NewErrorWithContext builds an *Error carrying structured key/value context.
// The context is surfaced to gRPC clients by ErrorToGRPC (see errorInfoFor).
func NewErrorWithContext(code ErrorCode, message string, context map[string]string) *Error {
	ctxArr := make([]*ErrorContext, 0, len(context))
	for k, v := range context {
		ctxArr = append(ctxArr, &ErrorContext{
			Key:   k,
			Value: v,
		})
	}

	return &Error{
		Code:    code,
		Message: message,
		Context: ctxArr,
	}
}

// NewError builds an *Error with the given code and message and no context.
func NewError(code ErrorCode, message string) *Error {
	return &Error{
		Code:    code,
		Message: message,
		Context: []*ErrorContext{},
	}
}

// ErrorToGRPC translates an error into a gRPC status. An *Error (possibly
// wrapped) is mapped to the corresponding gRPC code with its context attached
// both to the message and as a structured errdetails.ErrorInfo; any other error
// becomes codes.Internal. It returns nil for a nil error or an OK-coded *Error.
// Internal and unmapped codes collapse to codes.Internal with no context, to
// avoid leaking internal details to clients.
func ErrorToGRPC(err error) error {
	if err == nil {
		return nil
	}

	berr := &Error{}
	if !errors.As(err, &berr) {
		return status.Errorf(codes.Internal, "%v", err)
	}

	if berr.Code == OK {
		return nil
	}

	// Internal (and any unmapped code, e.g. IDCollision, which is handled
	// server-side and should never reach a client) is returned as codes.Internal
	// without context or structured details, to avoid leaking internal info.
	grpcCode, mapped := grpcCodeFor(berr.Code)
	if !mapped {
		return status.Errorf(codes.Internal, "%v", err)
	}

	// The context is appended to the human-readable message (for logs) and also
	// attached as a structured errdetails.ErrorInfo so clients can read it
	// programmatically without parsing the message string.
	st := status.New(grpcCode, fmt.Sprintf("%v%s", err, formatContext(berr.Context)))

	if info := errorInfoFor(berr); info != nil {
		if withDetails, derr := st.WithDetails(info); derr == nil {
			st = withDetails
		}
	}

	return st.Err()
}

// grpcCodeFor maps a monstera ErrorCode to a gRPC code. The boolean is false for
// codes that intentionally collapse to codes.Internal (Internal, IDCollision,
// and any future unmapped code), so callers can suppress context/details for
// those.
func grpcCodeFor(code ErrorCode) (codes.Code, bool) {
	switch code {
	case InvalidRequest:
		return codes.InvalidArgument, true
	case DeadlineExceeded:
		return codes.DeadlineExceeded, true
	case NotFound:
		return codes.NotFound, true
	case AlreadyExists:
		return codes.AlreadyExists, true
	case ResourceExhausted:
		return codes.ResourceExhausted, true
	case Unimplemented:
		return codes.Unimplemented, true
	default:
		return codes.Internal, false
	}
}

// formatContext renders the error context as " (key: value, ...)" for inclusion
// in the human-readable status message. It returns an empty string when there is
// no context.
func formatContext(context []*ErrorContext) string {
	if len(context) == 0 {
		return ""
	}
	parts := make([]string, 0, len(context))
	for _, c := range context {
		parts = append(parts, fmt.Sprintf("%s: %s", c.Key, c.Value))
	}
	return fmt.Sprintf(" (%s)", strings.Join(parts, ", "))
}

// errorInfoFor builds an errdetails.ErrorInfo carrying the error context as
// structured metadata, or nil when there is no context to attach.
func errorInfoFor(berr *Error) *errdetails.ErrorInfo {
	if len(berr.Context) == 0 {
		return nil
	}
	metadata := make(map[string]string, len(berr.Context))
	for _, c := range berr.Context {
		metadata[c.Key] = c.Value
	}
	return &errdetails.ErrorInfo{
		Reason:   berr.Code.String(),
		Domain:   errorInfoDomain,
		Metadata: metadata,
	}
}
