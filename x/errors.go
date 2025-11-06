package monsterax

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	OK                = ErrorCode_OK
	InvalidArgument   = ErrorCode_INVALID_ARGUMENT
	DeadlineExceeded  = ErrorCode_DEADLINE_EXCEEDED
	NotFound          = ErrorCode_NOT_FOUND
	AlreadyExists     = ErrorCode_ALREADY_EXISTS
	ResourceExhausted = ErrorCode_RESOURCE_EXHAUSTED
	Unimplemented     = ErrorCode_UNIMPLEMENTED
	Internal          = ErrorCode_INTERNAL
)

var _ error = &Error{}

func (e *Error) Error() string {
	return fmt.Sprintf("%v %s", e.Code, e.Message)
}

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

func NewError(code ErrorCode, message string) *Error {
	return &Error{
		Code:    code,
		Message: message,
		Context: []*ErrorContext{},
	}
}

func WrapError(err error) *Error {
	if err != nil {
		berr := &Error{}
		if errors.As(err, &berr) {
			return berr
		} else {
			return NewError(Internal, err.Error())
		}
	} else {
		return nil
	}
}

func ErrorToGRPC(err error) error {
	if err != nil {
		berr := &Error{}
		if errors.As(err, &berr) {
			switch berr.Code {
			case OK:
				return nil
			case InvalidArgument:
				return status.Errorf(codes.InvalidArgument, "%v", err)
			case DeadlineExceeded:
				return status.Errorf(codes.DeadlineExceeded, "%v", err)
			case NotFound:
				return status.Errorf(codes.NotFound, "%v", err)
			case AlreadyExists:
				return status.Errorf(codes.AlreadyExists, "%v", err)
			case ResourceExhausted:
				return status.Errorf(codes.ResourceExhausted, "%v", err)
			case Unimplemented:
				return status.Errorf(codes.Unimplemented, "%v", err)
			case Internal:
				return status.Errorf(codes.Internal, "%v", err) // TODO do not return internal error details
			default:
				return status.Errorf(codes.Internal, "%v", err)
			}
		} else {
			return status.Errorf(codes.Internal, "%v", err)
		}
	} else {
		return nil
	}
}
