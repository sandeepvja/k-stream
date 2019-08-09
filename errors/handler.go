package errors

import "context"

type StreamError struct {
	Key   []byte
	Value []byte
	Err   error
}

func (StreamError) Error() string {
	return ``
}

type ErrorHandler interface {
	Handle(ctx context.Context, err *StreamError)
}
