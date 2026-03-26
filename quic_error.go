package qtls

import (
	"errors"
	"io"
	"net"

	"github.com/sagernet/quic-go"
	"github.com/sagernet/quic-go/http3"
)

type quicError struct {
	err error
}

func WrapError(err error) error {
	if err == nil {
		return nil
	}
	return &quicError{err: err}
}

func (e *quicError) Error() string {
	return e.err.Error()
}

func (e *quicError) Unwrap() error {
	return e.err
}

func (e *quicError) Is(target error) bool {
	if errors.Is(e.err, target) {
		return true
	}
	switch target {
	case net.ErrClosed:
		if streamErr, ok := errors.AsType[*quic.StreamError](e.err); ok {
			return !streamErr.Remote && streamErr.ErrorCode == 0
		}
		if transportErr, ok := errors.AsType[*quic.TransportError](e.err); ok {
			return transportErr.ErrorCode == quic.NoError
		}
		if appErr, ok := errors.AsType[*quic.ApplicationError](e.err); ok {
			return appErr.Remote && appErr.ErrorCode == 0
		}
		if h3Err, ok := errors.AsType[*http3.Error](e.err); ok {
			return h3Err.ErrorCode == http3.ErrCodeNoError || h3Err.ErrorCode == http3.ErrCodeRequestCanceled
		}
	case io.EOF:
		if streamErr, ok := errors.AsType[*quic.StreamError](e.err); ok {
			return !streamErr.Remote && streamErr.ErrorCode == 0
		}
	}
	return false
}
