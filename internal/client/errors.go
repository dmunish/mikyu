package client

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dmunish/mikyu/internal/errs"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

// ErrProtocol wraps an error reported by the server whose code the client does not recognize.
var ErrProtocol = errors.New("protocol error")

// ErrModeMismatch reports an option that does not apply in this client mode.
var ErrModeMismatch = errors.New("option does not apply in this client mode")

// ErrUnauthorized reports a credential the server rejected.
var ErrUnauthorized = protocol.ErrUnauthorized

// decodeKnown rebuilds a sentinel from a recognized wire code, preserving the
// server's detail that follows the sentinel's own text (e.g. the field name in
// an immutable-field error). A message with no extra detail yields the bare
// sentinel.
func decodeKnown(sentinel error, msg string) error {
	if detail, ok := strings.CutPrefix(msg, sentinel.Error()+": "); ok && detail != "" {
		return fmt.Errorf("%w: %s", sentinel, detail)
	}
	return sentinel
}

func decodeProtocolError(code byte, msg string) error {
	if sentinel, ok := errs.SentinelFor(code); ok {
		return decodeKnown(sentinel, msg)
	}
	return fmt.Errorf("%w: %s", ErrProtocol, msg)
}