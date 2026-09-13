package client

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

// ErrProtocol wraps an error reported by the server whose code the client does not recognize.
var ErrProtocol = errors.New("protocol error")

// decodeKnown rebuilds a sentinel from a recognized wire code, preserving
// the server's detail that follows the sentinel's own text (e.g. the field
// name in an immutable-field error). A message with no extra detail yields
// the bare sentinel.
func decodeKnown(sentinel error, msg string) error {
	if detail, ok := strings.CutPrefix(msg, sentinel.Error()+": "); ok && detail != "" {
		return fmt.Errorf("%w: %s", sentinel, detail)
	}
	return sentinel
}

func decodeProtocolError(code byte, msg string) error {
	switch code {
	case protocol.ErrCodeQueueNotFound:
		return decodeKnown(engine.ErrQueueNotFound, msg)
	case protocol.ErrCodeQueueExists:
		return decodeKnown(engine.ErrQueueExists, msg)
	case protocol.ErrCodeInvalidConfig:
		return decodeKnown(spec.ErrInvalidConfig, msg)
	case protocol.ErrCodeImmutableField:
		return decodeKnown(spec.ErrImmutableField, msg)
	case protocol.ErrCodeSpecTooNew:
		return decodeKnown(spec.ErrSpecTooNew, msg)
	case protocol.ErrCodeSpecTooLarge:
		return decodeKnown(spec.ErrSpecTooLarge, msg)
	case protocol.ErrCodeOffsetNotFound:
		return decodeKnown(engine.ErrOffsetNotFound, msg)
	case protocol.ErrCodeOffsetTruncated:
		return decodeKnown(engine.ErrOffsetTruncated, msg)
	case protocol.ErrCodeMessageTooLarge:
		return decodeKnown(engine.ErrMessageTooLarge, msg)
	case protocol.ErrCodeQueueClosed:
		return decodeKnown(taskqueue.ErrTaskClosed, msg)
	case protocol.ErrCodeTaskNotAvailable:
		return decodeKnown(taskqueue.ErrTaskNotAvailable, msg)
	case protocol.ErrCodeTaskNotFound:
		return decodeKnown(taskqueue.ErrTaskNotFound, msg)
	case protocol.ErrCodeTaskNotInflight:
		return decodeKnown(taskqueue.ErrTaskNotInflight, msg)
	case protocol.ErrCodeTaskStale:
		return decodeKnown(taskqueue.ErrTaskStale, msg)
	case protocol.ErrCodeTaskInvalid:
		return decodeKnown(taskqueue.ErrInvalidTask, msg)
	case protocol.ErrCodeCheckpointCorrupt:
		return decodeKnown(taskqueue.ErrCheckpointCorrupt, msg)
	case protocol.ErrCodeDeadLetterNotFound:
		return decodeKnown(taskqueue.ErrDeadLetterNotFound, msg)
	case protocol.ErrCodeDeadLetterStale:
		return decodeKnown(taskqueue.ErrDeadLetterStale, msg)
	default:
		return fmt.Errorf("%w: %s", ErrProtocol, msg)
	}
}

func isRetryable(err error) bool {
	return errors.Is(err, engine.ErrOffsetNotFound) ||
		errors.Is(err, engine.ErrOffsetTruncated) || // optional; truncated offsets won't recover, but you may want to fail fast
		errors.Is(err, engine.ErrCorruptRecord) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF)
}
