package server

import (
	"errors"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

func opErr(code byte, msg string) (byte, []byte) {
	return protocol.OpError, protocol.EncodeError(code, msg)
}

// taskErr maps a driver, spec, or node error to its wire error code. The
// message is preserved verbatim; the code is the stable contract.
func taskErr(err error) (byte, []byte) {
	switch {
	case errors.Is(err, protocol.ErrUnauthorized):
		return opErr(protocol.ErrCodeUnauthorized, err.Error())
	case errors.Is(err, engine.ErrQueueNotFound):
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	case errors.Is(err, spec.ErrInvalidConfig):
		return opErr(protocol.ErrCodeInvalidConfig, err.Error())
	case errors.Is(err, spec.ErrImmutableField):
		return opErr(protocol.ErrCodeImmutableField, err.Error())
	case errors.Is(err, spec.ErrSpecTooNew):
		return opErr(protocol.ErrCodeSpecTooNew, err.Error())
	case errors.Is(err, spec.ErrSpecTooLarge):
		return opErr(protocol.ErrCodeSpecTooLarge, err.Error())
	case errors.Is(err, node.ErrRegistryCorrupt):
		// The registry is a config artifact; a corrupt one is a config error
		// even though it surfaces on a request path.
		return opErr(protocol.ErrCodeInvalidConfig, err.Error())
	case errors.Is(err, node.ErrLocked):
		// Process-level, not request-level: no dedicated block.
		return opErr(protocol.ErrCodeUnknown, err.Error())
	case errors.Is(err, engine.ErrOffsetNotFound):
		return opErr(protocol.ErrCodeOffsetNotFound, err.Error())
	case errors.Is(err, engine.ErrOffsetTruncated):
		return opErr(protocol.ErrCodeOffsetTruncated, err.Error())
	case errors.Is(err, engine.ErrMessageTooLarge):
		return opErr(protocol.ErrCodeMessageTooLarge, err.Error())
	case errors.Is(err, engine.ErrQueueClosed), errors.Is(err, taskqueue.ErrTaskClosed):
		return opErr(protocol.ErrCodeQueueClosed, err.Error())
	case errors.Is(err, engine.ErrNoData):
		return opErr(protocol.ErrCodeNoData, err.Error())
	case errors.Is(err, taskqueue.ErrTaskNotAvailable):
		return opErr(protocol.ErrCodeTaskNotAvailable, err.Error())
	case errors.Is(err, taskqueue.ErrTaskNotFound):
		return opErr(protocol.ErrCodeTaskNotFound, err.Error())
	case errors.Is(err, taskqueue.ErrTaskNotInflight):
		return opErr(protocol.ErrCodeTaskNotInflight, err.Error())
	case errors.Is(err, taskqueue.ErrTaskStale):
		return opErr(protocol.ErrCodeTaskStale, err.Error())
	case errors.Is(err, taskqueue.ErrInvalidTask):
		return opErr(protocol.ErrCodeTaskInvalid, err.Error())
	case errors.Is(err, taskqueue.ErrCheckpointCorrupt):
		return opErr(protocol.ErrCodeCheckpointCorrupt, err.Error())
	case errors.Is(err, taskqueue.ErrDeadLetterNotFound):
		return opErr(protocol.ErrCodeDeadLetterNotFound, err.Error())
	case errors.Is(err, taskqueue.ErrDeadLetterStale):
		return opErr(protocol.ErrCodeDeadLetterStale, err.Error())
	default:
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
}
