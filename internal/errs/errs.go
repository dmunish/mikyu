// Package errs is the single sentinel-to-wire-code taxonomy shared by the wire server and client.
package errs

import (
	"errors"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/manager"
	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

// Entry pairs a Go sentinel with the stable wire code the server sends for it.
type Entry struct {
	Sentinel error
	Code     byte
}

// Table is the single source of truth for the sentinel-to-wire-code mapping.
var Table = []Entry{
	{protocol.ErrUnauthorized, protocol.ErrCodeUnauthorized},

	{engine.ErrQueueNotFound, protocol.ErrCodeQueueNotFound},
	{engine.ErrQueueExists, protocol.ErrCodeQueueExists},
	{spec.ErrInvalidConfig, protocol.ErrCodeInvalidConfig},
	{node.ErrRegistryCorrupt, protocol.ErrCodeInvalidConfig},
	{spec.ErrImmutableField, protocol.ErrCodeImmutableField},
	{spec.ErrSpecTooNew, protocol.ErrCodeSpecTooNew},
	{spec.ErrSpecTooLarge, protocol.ErrCodeSpecTooLarge},

	{engine.ErrOffsetNotFound, protocol.ErrCodeOffsetNotFound},
	{engine.ErrOffsetTruncated, protocol.ErrCodeOffsetTruncated},
	{engine.ErrMessageTooLarge, protocol.ErrCodeMessageTooLarge},
	{taskqueue.ErrTaskClosed, protocol.ErrCodeQueueClosed},
	{engine.ErrQueueClosed, protocol.ErrCodeQueueClosed},
	{engine.ErrNoData, protocol.ErrCodeNoData},
	{engine.ErrDiskFull, protocol.ErrCodeDiskFull},
	{engine.ErrCorruptRecord, protocol.ErrCodeCorruptRecord},
	{engine.ErrRecordTooLarge, protocol.ErrCodeRecordTooLarge},
	{engine.ErrRecordCompacted, protocol.ErrCodeRecordCompacted},
	{engine.ErrEncryptionKey, protocol.ErrCodeEncryptionKey},

	{taskqueue.ErrTaskNotAvailable, protocol.ErrCodeTaskNotAvailable},
	{taskqueue.ErrTaskNotFound, protocol.ErrCodeTaskNotFound},
	{taskqueue.ErrTaskNotInflight, protocol.ErrCodeTaskNotInflight},
	{taskqueue.ErrTaskStale, protocol.ErrCodeTaskStale},
	{taskqueue.ErrInvalidTask, protocol.ErrCodeTaskInvalid},
	{taskqueue.ErrCheckpointCorrupt, protocol.ErrCodeCheckpointCorrupt},
	{taskqueue.ErrQueueFull, protocol.ErrCodeQueueFull},

	{taskqueue.ErrDeadLetterNotFound, protocol.ErrCodeDeadLetterNotFound},
	{taskqueue.ErrDeadLetterStale, protocol.ErrCodeDeadLetterStale},
	{taskqueue.ErrArchiveFailed, protocol.ErrCodeArchiveFailed},

	{protocol.ErrTooManyConnections, protocol.ErrCodeTooManyConnections},
	{manager.ErrWrongKind, protocol.ErrCodeWrongKind},
}

// CodeFor returns the wire code for err, or false when err has no stable code.
func CodeFor(err error) (byte, bool) {
	for _, e := range Table {
		if errors.Is(err, e.Sentinel) {
			return e.Code, true
		}
	}
	return protocol.ErrCodeUnknown, false
}

// SentinelFor returns the canonical sentinel for a wire code, or false when the code has no sentinel.
func SentinelFor(code byte) (error, bool) {
	for _, e := range Table {
		if e.Code == code {
			return e.Sentinel, true
		}
	}
	return nil, false
}

// NonWireSentinels are sentinels deliberately kept off the wire, with their reason.
var NonWireSentinels = map[error]string{
	node.ErrLocked:                  "process-level lock contention is not a request error",
	node.ErrSessionDefaultsMismatch: "process-level pooling conflict is not a request error",
	protocol.ErrTruncatedBody:       "codec-level truncation is rejected before dispatch",
	protocol.ErrInvalidTask:         "codec-level task decode failure is rejected before dispatch",
}