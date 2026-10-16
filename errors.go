package mikyu

import (
	"github.com/dmunish/mikyu/internal/client"
	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

var (
	ErrInvalidConfig           = spec.ErrInvalidConfig
	ErrImmutableField          = spec.ErrImmutableField
	ErrSpecTooNew              = spec.ErrSpecTooNew
	ErrSpecTooLarge            = spec.ErrSpecTooLarge
	ErrLocked                  = node.ErrLocked
	ErrRegistryCorrupt         = node.ErrRegistryCorrupt
	ErrSessionDefaultsMismatch = node.ErrSessionDefaultsMismatch
	ErrWrongKind               = node.ErrWrongKind
)

var (
	ErrQueueExists      = engine.ErrQueueExists
	ErrQueueNotFound    = engine.ErrQueueNotFound
	ErrQueueClosed      = engine.ErrQueueClosed
	ErrQueueFull        = taskqueue.ErrQueueFull
	ErrOffsetNotFound   = engine.ErrOffsetNotFound
	ErrOffsetTruncated  = engine.ErrOffsetTruncated
	ErrMessageTooLarge  = engine.ErrMessageTooLarge
	ErrRecordTooLarge   = engine.ErrRecordTooLarge
	ErrRecordCompacted  = engine.ErrRecordCompacted
	ErrNoData           = engine.ErrNoData
	ErrDiskFull         = engine.ErrDiskFull
	ErrCorruptRecord    = engine.ErrCorruptRecord
	ErrEncryptionKey    = engine.ErrEncryptionKey
	ErrTaskNotAvailable = taskqueue.ErrTaskNotAvailable
	ErrTaskNotFound     = taskqueue.ErrTaskNotFound
	ErrTaskNotInflight  = taskqueue.ErrTaskNotInflight
	ErrTaskStale        = taskqueue.ErrTaskStale
	ErrTaskClosed       = taskqueue.ErrTaskClosed
	ErrInvalidTask      = taskqueue.ErrInvalidTask
	ErrCheckpointCorrupt = taskqueue.ErrCheckpointCorrupt
	ErrDeadLetterNotFound = taskqueue.ErrDeadLetterNotFound
	ErrDeadLetterStale    = taskqueue.ErrDeadLetterStale
	ErrArchiveFailed      = taskqueue.ErrArchiveFailed
)

var (
	ErrProtocol            = client.ErrProtocol
	ErrModeMismatch        = client.ErrModeMismatch
	ErrUnauthorized        = client.ErrUnauthorized
	ErrTooManyConnections  = protocol.ErrTooManyConnections
)
