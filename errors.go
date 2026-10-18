package mikyu

import (
	"errors"

	"github.com/dmunish/mikyu/internal/client"
	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

// Every error in this list is a stable sentinel: compare with errors.Is
// and the comparison survives wrapping, transport, and release
// changes. The human-readable text is not a contract; the identity is.
var (
	// ErrInvalidConfig reports a malformed or out-of-range
	// configuration value. The error names the key.
	ErrInvalidConfig = spec.ErrInvalidConfig

	// ErrImmutableField reports an update patch that touched a
	// create-only field. The error names the field.
	ErrImmutableField = spec.ErrImmutableField

	// ErrSpecTooNew reports a spec schema version this build cannot
	// decode. Upgrade the client.
	ErrSpecTooNew = spec.ErrSpecTooNew

	// ErrSpecTooLarge reports a spec payload over the wire or disk
	// limit.
	ErrSpecTooLarge = spec.ErrSpecTooLarge

	// ErrLocked reports that the data directory is held by another
	// process. Only one writer may open a directory.
	ErrLocked = node.ErrLocked

	// ErrRegistryCorrupt reports a queues.json that could not be
	// decoded. The queue directories themselves are unaffected.
	ErrRegistryCorrupt = node.ErrRegistryCorrupt

	// ErrSessionDefaultsMismatch reports a second Open of the same
	// directory with different session defaults or a different
	// encryption key file.
	ErrSessionDefaultsMismatch = node.ErrSessionDefaultsMismatch

	// ErrWrongKind reports a queue addressed as the wrong kind, for
	// example a task queue name passed to Client.Log.
	ErrWrongKind = node.ErrWrongKind
)

// Queue, record, and task failures. These are what queue operations
// return when the system is healthy but the request cannot be served.
var (
	// ErrQueueExists reports a create for a queue name that already
	// exists.
	ErrQueueExists = engine.ErrQueueExists

	// ErrQueueNotFound reports an operation on a queue that does not
	// exist. Resolution never creates queues, so this is also what a
	// typo looks like.
	ErrQueueNotFound = engine.ErrQueueNotFound

	// ErrQueueClosed reports an operation on a log queue after Close.
	ErrQueueClosed = engine.ErrQueueClosed

	// ErrQueueFull reports an enqueue refused because the queue's
	// max_in_flight bound is saturated.
	ErrQueueFull = taskqueue.ErrQueueFull

	// ErrOffsetNotFound reports a read of an offset that never existed
	// or lies past the end of the stream.
	ErrOffsetNotFound = engine.ErrOffsetNotFound

	// ErrOffsetTruncated reports a read of an offset that retention or
	// Truncate removed.
	ErrOffsetTruncated = engine.ErrOffsetTruncated

	// ErrMessageTooLarge reports a payload over the queue's max_payload
	// setting.
	ErrMessageTooLarge = engine.ErrMessageTooLarge

	// ErrRecordTooLarge reports a record that cannot fit in one
	// segment even though it respects max_payload; raise max_segment.
	ErrRecordTooLarge = engine.ErrRecordTooLarge

	// ErrRecordCompacted reports a read of a record that keyed
	// compaction replaced with a newer version of the same Group.
	ErrRecordCompacted = engine.ErrRecordCompacted

	// ErrNoData reports that a non-blocking fetch found nothing to
	// return. It is a normal condition, not a failure.
	ErrNoData = engine.ErrNoData

	// ErrDiskFull reports a write the storage device refused.
	ErrDiskFull = engine.ErrDiskFull

	// ErrCorruptRecord reports a record that failed validation on
	// read, including a payload that does not decrypt under the
	// queue's keys.
	ErrCorruptRecord = engine.ErrCorruptRecord

	// ErrEncryptionKey reports that a queue sealed with encryption aes
	// cannot be opened because the master keys are missing or wrong.
	// The data is unreadable without them.
	ErrEncryptionKey = engine.ErrEncryptionKey

	// ErrTaskNotAvailable reports a non-blocking dequeue with no task
	// due. It is a normal condition, not a failure.
	ErrTaskNotAvailable = taskqueue.ErrTaskNotAvailable

	// ErrTaskNotFound reports a task ID the queue does not know.
	ErrTaskNotFound = taskqueue.ErrTaskNotFound

	// ErrTaskNotInflight reports a task that exists but is not
	// currently leased, for example one that was acked already.
	ErrTaskNotInflight = taskqueue.ErrTaskNotInflight

	// ErrTaskStale reports an ack or nack from a delivery that no
	// longer owns the task: the lease expired and the task was
	// redelivered, or a newer delivery superseded it.
	ErrTaskStale = taskqueue.ErrTaskStale

	// ErrTaskClosed reports an operation on a task queue after Close.
	ErrTaskClosed = taskqueue.ErrTaskClosed

	// ErrInvalidTask reports an invalid task record or argument.
	ErrInvalidTask = taskqueue.ErrInvalidTask

	// ErrCheckpointCorrupt reports a task checkpoint that could not be
	// trusted during recovery.
	ErrCheckpointCorrupt = taskqueue.ErrCheckpointCorrupt

	// ErrDeadLetterNotFound reports a dead-letter operation on a task
	// that is not in the dead-letter queue.
	ErrDeadLetterNotFound = taskqueue.ErrDeadLetterNotFound

	// ErrDeadLetterStale reports a dead-letter ack whose task changed
	// underneath the caller.
	ErrDeadLetterStale = taskqueue.ErrDeadLetterStale

	// ErrArchiveFailed reports that a task could not be moved to the
	// archive log. The task stays where it was.
	ErrArchiveFailed = taskqueue.ErrArchiveFailed
)

// Transport failures. These describe the connection between a remote
// client and its server, not the queues behind it.
var (
	// ErrProtocol reports a malformed exchange: the two sides do not
	// agree on the wire format.
	ErrProtocol = client.ErrProtocol

	// ErrModeMismatch reports an option that does not apply to the
	// client mode, such as WithTarget on an embedded Open.
	ErrModeMismatch = client.ErrModeMismatch

	// ErrUnauthorized reports a credential the server rejected.
	ErrUnauthorized = client.ErrUnauthorized

	// ErrTooManyConnections reports a server already at its
	// concurrent-connection limit.
	ErrTooManyConnections = protocol.ErrTooManyConnections
)

// The Is helpers group sentinels that callers usually treat the same
// way. Each one documents exactly which errors it matches, and all of
// them use errors.Is, so wrapping anywhere in the chain is respected.
// A nil error matches nothing.

// IsNotFound reports whether err means "this thing does not exist".
// It matches ErrQueueNotFound, ErrTaskNotFound, and
// ErrDeadLetterNotFound. It deliberately does not match
// ErrOffsetNotFound: a missing queue is a naming problem, while a
// missing offset is usually a reading-order problem.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrQueueNotFound) ||
		errors.Is(err, ErrTaskNotFound) ||
		errors.Is(err, ErrDeadLetterNotFound)
}

// IsStale reports whether err means "someone else owns this now". It
// matches ErrTaskStale and ErrDeadLetterStale, the two errors that
// come from losing a race for a task.
func IsStale(err error) bool {
	return errors.Is(err, ErrTaskStale) || errors.Is(err, ErrDeadLetterStale)
}

// IsFull reports whether err means "the queue is at a bound". It
// matches ErrQueueFull.
func IsFull(err error) bool { return errors.Is(err, ErrQueueFull) }

// IsClosed reports whether err means "this queue is shut down". It
// matches ErrQueueClosed and ErrTaskClosed.
func IsClosed(err error) bool {
	return errors.Is(err, ErrQueueClosed) || errors.Is(err, ErrTaskClosed)
}

// IsTooLarge reports whether err means "this payload cannot fit". It
// matches ErrMessageTooLarge and ErrRecordTooLarge.
func IsTooLarge(err error) bool {
	return errors.Is(err, ErrMessageTooLarge) || errors.Is(err, ErrRecordTooLarge)
}

// IsCorrupt reports whether err means "stored data failed
// validation". It matches ErrCorruptRecord and ErrCheckpointCorrupt.
func IsCorrupt(err error) bool {
	return errors.Is(err, ErrCorruptRecord) || errors.Is(err, ErrCheckpointCorrupt)
}
