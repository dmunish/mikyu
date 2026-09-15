// Package mikyu is the public SDK for the Mikyu message queue.
//
// Mikyu separates process configuration from data:
//
//   - mikyu.yaml lives with the binary and describes how a daemon runs:
//     bind target, TLS, metrics HTTP, and the ambient session defaults new
//     queues inherit on this host. The daemon reads it; embedded clients
//     do not.
//
//   - queues.json lives inside the data directory alongside the log
//     segments, index files, offset store, and lock. It records the name
//     and sparse spec of every queue and is written only by mikyu.
//
// Copying a data directory copies everything durable. Installing a binary
// copies the process's launch configuration. They travel independently
// because they are different things.
//
// Session defaults are ambient: they belong to the node that opened the
// directory, not to the data inside it. To make a queue's behavior
// portable across hosts, set the field on the queue itself.
package mikyu

import (
	"context"
	"io"
	"iter"

	"github.com/dmunish/mikyu/internal/client"
	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

// Client is a handle to a Mikyu server, embedded in this process or remote.
// Close releases it; an embedded handle drops one reference to the shared node.
type Client interface {
	io.Closer

	// CreateQueue creates a queue from the options given; unset fields inherit
	// the session defaults and then the built-ins.
	CreateQueue(ctx context.Context, name string, opts ...QueueOption) error

	// UpdateQueue merges a sparse patch into the queue's stored spec and applies
	// the mutable fields to the live queue. Create-only fields are rejected.
	UpdateQueue(ctx context.Context, name string, opts ...QueueOption) error

	// DescribeQueue returns the stored spec, effective spec, and session
	// defaults for one queue.
	DescribeQueue(ctx context.Context, name string) (QueueDescription, error)

	// DeleteQueue removes a queue and its on-disk data.
	DeleteQueue(ctx context.Context, name string) error

	// ListQueues returns the names of all queues in the node's data directory.
	ListQueues(ctx context.Context) ([]string, error)

	// Truncate drops log segments before minOffset and reports the bytes freed.
	Truncate(ctx context.Context, name string, minOffset uint64) (int64, error)

	// Metrics returns the node's live telemetry snapshot.
	Metrics(ctx context.Context) (Metrics, error)

	// LogQueue returns a handle for append, read, stream, tail, and groups.
	LogQueue(name string) (LogQueue, error)

	// TaskQueue returns a handle for at-least-once task processing.
	TaskQueue(name string) (TaskQueue, error)

	// Schema returns the parameter table the server accepts. Embedded clients
	// return spec.Table directly; remote clients fetch it over OpSchema.
	Schema(ctx context.Context) ([]Param, error)

	// SubscribeSnapshots streams live snapshots until the returned cancel runs.
	// Embedded clients delegate to the node's collector; remote clients poll
	// OpMetrics on the session's metrics interval.
	SubscribeSnapshots() (<-chan Metrics, func())
}

// Open creates an in-process Client over the data directory dir. Two Open calls
// on the same directory share one node; differing session defaults are rejected
// with ErrSessionDefaultsMismatch.
func Open(dir string, opts ...ClientOption) (Client, error) {
	return client.NewEmbedded(dir, opts...)
}

// Dial creates a lazy remote Client for target ("tcp://", "unix://", or
// "pipe://"). Dial does not connect; the first operation does.
func Dial(target string, opts ...ClientOption) (Client, error) {
	return client.NewRemote(target, opts...)
}

// Spec is a sparse set of queue or session parameters. Unset fields inherit
// from the next layer down: call over spec over session over built-in.
type Spec = spec.Spec

// EngineSpec covers the log-segment writer parameters of a Spec.
type EngineSpec = spec.EngineSpec

// RetentionSpec caps how much history may be deleted.
type RetentionSpec = spec.RetentionSpec

// TaskSpec holds the task-queue defaults of a Spec.
type TaskSpec = spec.TaskSpec

// Duration is a time span that serializes as a Go duration string such as "10ms".
type Duration = spec.Duration

// Message is one record read from a log queue.
type Message = engine.Message

// LogQueue provides log-oriented operations (append, streaming, tailing, groups).
type LogQueue = logqueue.ClientLogQueue

// Group tracks consumer offsets for a log queue.
type Group = logqueue.Group

// TaskQueue provides at-least-once delivery, visibility leases, retries,
// stale-delivery protection, checkpoint recovery, and a durable per-queue
// dead-letter queue.
type TaskQueue = client.TaskQueue

// Task represents a unit of work owned by a TaskQueue.
type Task = taskqueue.Task

// TaskRef identifies a task for Ack, Nack, Extend, and AckDeadLetter. Pass a
// Task for the strict stale-lease check; pass a TaskID to skip that check.
type TaskRef = taskqueue.TaskRef

// TaskID is a bare task identifier.
type TaskID = taskqueue.TaskID

// QueueDescription is the full truth about one queue: its stored spec, its
// effective spec, and the session defaults it inherits from.
type QueueDescription = client.QueueDescription

// Metrics is the node's live telemetry snapshot: runtime and process gauges,
// per-queue instruments, and totals.
type Metrics = metrics.Snapshot

// Error variables re-exported from the spec, engine, node, and driver layers.
var (
	// ErrImmutableField names a create-only field an update patch tried to change.
	ErrImmutableField = spec.ErrImmutableField
	// ErrInvalidConfig reports a malformed or out-of-range configuration value.
	ErrInvalidConfig = spec.ErrInvalidConfig
	// ErrSpecTooNew reports a spec schema number this build cannot decode.
	ErrSpecTooNew = spec.ErrSpecTooNew
	// ErrSpecTooLarge reports a spec payload over the wire or disk limit.
	ErrSpecTooLarge = spec.ErrSpecTooLarge

	// ErrLocked reports that the data directory is held by another process.
	ErrLocked = node.ErrLocked
	// ErrRegistryCorrupt reports a queues.json that could not be decoded.
	ErrRegistryCorrupt = node.ErrRegistryCorrupt
	// ErrSessionDefaultsMismatch reports a second Open with different session defaults.
	ErrSessionDefaultsMismatch = node.ErrSessionDefaultsMismatch

	// ErrQueueExists reports a create for a queue that already exists.
	ErrQueueExists = engine.ErrQueueExists
	// ErrQueueNotFound reports an operation on a missing queue.
	ErrQueueNotFound = engine.ErrQueueNotFound
	// ErrOffsetNotFound reports a read of an offset that never existed.
	ErrOffsetNotFound = engine.ErrOffsetNotFound
	// ErrOffsetTruncated reports a read of an offset that retention deleted.
	ErrOffsetTruncated = engine.ErrOffsetTruncated
	// ErrMessageTooLarge reports a payload over the queue's contract.
	ErrMessageTooLarge = engine.ErrMessageTooLarge
	// ErrQueueClosed reports an operation on a closed queue.
	ErrQueueClosed = engine.ErrQueueClosed
	// ErrCorruptRecord reports an unreadable record on disk.
	ErrCorruptRecord = engine.ErrCorruptRecord

	// ErrNoData reports that a fetch or read found no available records.
	ErrNoData = engine.ErrNoData
	// ErrRecordTooLarge reports a payload exceeding the segment size.
	ErrRecordTooLarge = engine.ErrRecordTooLarge
	// ErrProtocol reports a wire-level protocol error.
	ErrProtocol = client.ErrProtocol
	// ErrModeMismatch reports an option that does not apply in this client mode.
	ErrModeMismatch = client.ErrModeMismatch
	// ErrTooManyConnections reports a server at its concurrent-connection limit.
	ErrTooManyConnections = protocol.ErrTooManyConnections

	// ErrTaskNotAvailable reports a non-blocking dequeue with no task due.
	ErrTaskNotAvailable = taskqueue.ErrTaskNotAvailable
	// ErrTaskNotFound reports an unknown task ID.
	ErrTaskNotFound = taskqueue.ErrTaskNotFound
	// ErrTaskNotInflight reports a task that is not currently leased.
	ErrTaskNotInflight = taskqueue.ErrTaskNotInflight
	// ErrTaskStale reports a lease that expired or was redelivered.
	ErrTaskStale = taskqueue.ErrTaskStale
	// ErrTaskClosed reports an operation on a closed task queue.
	ErrTaskClosed = taskqueue.ErrTaskClosed
	// ErrInvalidTask reports an invalid task record or argument.
	ErrInvalidTask = taskqueue.ErrInvalidTask
	// ErrCheckpointCorrupt reports an unreadable task checkpoint.
	ErrCheckpointCorrupt = taskqueue.ErrCheckpointCorrupt
	// ErrDeadLetterNotFound reports an unknown dead-lettered task.
	ErrDeadLetterNotFound = taskqueue.ErrDeadLetterNotFound
	// ErrDeadLetterStale reports a dead-letter ack whose task changed.
	ErrDeadLetterStale = taskqueue.ErrDeadLetterStale
)

type _ = iter.Seq2[Message, error]
