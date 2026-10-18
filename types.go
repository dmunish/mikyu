package mikyu

import (
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/node"
)

// QueueKind identifies a queue's structural kind. The kind decides
// which interface a queue speaks: LogQueue for QueueKindLog, TaskQueue
// for QueueKindTask. It is part of the queue's identity on disk, not a
// setting.
type QueueKind = node.Kind

const (
	// QueueKindLog marks log queues.
	QueueKindLog = node.KindLog

	// QueueKindTask marks task queues.
	QueueKindTask = node.KindTask
)

// Record is one record from a log queue.
type Record struct {
	// Offset is the record's permanent position in the queue. Offsets
	// start at zero and only grow.
	Offset uint64

	// ID is a unique identifier assigned at append time. It is stable
	// across restarts and safe to store elsewhere as a reference.
	ID string

	// Timestamp is when the record was appended.
	Timestamp time.Time

	// Group is the record's compaction key, set through WithRecordKey.
	// When a queue compacts by keys, only the newest record per Group
	// survives; an empty Group means the record is never compacted
	// away.
	Group []byte

	// Payload is the bytes the producer appended.
	Payload []byte
}

// Ref identifies a task to Ack, Nack, Extend, AckDeadLetter, and
// Archive. The implementation set is closed: Task and TaskID.
//
// The two trade strictness for convenience. A Task carries the full
// delivery identity and gets the strict stale-lease check, which is
// what normal workers want. A TaskID is just the identifier and skips
// the check, which suits admin tools reconciling a queue they never
// dequeued from. Either way the task must still be in flight under a
// live lease.
type Ref interface{ taskRef() }

// TaskRef is the older name for Ref. Use Ref in new code.
type TaskRef = Ref

// Task is one unit of work in a task queue, as enqueued and as
// delivered. Passing a Task as a Ref runs the strict stale-lease check
// on Ack, Nack, Extend, AckDeadLetter, and Archive.
type Task struct {
	// ID identifies the task itself. It is assigned at enqueue time and
	// survives every redelivery.
	ID string

	// Payload is the work to be done, exactly as enqueued.
	Payload []byte

	// Attempts is how many times this task has been delivered so far.
	// The first delivery is attempt 1.
	Attempts uint32

	// MaxAttempts is the delivery budget after which the task moves to
	// the dead-letter queue. It comes from the queue's max_attempts
	// setting unless the task was enqueued with WithTaskMaxAttempts.
	MaxAttempts uint32

	// Lease is when the current delivery's visibility lease expires.
	// After that moment the task can be delivered again; Extend moves
	// it forward. A zero Lease means the task is not in flight.
	Lease time.Time

	// ScheduledAt is when the task first became deliverable. Tasks
	// enqueued with WithScheduledAt wait until then.
	ScheduledAt time.Time

	// DeliveryID identifies this particular delivery. It changes on
	// every redelivery and is half of the strict stale-lease identity:
	// an ack from a superseded delivery is rejected.
	DeliveryID string

	// Offset is the task's position in the queue's underlying log. It
	// is part of the strict identity and orders dead-lettered tasks.
	Offset uint64

	// Key is the optional key attached at enqueue time with WithKey.
	// It travels with the task into the archive.
	Key []byte

	// Stalled is how many deliveries were lost to an expired lease
	// before the current one. Zero means the task has always been
	// processed within its lease.
	Stalled uint32
}

func (Task) taskRef() {}

// TaskID is a bare task identifier. Use it as a Ref when you know the
// ID but not the delivery, typically in admin flows; the strict
// stale-lease check is skipped, but the task must still be in flight.
type TaskID string

func (TaskID) taskRef() {}

// QueueInfo names a queue and gives its kind, as ListQueues reports it.
type QueueInfo struct {
	// Name is the queue's name.
	Name string

	// Kind is the queue's structural kind.
	Kind QueueKind
}

// QueueDescription is the complete truth about one queue, as
// DescribeQueue reports it.
type QueueDescription struct {
	// Name is the queue's name.
	Name string

	// Kind is the queue's structural kind.
	Kind QueueKind

	// Spec is the sparse spec the queue was created and updated with:
	// exactly the fields someone set, nothing else.
	Spec Spec

	// Derived is Spec with every unset field resolved against the
	// built-in defaults, ignoring session defaults. It answers "what
	// would this queue do on any host?".
	Derived Spec

	// Effective is the spec the queue actually runs with: the stored
	// spec over the session defaults over the built-ins.
	Effective Spec

	// Defaults are the session defaults the queue inherits from the
	// node that opened the directory.
	Defaults Spec
}

// Metrics is a snapshot of live telemetry: what the node and its
// queues were doing at one instant.
//
// The snapshot carries build information, runtime and process gauges
// (goroutines, heap, CPU, file descriptors), garbage collector and
// scheduler readings, lock contention, per-queue counters, gauges,
// rates and histograms, and storage totals. Queue views know their kind
// and carry the fields that kind owns: a task queue reports its
// in-flight and dead-letter counts, a log queue reports its segments
// and consumer groups.
//
// Read one snapshot through Client.Metrics, or a stream of them
// through Client.SubscribeSnapshots. History is available on a daemon's
// metrics HTTP endpoint.
type Metrics = metrics.Snapshot
