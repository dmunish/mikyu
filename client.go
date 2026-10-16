package mikyu

import (
	"context"
	"io"
)

// Client is a handle to a Mikyu queue system, embedded in this process
// or reached over the network. It hands out queue handles, manages
// queue lifecycle, and reports telemetry.
//
// A Client is safe for concurrent use. Close releases whatever the
// client holds: one reference to a shared embedded node, or the pooled
// connections of a remote client. Closing does not delete data.
type Client interface {
	io.Closer
	QueueAccess
	QueueAdmin
	Telemetry
}

// QueueAccess hands out handles to queues. Two queue kinds and three
// verbs cover every way you might want to reach one:
//
//   - Log and Task resolve a queue that must already exist. A miss is
//     ErrQueueNotFound, never a quiet creation, so readers and
//     inspection tools cannot create state by typo.
//   - OpenLog and OpenTask resolve or create. This is the path most
//     applications want: the first call creates the queue with the
//     options you pass, and every later call returns the same queue.
//   - CreateLog and CreateTask, on QueueAdmin, declare a queue and
//     refuse with ErrQueueExists when the name is taken. Use them when
//     the queue's settings are infrastructure, not an accident of which
//     process got there first.
//
// Options passed to OpenLog and OpenTask apply only when the queue is
// created. When the queue already exists they are ignored, so callers
// can pass their preferred settings unconditionally.
type QueueAccess interface {
	// OpenLog returns the log queue named name, creating it with opts
	// when it does not exist yet.
	OpenLog(ctx context.Context, name string, opts ...QueueOption) (LogQueue, error)

	// OpenTask returns the task queue named name, creating it with opts
	// when it does not exist yet.
	OpenTask(ctx context.Context, name string, opts ...QueueOption) (TaskQueue, error)

	// Log returns a handle to an existing log queue. It never creates
	// one: a missing name is ErrQueueNotFound, and a name that belongs
	// to a task queue is ErrWrongKind.
	Log(ctx context.Context, name string) (LogQueue, error)

	// Task returns a handle to an existing task queue. It never creates
	// one: a missing name is ErrQueueNotFound, and a name that belongs
	// to a log queue is ErrWrongKind.
	Task(ctx context.Context, name string) (TaskQueue, error)
}

// QueueAdmin manages queue lifecycle and shape. These verbs are
// kind-agnostic: they take a queue name and work on whatever kind lives
// there.
type QueueAdmin interface {
	// CreateLog declares a log queue with the given settings and
	// returns a ready handle. A second create for the same name fails
	// with ErrQueueExists.
	CreateLog(ctx context.Context, name string, opts ...QueueOption) (LogQueue, error)

	// CreateTask declares a task queue with the given settings and
	// returns a ready handle. A second create for the same name fails
	// with ErrQueueExists. Compaction settings are rejected here: only
	// log queues compact.
	CreateTask(ctx context.Context, name string, opts ...QueueOption) (TaskQueue, error)

	// UpdateQueue merges a sparse patch into the queue's stored spec
	// and applies every mutable field to the live queue. A patch that
	// touches a create-only field fails with ErrImmutableField, and the
	// error names the field.
	UpdateQueue(ctx context.Context, name string, opts ...QueueOption) error

	// DescribeQueue reports everything true about one queue: its kind,
	// the spec it was created with, the derived values those settings
	// produce, the effective spec it actually runs with, and the
	// session defaults it inherits from.
	DescribeQueue(ctx context.Context, name string) (QueueDescription, error)

	// DeleteQueue removes a queue and all of its on-disk data. This
	// cannot be undone.
	DeleteQueue(ctx context.Context, name string) error

	// ListQueues returns every queue in the data directory, sorted by
	// name. The filesystem is the source of truth, so this includes
	// queues this process did not create.
	ListQueues(ctx context.Context) ([]QueueInfo, error)

	// Truncate drops whole log segments that end before minOffset and
	// returns the bytes freed. Segments are the unit, so the cut stops
	// at the first segment that straddles the offset, and it stops
	// short of any consumer group's position: data a consumer still
	// needs is never removed.
	//
	// Task queues do not truncate this way; their records are
	// checkpoints and bookkeeping the driver manages itself.
	Truncate(ctx context.Context, name string, minOffset uint64) (int64, error)
}

// Telemetry exposes the node's live measurements. Mikyu has exactly one
// observation surface: everything you can learn about a running system
// flows through Metrics and Schema.
type Telemetry interface {
	// Metrics returns a snapshot of live telemetry: build information,
	// runtime and process gauges, per-queue counters, rates and
	// histograms, and storage totals.
	Metrics(ctx context.Context) (Metrics, error)

	// Schema returns the parameter table this build accepts: every
	// configurable key with its type, layer, mutability, and the name
	// it carries on each surface (option, flag, env var). Embedded
	// clients return the compiled-in table; remote clients fetch it
	// from the server, so the two always agree.
	Schema(ctx context.Context) ([]Param, error)

	// SubscribeSnapshots delivers a Metrics snapshot on the returned
	// channel after every collection tick until the returned cancel
	// function runs. Embedded clients tap the local collector; remote
	// clients poll the server on the session's metrics interval.
	SubscribeSnapshots() (<-chan Metrics, func())
}
