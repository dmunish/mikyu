package client

import (
	"context"
	"io"

	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
	"github.com/dmunish/mikyu/internal/spec"
)

// QueueInfo is a queue's name and its kind.
type QueueInfo = node.QueueInfo

// QueueDescription is the full truth about one queue: its kind, stored spec, derived spec, effective spec, and the session defaults it inherits.
type QueueDescription struct {
	Name      string    `json:"name"`
	Kind      node.Kind `json:"kind"`
	Spec      spec.Spec `json:"spec"`
	Derived   spec.Spec `json:"derived"`
	Effective spec.Spec `json:"effective"`
	Defaults  spec.Spec `json:"defaults"`
}

// Client is the shared surface of embedded and remote clients.
type Client interface {
	io.Closer

	// CreateLogQueue declares a log queue and returns a usable handle.
	CreateLogQueue(ctx context.Context, name string, opts ...QueueOption) (logqueue.Log, error)
	// CreateTaskQueue declares a task queue and returns a usable handle.
	CreateTaskQueue(ctx context.Context, name string, opts ...QueueOption) (taskqueue.Queue, error)
	// LogQueue returns a handle to an existing log queue or ErrWrongKind/ErrQueueNotFound.
	LogQueue(name string) (logqueue.Log, error)
	// TaskQueue returns a handle to an existing task queue or ErrWrongKind/ErrQueueNotFound.
	TaskQueue(name string) (taskqueue.Queue, error)

	// UpdateQueue merges a sparse patch into the queue's stored spec.
	UpdateQueue(ctx context.Context, name string, opts ...QueueOption) error
	// DescribeQueue returns the kind, stored spec, effective spec, and session defaults.
	DescribeQueue(ctx context.Context, name string) (QueueDescription, error)
	// DeleteQueue removes a queue and its on-disk data.
	DeleteQueue(ctx context.Context, name string) error
	// ListQueues returns every queue's name and kind.
	ListQueues(ctx context.Context) ([]QueueInfo, error)
	// Truncate drops log segments before minOffset and reports the bytes freed.
	Truncate(ctx context.Context, name string, minOffset uint64) (int64, error)

	// Metrics returns the live telemetry snapshot.
	Metrics(ctx context.Context) (metrics.Snapshot, error)
	// Schema returns the parameter table the server accepts.
	Schema(ctx context.Context) ([]spec.Param, error)
	// SubscribeSnapshots streams live snapshots until the returned cancel runs.
	SubscribeSnapshots() (<-chan metrics.Snapshot, func())
}

func toClientDescription(d node.QueueDescription) QueueDescription {
	return QueueDescription{
		Name:      d.Name,
		Kind:      d.Kind,
		Spec:      d.Spec,
		Derived:   d.Derived,
		Effective: d.Effective,
		Defaults:  d.Defaults,
	}
}