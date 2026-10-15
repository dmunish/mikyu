package client

import (
	"context"
	"fmt"

	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
	"github.com/dmunish/mikyu/internal/spec"
)

// TaskQueue is the client-facing task driver interface.
type TaskQueue = taskqueue.Queue

// embeddedClient implements the client on top of a reference-counted
// node.Node from the process pool. It owns one reference; Close releases it.
type embeddedClient struct {
	node    *node.Node
	release func() error
}

// NewEmbedded creates an in-process client.
func NewEmbedded(dir string, opts ...Option) (*embeddedClient, error) {
	cfg := DefaultSessionConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.Target != "" {
		return nil, fmt.Errorf("%w: target is for remote clients, use Dial", ErrModeMismatch)
	}
	if cfg.DataDir != "" {
		dir = cfg.DataDir
	}
	if err := spec.ValidateCreate(cfg.Spec.Effective(spec.Spec{}, spec.Builtin())); err != nil {
		return nil, err
	}

	n, release, err := node.Acquire(dir, cfg.Spec,
		node.WithMetrics(cfg.metricsConfig()),
		node.WithEncryptionKeyFile(cfg.EncryptionKeyFile),
	)
	if err != nil {
		return nil, err
	}
	return &embeddedClient{node: n, release: release}, nil
}

// CreateLogQueue declares a log queue from a sparse spec and returns its handle.
func (c *embeddedClient) CreateLogQueue(ctx context.Context, name string, opts ...QueueOption) (logqueue.Log, error) {
	return c.node.CreateLogQueue(ctx, name, applyQueueOptions(opts))
}

// CreateTaskQueue declares a task queue from a sparse spec and returns its handle.
func (c *embeddedClient) CreateTaskQueue(ctx context.Context, name string, opts ...QueueOption) (taskqueue.Queue, error) {
	s := applyQueueOptions(opts)
	if err := rejectCompaction(s); err != nil {
		return nil, err
	}
	return c.node.CreateTaskQueue(ctx, name, s)
}

// rejectCompaction enforces at the SDK boundary that compaction is a log-queue setting.
func rejectCompaction(s spec.Spec) error {
	if s.Storage.Compaction != nil && *s.Storage.Compaction != spec.CompactionNone {
		return fmt.Errorf("%w: compaction is a log-queue setting", spec.ErrInvalidConfig)
	}
	return nil
}

// UpdateQueue merges a sparse patch into the queue's stored spec and applies the mutable fields to the live queue.
func (c *embeddedClient) UpdateQueue(ctx context.Context, name string, opts ...QueueOption) error {
	return c.node.UpdateQueue(ctx, name, applyQueueOptions(opts))
}

// DescribeQueue returns the stored spec, effective spec, defaults, and stats.
func (c *embeddedClient) DescribeQueue(ctx context.Context, name string) (QueueDescription, error) {
	d, err := c.node.DescribeQueue(ctx, name)
	if err != nil {
		return QueueDescription{}, err
	}
	return toClientDescription(d), nil
}

// Metrics returns the node's current live snapshot.
func (c *embeddedClient) Metrics(ctx context.Context) (metrics.Snapshot, error) {
	return c.node.Metrics(ctx)
}

// Schema returns a fresh copy of the node's parameter table.
func (c *embeddedClient) Schema(ctx context.Context) ([]spec.Param, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]spec.Param, len(spec.Table))
	copy(out, spec.Table)
	return out, nil
}

// ListQueues returns every queue's name and kind.
func (c *embeddedClient) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	return c.node.ListQueues(ctx)
}

// DeleteQueue removes a queue.
func (c *embeddedClient) DeleteQueue(ctx context.Context, name string) error {
	return c.node.DeleteQueue(ctx, name)
}

// SubscribeSnapshots streams live snapshots from the embedded node's collector.
func (c *embeddedClient) SubscribeSnapshots() (<-chan metrics.Snapshot, func()) {
	return c.node.Collector().Subscribe()
}

// Close releases this client's reference to the shared node.
func (c *embeddedClient) Close() error {
	if c.release == nil {
		return nil
	}
	return c.release()
}

var _ Client = (*embeddedClient)(nil)
