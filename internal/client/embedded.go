package client

import (
	"context"
	"io"

	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
)

// TaskQueue is the client-facing task driver interface.
type TaskQueue = taskqueue.ClientTaskQueue

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
	if cfg.DataDir != "" {
		dir = cfg.DataDir
	}

	n, release, err := node.Acquire(dir, cfg.Spec)
	if err != nil {
		return nil, err
	}
	return &embeddedClient{node: n, release: release}, nil
}

// CreateQueue creates a new log queue from a sparse spec.
func (c *embeddedClient) CreateQueue(ctx context.Context, name string, opts ...QueueOption) error {
	return c.node.CreateQueue(ctx, name, applyQueueOptions(opts))
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

// Info returns a snapshot of the node's identity and aggregate storage state.
func (c *embeddedClient) Info(ctx context.Context) (Info, error) {
	i, err := c.node.Info(ctx)
	if err != nil {
		return Info{}, err
	}
	return toClientInfo(i), nil
}

// ListQueues returns all queue names.
func (c *embeddedClient) ListQueues(ctx context.Context) ([]string, error) {
	return c.node.ListQueues(ctx)
}

// DeleteQueue removes a queue.
func (c *embeddedClient) DeleteQueue(ctx context.Context, name string) error {
	return c.node.DeleteQueue(ctx, name)
}

// Close releases this client's reference to the shared node.
func (c *embeddedClient) Close() error {
	if c.release == nil {
		return nil
	}
	return c.release()
}

var _ interface {
	io.Closer
	CreateQueue(context.Context, string, ...QueueOption) error
	UpdateQueue(context.Context, string, ...QueueOption) error
	DescribeQueue(context.Context, string) (QueueDescription, error)
	LogQueue(string) (logqueue.ClientLogQueue, error)
	TaskQueue(string) (TaskQueue, error)
	ListQueues(context.Context) ([]string, error)
	DeleteQueue(context.Context, string) error
	Truncate(context.Context, string, uint64) (int64, error)
	Info(context.Context) (Info, error)
} = (*embeddedClient)(nil)
