package mikyu

import (
	"context"
	"io"
)

type Client interface {
	io.Closer
	QueueAccess
	QueueAdmin
	Telemetry
}

type QueueAccess interface {
	OpenLog(ctx context.Context, name string, opts ...QueueOption) (LogQueue, error)
	OpenTask(ctx context.Context, name string, opts ...QueueOption) (TaskQueue, error)
	Log(ctx context.Context, name string) (LogQueue, error)
	Task(ctx context.Context, name string) (TaskQueue, error)
}

type QueueAdmin interface {
	CreateLog(ctx context.Context, name string, opts ...QueueOption) (LogQueue, error)
	CreateTask(ctx context.Context, name string, opts ...QueueOption) (TaskQueue, error)
	UpdateQueue(ctx context.Context, name string, opts ...QueueOption) error
	DescribeQueue(ctx context.Context, name string) (QueueDescription, error)
	DeleteQueue(ctx context.Context, name string) error
	ListQueues(ctx context.Context) ([]QueueInfo, error)
	Truncate(ctx context.Context, name string, minOffset uint64) (int64, error)
}

type Telemetry interface {
	Metrics(ctx context.Context) (Metrics, error)
	Schema(ctx context.Context) ([]Param, error)
	SubscribeSnapshots() (<-chan Metrics, func())
}
