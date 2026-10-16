package mikyu

import (
	"context"
	"errors"
	"sync"

	"github.com/dmunish/mikyu/internal/client"
	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/node"
)

func toQueueInfo(info node.QueueInfo) QueueInfo {
	return QueueInfo{Name: info.Name, Kind: info.Kind}
}

func toQueueInfos(infos []node.QueueInfo) []QueueInfo {
	out := make([]QueueInfo, len(infos))
	for i, info := range infos {
		out[i] = toQueueInfo(info)
	}
	return out
}

func toQueueDescription(d client.QueueDescription) QueueDescription {
	return QueueDescription{
		Name:      d.Name,
		Kind:      d.Kind,
		Spec:      d.Spec,
		Derived:   d.Derived,
		Effective: d.Effective,
		Defaults:  d.Defaults,
	}
}

type clientAdapter struct {
	inner client.Client

	mu    sync.Mutex
	logs  map[string]LogQueue
	tasks map[string]TaskQueue
}

func newClient(inner client.Client) *clientAdapter {
	return &clientAdapter{
		inner: inner,
		logs:  map[string]LogQueue{},
		tasks: map[string]TaskQueue{},
	}
}

func (c *clientAdapter) cachedLog(name string, resolve func() (LogQueue, error)) (LogQueue, error) {
	c.mu.Lock()
	if q, ok := c.logs[name]; ok {
		c.mu.Unlock()
		return q, nil
	}
	c.mu.Unlock()
	q, err := resolve()
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.logs[name]; ok {
		return existing, nil
	}
	c.logs[name] = q
	return q, nil
}

func (c *clientAdapter) cachedTask(name string, resolve func() (TaskQueue, error)) (TaskQueue, error) {
	c.mu.Lock()
	if q, ok := c.tasks[name]; ok {
		c.mu.Unlock()
		return q, nil
	}
	c.mu.Unlock()
	q, err := resolve()
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.tasks[name]; ok {
		return existing, nil
	}
	c.tasks[name] = q
	return q, nil
}

func (c *clientAdapter) storeLog(name string, q LogQueue) LogQueue {
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.logs[name]; ok {
		return existing
	}
	c.logs[name] = q
	return q
}

func (c *clientAdapter) storeTask(name string, q TaskQueue) TaskQueue {	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.tasks[name]; ok {
		return existing
	}
	c.tasks[name] = q
	return q
}

func (c *clientAdapter) OpenLog(ctx context.Context, name string, opts ...QueueOption) (LogQueue, error) {
	q, err := c.Log(ctx, name)
	if err == nil {
		return q, nil
	}
	if !errors.Is(err, engine.ErrQueueNotFound) {
		return nil, err
	}
	return c.CreateLog(ctx, name, opts...)
}

func (c *clientAdapter) OpenTask(ctx context.Context, name string, opts ...QueueOption) (TaskQueue, error) {
	q, err := c.Task(ctx, name)
	if err == nil {
		return q, nil
	}
	if !errors.Is(err, engine.ErrQueueNotFound) {
		return nil, err
	}
	return c.CreateTask(ctx, name, opts...)
}

func (c *clientAdapter) Log(ctx context.Context, name string) (LogQueue, error) {
	d, err := c.inner.DescribeQueue(ctx, name)
	if err != nil {
		return nil, err
	}
	if d.Kind != node.KindLog {
		return nil, node.ErrWrongKind
	}
	return c.cachedLog(name, func() (LogQueue, error) {
		q, err := c.inner.LogQueue(name)
		if err != nil {
			return nil, err
		}
		return &logQueue{inner: q}, nil
	})
}

func (c *clientAdapter) Task(ctx context.Context, name string) (TaskQueue, error) {
	d, err := c.inner.DescribeQueue(ctx, name)
	if err != nil {
		return nil, err
	}
	if d.Kind != node.KindTask {
		return nil, node.ErrWrongKind
	}
	return c.cachedTask(name, func() (TaskQueue, error) {
		q, err := c.inner.TaskQueue(name)
		if err != nil {
			return nil, err
		}
		return &taskQueue{inner: q}, nil
	})
}

func (c *clientAdapter) CreateLog(ctx context.Context, name string, opts ...QueueOption) (LogQueue, error) {
	q, err := c.inner.CreateLogQueue(ctx, name, opts...)
	if err != nil {
		return nil, err
	}
	return c.storeLog(name, &logQueue{inner: q}), nil
}

func (c *clientAdapter) CreateTask(ctx context.Context, name string, opts ...QueueOption) (TaskQueue, error) {
	q, err := c.inner.CreateTaskQueue(ctx, name, opts...)
	if err != nil {
		return nil, err
	}
	return c.storeTask(name, &taskQueue{inner: q}), nil
}

func (c *clientAdapter) UpdateQueue(ctx context.Context, name string, opts ...QueueOption) error {
	return c.inner.UpdateQueue(ctx, name, opts...)
}

func (c *clientAdapter) DescribeQueue(ctx context.Context, name string) (QueueDescription, error) {
	d, err := c.inner.DescribeQueue(ctx, name)
	if err != nil {
		return QueueDescription{}, err
	}
	return toQueueDescription(d), nil
}

func (c *clientAdapter) DeleteQueue(ctx context.Context, name string) error {
	err := c.inner.DeleteQueue(ctx, name)
	if err == nil {
		c.mu.Lock()
		delete(c.logs, name)
		delete(c.tasks, name)
		c.mu.Unlock()
	}
	return err
}

func (c *clientAdapter) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	infos, err := c.inner.ListQueues(ctx)
	if err != nil {
		return nil, err
	}
	return toQueueInfos(infos), nil
}

func (c *clientAdapter) Truncate(ctx context.Context, name string, minOffset uint64) (int64, error) {
	return c.inner.Truncate(ctx, name, minOffset)
}

func (c *clientAdapter) Metrics(ctx context.Context) (Metrics, error) {
	return c.inner.Metrics(ctx)
}

func (c *clientAdapter) Schema(ctx context.Context) ([]Param, error) {
	return c.inner.Schema(ctx)
}

func (c *clientAdapter) SubscribeSnapshots() (<-chan Metrics, func()) {
	return c.inner.SubscribeSnapshots()
}

func (c *clientAdapter) Close() error {
	return c.inner.Close()
}

var _ Client = (*clientAdapter)(nil)
