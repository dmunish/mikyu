package mikyu

import (
	"context"
	"errors"
	"iter"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/client"
	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
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

func (c *clientAdapter) storeTask(name string, q TaskQueue) TaskQueue {
	c.mu.Lock()
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

// The log queue adapter converts engine records to the public Record
// value at the boundary.

func toRecord(r engine.Record) Record {
	return Record{
		Offset:    r.Offset,
		ID:        r.ID,
		Timestamp: r.Timestamp,
		Group:     r.Group,
		Payload:   r.Payload,
	}
}

func toRecords(rs []engine.Record) []Record {
	out := make([]Record, len(rs))
	for i, r := range rs {
		out[i] = toRecord(r)
	}
	return out
}

type logQueue struct {
	inner logqueue.Log
}

func (q *logQueue) Name() string { return q.inner.Name() }

func (q *logQueue) Append(ctx context.Context, payload []byte, opts ...AppendOption) (uint64, error) {
	return q.inner.Append(ctx, payload, opts...)
}

func (q *logQueue) AppendBatch(ctx context.Context, payloads [][]byte, opts ...AppendOption) ([]uint64, error) {
	return q.inner.AppendBatch(ctx, payloads, opts...)
}

func (q *logQueue) Read(ctx context.Context, offset uint64) (Record, error) {
	r, err := q.inner.Read(ctx, offset)
	if err != nil {
		return Record{}, err
	}
	return toRecord(r), nil
}

func (q *logQueue) ReadBatch(ctx context.Context, start uint64, count int) ([]Record, error) {
	rs, err := q.inner.ReadBatch(ctx, start, count)
	if err != nil {
		return nil, err
	}
	return toRecords(rs), nil
}

func (q *logQueue) Fetch(ctx context.Context, offset uint64, maxCount int, maxWait time.Duration) ([]Record, error) {
	rs, err := q.inner.Fetch(ctx, offset, maxCount, maxWait)
	if err != nil {
		return nil, err
	}
	return toRecords(rs), nil
}

func (q *logQueue) Stream(ctx context.Context, start uint64) iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		for r, err := range q.inner.Stream(ctx, start) {
			if err != nil {
				yield(Record{}, err)
				return
			}
			if !yield(toRecord(r), nil) {
				return
			}
		}
	}
}

func (q *logQueue) Tail(ctx context.Context, start uint64) iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		for r, err := range q.inner.Tail(ctx, start) {
			if err != nil {
				yield(Record{}, err)
				return
			}
			if !yield(toRecord(r), nil) {
				return
			}
		}
	}
}

func (q *logQueue) LastOffset(ctx context.Context) (uint64, error) {
	return q.inner.LastOffset(ctx)
}

func (q *logQueue) MinOffset(ctx context.Context) (uint64, error) {
	return q.inner.MinOffset(ctx)
}

func (q *logQueue) Group(name string) Group {
	return q.inner.Group(name)
}

var _ LogQueue = (*logQueue)(nil)

// The task queue adapter converts tasks and refs at the boundary. Ref
// is a sealed union: the full Task runs the strict stale-lease check,
// a bare TaskID skips it.

func toTask(t taskqueue.Task) Task {
	return Task{
		ID:          t.ID,
		Payload:     t.Payload,
		Attempts:    t.Attempts,
		MaxAttempts: t.MaxAttempts,
		Lease:       t.Lease,
		ScheduledAt: t.ScheduledAt,
		DeliveryID:  t.DeliveryID,
		Offset:      t.Offset,
		Key:         t.Key,
		Stalled:     t.Stalled,
	}
}

func toTasks(ts []taskqueue.Task) []Task {
	out := make([]Task, len(ts))
	for i, t := range ts {
		out[i] = toTask(t)
	}
	return out
}

func fromTask(t Task) taskqueue.Task {
	return taskqueue.Task{
		ID:          t.ID,
		Payload:     t.Payload,
		Attempts:    t.Attempts,
		MaxAttempts: t.MaxAttempts,
		Lease:       t.Lease,
		ScheduledAt: t.ScheduledAt,
		DeliveryID:  t.DeliveryID,
		Offset:      t.Offset,
		Key:         t.Key,
		Stalled:     t.Stalled,
	}
}

func toInternalRef(r Ref) taskqueue.Ref {
	switch v := r.(type) {
	case Task:
		return fromTask(v)
	case TaskID:
		return taskqueue.ID(v)
	default:
		return nil
	}
}

type taskQueue struct {
	inner taskqueue.Queue
}

func (q *taskQueue) Name() string { return q.inner.Name() }

func (q *taskQueue) Enqueue(ctx context.Context, payload []byte, opts ...EnqueueOption) (Task, error) {
	t, err := q.inner.Enqueue(ctx, payload, opts...)
	if err != nil {
		return Task{}, err
	}
	return toTask(t), nil
}

func (q *taskQueue) Dequeue(ctx context.Context) (Task, error) {
	t, err := q.inner.Dequeue(ctx)
	if err != nil {
		return Task{}, err
	}
	return toTask(t), nil
}

func (q *taskQueue) TryDequeue(ctx context.Context) (Task, error) {
	t, err := q.inner.TryDequeue(ctx)
	if err != nil {
		return Task{}, err
	}
	return toTask(t), nil
}

func (q *taskQueue) Ack(ctx context.Context, ref Ref) error {
	return q.inner.Ack(ctx, toInternalRef(ref))
}

func (q *taskQueue) Nack(ctx context.Context, ref Ref, opts ...NackOption) error {
	return q.inner.Nack(ctx, toInternalRef(ref), opts...)
}

func (q *taskQueue) Extend(ctx context.Context, ref Ref, timeout time.Duration) error {
	return q.inner.Extend(ctx, toInternalRef(ref), timeout)
}

func (q *taskQueue) DeadLetters(ctx context.Context) ([]Task, error) {
	ts, err := q.inner.DeadLetters(ctx)
	if err != nil {
		return nil, err
	}
	return toTasks(ts), nil
}

func (q *taskQueue) AckDeadLetter(ctx context.Context, ref Ref) error {
	return q.inner.AckDeadLetter(ctx, toInternalRef(ref))
}

func (q *taskQueue) Archive(ctx context.Context, ref Ref) error {
	return q.inner.Archive(ctx, toInternalRef(ref))
}

var _ TaskQueue = (*taskQueue)(nil)
