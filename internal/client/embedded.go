package client

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/logqueue"
	"github.com/dmunish/mikyu/internal/retention"
	"github.com/dmunish/mikyu/internal/taskqueue"
)

// TaskQueue is the client-facing task driver interface.
type TaskQueue = taskqueue.ClientTaskQueue

// ErrTaskNotSupported is retained for source compatibility; task queues are now supported.
var ErrTaskNotSupported = errors.New("task queues are not supported by this client")

// embeddedClient implements the client using an in-process engine.Manager.
type embeddedClient struct {
	mgr    *engine.Manager
	queues map[string]*logqueue.LogQueue
	tasks  map[string]TaskQueue
	mu     sync.RWMutex
	cfg    Config
	closed bool

	retention       *retention.Coordinator
	retentionCancel context.CancelFunc
	retentionWG     sync.WaitGroup
}

// NewEmbedded creates an in-process client.
func NewEmbedded(dir string, opts ...Option) (*embeddedClient, error) {
	cfg := DefaultClientConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	engineCfg := engine.Config{
		BatchSize:       cfg.BatchSize,
		MaxInterval:     cfg.MaxInterval,
		MinInterval:     cfg.MinInterval,
		MaxSegmentBytes: cfg.DefaultMaxSegmentBytes,
		MaxMessageBytes: cfg.DefaultMaxMessageBytes,
		MaxSegmentAge:   cfg.DefaultMaxSegmentAge,
		Encrypted:       cfg.Encrypted,
		Fsync:           cfg.Fsync,
	}

	mgr, err := engine.OpenManager(dir, engineCfg)
	if err != nil {
		return nil, err
	}
	retentionCtx, retentionCancel := context.WithCancel(context.Background())
	c := &embeddedClient{
		mgr:             mgr,
		queues:          make(map[string]*logqueue.LogQueue),
		tasks:           make(map[string]TaskQueue),
		cfg:             cfg,
		retention:       retention.NewCoordinator(0),
		retentionCancel: retentionCancel,
	}
	c.retentionWG.Go(func() {
		c.retention.Start(retentionCtx)
	})
	return c, nil
}

// CreateQueue creates a new log queue.
func (c *embeddedClient) CreateQueue(ctx context.Context, name string, opts ...QueueOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	qCfg := QueueConfig{
		BatchSize:       c.cfg.BatchSize,
		MaxInterval:     c.cfg.MaxInterval,
		MinInterval:     c.cfg.MinInterval,
		MaxSegmentBytes: c.cfg.DefaultMaxSegmentBytes,
		MaxMessageBytes: c.cfg.DefaultMaxMessageBytes,
		MaxSegmentAge:   c.cfg.DefaultMaxSegmentAge,
		Encrypted:       c.cfg.Encrypted,
		Fsync:           c.cfg.Fsync,
	}
	for _, opt := range opts {
		opt(&qCfg)
	}

	engineCfg := engine.Config{
		BatchSize:       qCfg.BatchSize,
		MaxInterval:     qCfg.MaxInterval,
		MinInterval:     qCfg.MinInterval,
		MaxSegmentBytes: qCfg.MaxSegmentBytes,
		MaxMessageBytes: qCfg.MaxMessageBytes,
		MaxSegmentAge:   qCfg.MaxSegmentAge,
		Encrypted:       qCfg.Encrypted,
		Fsync:           qCfg.Fsync,
	}

	storage, err := c.mgr.CreateQueue(name, engineCfg)
	if err != nil {
		return err
	}
	lq := logqueue.NewLogQueue(name, storage)
	c.mu.Lock()
	c.queues[name] = lq
	c.mu.Unlock()
	c.retention.RegisterDriver(name, storage, lq)
	return nil
}

// LogQueue returns a cached handle to an existing log queue.
func (c *embeddedClient) LogQueue(name string) (logqueue.ClientLogQueue, error) {
	return c.getOrCreateLogQueue(name)
}

func (c *embeddedClient) getOrCreateLogQueue(name string) (*logqueue.LogQueue, error) {
	c.mu.RLock()
	lq, ok := c.queues[name]
	c.mu.RUnlock()
	if ok {
		return lq, nil
	}

	storage, err := c.mgr.GetQueue(name)
	if err != nil {
		return nil, err
	}
	lq = logqueue.NewLogQueue(name, storage)

	c.mu.Lock()
	if existing, ok := c.queues[name]; ok {
		c.mu.Unlock()
		return existing, nil
	}
	c.queues[name] = lq
	c.mu.Unlock()
	c.retention.RegisterDriver(name, storage, lq)
	return lq, nil
}

func (c *embeddedClient) TaskQueue(name string) (TaskQueue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if tq, ok := c.tasks[name]; ok {
		return tq, nil
	}
	storage, err := c.mgr.GetQueue(name)
	if err != nil {
		return nil, err
	}
	tq, err := taskqueue.NewTaskQueue(name, storage)
	if err != nil {
		return nil, err
	}
	c.tasks[name] = tq
	c.retention.RegisterDriver(name, storage, tq)
	return tq, nil
}

// ListQueues returns all queue names.
func (c *embeddedClient) ListQueues(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.mgr.ListQueues(), nil
}

// DeleteQueue removes a queue.
func (c *embeddedClient) DeleteQueue(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	if tq, ok := c.tasks[name]; ok {
		_ = tq.Close()
		delete(c.tasks, name)
	}
	delete(c.queues, name)
	c.mu.Unlock()
	c.retention.UnregisterTarget(name)
	return c.mgr.DeleteQueue(name)
}

// TruncateQueue manually truncates log segments before minOffset.
func (c *embeddedClient) TruncateQueue(ctx context.Context, name string, minOffset uint64) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if _, err := c.getOrCreateLogQueue(name); err != nil {
		return 0, err
	}
	// Manual truncation is an irreversible operation. Materialize the task
	// driver first so durable task state from a previous process lifetime is
	// included in the retention floor even when no task client is currently
	// active.
	if _, err := c.TaskQueue(name); err != nil {
		return 0, err
	}
	return c.retention.ReconcileQueueAt(ctx, name, minOffset)
}

// Stats returns engine statistics.
func (c *embeddedClient) Stats(ctx context.Context) (engine.EngineStats, error) {
	if err := ctx.Err(); err != nil {
		return engine.EngineStats{}, err
	}
	es := c.mgr.Stats()
	ss := engine.EngineStats{
		ActiveQueues: es.ActiveQueues,
		TotalBytes:   es.TotalBytes,
		Queues:       make(map[string]engine.QueueStats, len(es.Queues)),
	}
	for name, qs := range es.Queues {
		ss.Queues[name] = engine.QueueStats{
			MessageCount: qs.MessageCount,
			SegmentCount: qs.SegmentCount,
			SizeBytes:    qs.SizeBytes,
			LastOffset:   qs.LastOffset,
			MinOffset:    qs.MinOffset,
		}
	}
	return ss, nil
}

// Close shuts down the embedded client.
func (c *embeddedClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.retentionCancel != nil {
		c.retentionCancel()
	}
	c.mu.Unlock()
	c.retentionWG.Wait()
	c.mu.Lock()
	var firstErr error
	for name, tq := range c.tasks {
		if err := tq.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(c.tasks, name)
	}
	if err := c.mgr.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

var _ interface {
	io.Closer
	CreateQueue(context.Context, string, ...QueueOption) error
	LogQueue(string) (logqueue.ClientLogQueue, error)
	TaskQueue(string) (TaskQueue, error)
	ListQueues(context.Context) ([]string, error)
	DeleteQueue(context.Context, string) error
	TruncateQueue(context.Context, string, uint64) (int64, error)
	Stats(context.Context) (engine.EngineStats, error)
} = (*embeddedClient)(nil)
