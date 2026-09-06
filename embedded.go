package mikyu

import (
	"context"
	"sync"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/logqueue"
)

// embeddedClient implements Client using an in-process engine.Manager.
type embeddedClient struct {
	mgr    *engine.Manager
	queues map[string]*logqueue.LogQueue
	tasks  map[string]TaskQueue // stub, unused
	mu     sync.RWMutex
	cfg    ClientConfig
	closed bool
}

// Open creates an in-process Client.
func Open(dir string, opts ...ClientOption) (Client, error) {
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
	}
	mgr, err := engine.OpenManager(dir, engineCfg)
	if err != nil {
		return nil, err
	}
	return &embeddedClient{
		mgr:    mgr,
		queues: make(map[string]*logqueue.LogQueue),
		tasks:  make(map[string]TaskQueue),
		cfg:    cfg,
	}, nil
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
	}
	storage, err := c.mgr.CreateQueue(name, engineCfg)
	if err != nil {
		return err
	}
	lq := logqueue.NewLogQueue(name, storage)
	c.mu.Lock()
	c.queues[name] = lq
	c.mu.Unlock()
	return nil
}

// LogQueue returns a handle to an existing log queue.
func (c *embeddedClient) LogQueue(name string) (LogQueue, error) {
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
	return lq, nil
}

// TaskQueue is not yet implemented.
func (c *embeddedClient) TaskQueue(name string) (TaskQueue, error) {
	return nil, ErrTaskNotSupported
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
	delete(c.queues, name)
	c.mu.Unlock()
	return c.mgr.DeleteQueue(name)
}

// TruncateQueue manually truncates log segments before minOffset.
func (c *embeddedClient) TruncateQueue(ctx context.Context, name string, minOffset uint64) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	q, err := c.mgr.GetQueue(name)
	if err != nil {
		return 0, err
	}
	return q.TruncateBefore(ctx, minOffset)
}

// Stats returns engine statistics.
func (c *embeddedClient) Stats(ctx context.Context) (SystemStats, error) {
	if err := ctx.Err(); err != nil {
		return SystemStats{}, err
	}
	es := c.mgr.Stats()
	ss := SystemStats{
		ActiveQueues: es.ActiveQueues,
		TotalBytes:   es.TotalBytes,
		Queues:       make(map[string]QueueStats, len(es.Queues)),
	}
	for name, qs := range es.Queues {
		ss.Queues[name] = QueueStats{
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
	return c.mgr.Close()
}

// Ensure that embeddedClient implements Client.
var _ Client = (*embeddedClient)(nil)
