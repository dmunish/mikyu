package mikyu

import (
	"context"
	"errors"
	"iter"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
)

var (
	ErrQueueExists     = engine.ErrQueueExists
	ErrQueueNotFound   = engine.ErrQueueNotFound
	ErrOffsetNotFound  = engine.ErrOffsetNotFound
	ErrMessageTooLarge = engine.ErrMessageTooLarge
)

type embeddedClient struct {
	mgr *engine.Manager
}

// Open initializes an embedded, in-process Mikyu engine at the specified directory.
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

	return &embeddedClient{mgr: mgr}, nil
}

func (c *embeddedClient) Close() error {
	return c.mgr.Close()
}

func (c *embeddedClient) CreateQueue(ctx context.Context, name string, opts ...QueueOption) (Queue, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	qCfg := QueueConfig{}
	for _, opt := range opts {
		opt(&qCfg)
	}

	cfg := engine.Config{
		BatchSize:       qCfg.BatchSize,
		MaxInterval:     qCfg.MaxInterval,
		MinInterval:     qCfg.MinInterval,
		MaxSegmentBytes: qCfg.MaxSegmentBytes,
		MaxMessageBytes: qCfg.MaxMessageBytes,
		MaxSegmentAge:   qCfg.MaxSegmentAge,
		Encrypted:       qCfg.Encrypted,
	}

	q, err := c.mgr.CreateQueue(name, cfg)
	if err != nil {
		return nil, err
	}

	return &embeddedQueue{q: q, name: name}, nil
}

func (c *embeddedClient) Queue(name string) (Queue, error) {
	q, err := c.mgr.GetQueue(name)
	if err != nil {
		return nil, err
	}
	return &embeddedQueue{q: q, name: name}, nil
}

func (c *embeddedClient) ListQueues(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.mgr.ListQueues(), nil
}

func (c *embeddedClient) DeleteQueue(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.mgr.DeleteQueue(name)
}

func (c *embeddedClient) Stats(ctx context.Context) (SystemStats, error) {
	if err := ctx.Err(); err != nil {
		return SystemStats{}, err
	}
	raw := c.mgr.Stats()

	stats := SystemStats{
		ActiveQueues: raw.ActiveQueues,
		TotalBytes:   raw.TotalBytes,
		Queues:       make(map[string]QueueStats, len(raw.Queues)),
	}

	for k, v := range raw.Queues {
		stats.Queues[k] = QueueStats{
			MessageCount: v.MessageCount,
			SegmentCount: v.SegmentCount,
			SizeBytes:    v.SizeBytes,
			LastOffset:   v.LastOffset,
		}
	}
	return stats, nil
}

// embeddedQueue wraps internal/engine.Queue directly.
type embeddedQueue struct {
	q    *engine.Queue
	name string
}

func (q *embeddedQueue) Name() string {
	return q.name
}

func (q *embeddedQueue) Produce(ctx context.Context, payload []byte) (uint64, error) {
	return q.q.Produce(ctx, payload)
}

func (q *embeddedQueue) ProduceBatch(ctx context.Context, payloads [][]byte) ([]uint64, error) {
	return q.q.ProduceBatch(ctx, payloads)
}

func (q *embeddedQueue) Read(ctx context.Context, offset uint64) (Message, error) {
	msg, err := q.q.Read(ctx, offset)
	if err != nil {
		return Message{}, err
	}
	return Message{
		Offset:    msg.Offset,
		ID:        msg.ID,
		Timestamp: msg.Timestamp,
		Payload:   msg.Payload,
	}, nil
}

func (q *embeddedQueue) ReadBatch(ctx context.Context, startOffset uint64, count int) ([]Message, error) {
	rawMsgs, err := q.q.ReadBatch(ctx, startOffset, count)
	if err != nil {
		return nil, err
	}
	msgs := make([]Message, len(rawMsgs))
	for i, msg := range rawMsgs {
		msgs[i] = Message{
			Offset:    msg.Offset,
			ID:        msg.ID,
			Timestamp: msg.Timestamp,
			Payload:   msg.Payload,
		}
	}
	return msgs, nil
}

func (q *embeddedQueue) LastOffset(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return q.q.LastOffset(), nil
}

func (q *embeddedQueue) Stream(ctx context.Context, startOffset uint64) iter.Seq2[Message, error] {
	return func(yield func(Message, error) bool) {
		for curr := startOffset; ; curr++ {
			if err := ctx.Err(); err != nil {
				yield(Message{}, err)
				return
			}
			msg, err := q.Read(ctx, curr)
			if errors.Is(err, ErrOffsetNotFound) {
				return
			}
			if !yield(msg, err) || err != nil {
				return
			}
		}
	}
}

func (q *embeddedQueue) Tail(ctx context.Context, startOffset uint64) iter.Seq2[Message, error] {
	return func(yield func(Message, error) bool) {
		curr := startOffset
		for {
			if err := ctx.Err(); err != nil {
				yield(Message{}, err)
				return
			}
			last, err := q.LastOffset(ctx)
			if err != nil {
				yield(Message{}, err)
				return
			}
			if curr >= last {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			msg, err := q.Read(ctx, curr)
			if err != nil {
				yield(Message{}, err)
				return
			}
			if !yield(msg, nil) {
				return
			}
			curr++
		}
	}
}

func (q *embeddedQueue) Group(name string) Group {
	return &embeddedGroup{
		q:         q.q,
		queueName: q.name,
		groupName: name,
		highWater: 0,
	}
}

// embeddedGroup handles consumer group offset state in embedded mode.
type embeddedGroup struct {
	q         *engine.Queue
	queueName string
	groupName string
	mu        sync.Mutex
	highWater uint64
}

func (g *embeddedGroup) Name() string {
	return g.groupName
}

func (g *embeddedGroup) Mark(offset uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if offset >= g.highWater {
		g.highWater = offset + 1
	}
}

func (g *embeddedGroup) Commit(ctx context.Context) error {
	g.mu.Lock()
	off := g.highWater
	g.mu.Unlock()
	return g.q.CommitOffset(ctx, g.groupName, off)
}

func (g *embeddedGroup) FetchOffset(ctx context.Context) (uint64, error) {
	return g.q.FetchOffset(ctx, g.groupName)
}

func (g *embeddedGroup) Close() error {
	return g.Commit(context.Background())
}
