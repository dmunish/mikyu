package mikyu

import (
	"context"
	"errors"
	"iter"
	"sync"

	"github.com/dmunish/mikyu/queue"
)

// Engine is the unified contract satisfied by both the in-process
// queue.Queue (embedded use) and a queue-bound view of *Client (remote use).
// The Engine is always already bound to a single queue; methods take no
// queue-name argument. Use Client.Queue(name) to obtain a remote Engine.
type Engine interface {
	Produce(ctx context.Context, payload []byte) (uint64, error)
	ProduceBatch(ctx context.Context, payloads [][]byte) ([]uint64, error)
	Read(ctx context.Context, offset uint64) (Message, error)
	ReadBatch(ctx context.Context, startOffset uint64, count int) ([]Message, error)
	LastOffset(ctx context.Context) (uint64, error)
	CommitOffset(ctx context.Context, group string, offset uint64) error
	FetchOffset(ctx context.Context, group string) (uint64, error)
}

// LastOffsetAdapter bridges queue.Queue's lock-only LastOffset into the
// ctx-aware LastOffset that Engine requires. queue.Queue already satisfies
// every other Engine method; this is the only seam, so the adapter embeds
// *queue.Queue and only overrides LastOffset.
type LastOffsetAdapter struct{ *queue.Queue }

func (a LastOffsetAdapter) LastOffset(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return a.Queue.LastOffset(), nil
}

// Compile-time assertion that LastOffsetAdapter satisfies Engine.
var _ Engine = LastOffsetAdapter{}

// remoteEngine is a queue-bound Engine view over a Client.
type remoteEngine struct {
	c *Client
	q string
}

func (e *remoteEngine) Produce(ctx context.Context, payload []byte) (uint64, error) {
	return e.c.Produce(ctx, e.q, payload)
}

func (e *remoteEngine) ProduceBatch(ctx context.Context, payloads [][]byte) ([]uint64, error) {
	offsets := make([]uint64, 0, len(payloads))
	for _, p := range payloads {
		off, err := e.c.Produce(ctx, e.q, p)
		if err != nil {
			return offsets, err
		}
		offsets = append(offsets, off)
	}
	return offsets, nil
}

func (e *remoteEngine) Read(ctx context.Context, offset uint64) (Message, error) {
	return e.c.Read(ctx, e.q, offset)
}

func (e *remoteEngine) ReadBatch(ctx context.Context, startOffset uint64, count int) ([]Message, error) {
	if count <= 0 {
		return nil, nil
	}
	out := make([]Message, 0, count)
	for i := range count {
		msg, err := e.c.Read(ctx, e.q, startOffset+uint64(i))
		if err != nil {
			return out, err
		}
		out = append(out, msg)
	}
	return out, nil
}

func (e *remoteEngine) LastOffset(ctx context.Context) (uint64, error) {
	return e.c.LastOffset(ctx, e.q)
}

func (e *remoteEngine) CommitOffset(ctx context.Context, group string, offset uint64) error {
	return e.c.CommitOffset(ctx, e.q, group, offset)
}

func (e *remoteEngine) FetchOffset(ctx context.Context, group string) (uint64, error) {
	return e.c.FetchOffset(ctx, e.q, group)
}

// Queue returns an Engine bound to the named queue on this client connection.
// The returned Engine multiplexes operations over the existing TCP connection.
func (c *Client) Queue(name string) Engine {
	return &remoteEngine{c: c, q: name}
}

// Stream iterates messages from startOffset up to the current tail. It is a
// pure read; non-destructive. ErrOffsetNotFound naturally ends the stream.
// The iterator respects ctx cancellation.
func (c *Client) Stream(ctx context.Context, queue string, startOffset uint64) iter.Seq2[Message, error] {
	return func(yield func(Message, error) bool) {
		for curr := startOffset; ; curr++ {
			if err := ctx.Err(); err != nil {
				yield(Message{}, err)
				return
			}
			msg, err := c.Read(ctx, queue, curr)
			if errors.Is(err, ErrOffsetNotFound) {
				return
			}
			if !yield(msg, err) || err != nil {
				return
			}
		}
	}
}

// Tail streams messages from the live tail: it yields every new message as it
// is produced. Cancel the ctx to stop.
func (c *Client) Tail(ctx context.Context, queue string, startOffset uint64) iter.Seq2[Message, error] {
	return func(yield func(Message, error) bool) {
		next := startOffset
		for {
			if err := ctx.Err(); err != nil {
				yield(Message{}, err)
				return
			}
			tail, err := c.LastOffset(ctx, queue)
			if err != nil {
				yield(Message{}, err)
				return
			}
			if next >= tail {
				continue // tight spin — caller should bound with ctx
			}
			msg, err := c.Read(ctx, queue, next)
			if err != nil {
				yield(Message{}, err)
				return
			}
			if !yield(msg, nil) {
				return
			}
			next++
		}
	}
}

// ConsumerGroup tracks per-group offset progress on top of a Client. Mark is
// the local fast-path; Commit persists the high-water-mark durably on the
// server. FetchOffset returns the server-side persisted progress.
type ConsumerGroup struct {
	client *Client
	queue  string
	group  string

	mu        sync.Mutex
	highWater uint64 // next offset to read
	dirty     bool
}

func (c *Client) JoinGroup(queue, group string) *ConsumerGroup {
	return &ConsumerGroup{client: c, queue: queue, group: group}
}

// Mark records progress locally. Out-of-order marks are tolerated by keeping
// the high-water mark.
func (cg *ConsumerGroup) Mark(offset uint64) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	if offset >= cg.highWater {
		cg.highWater = offset + 1
		cg.dirty = true
	}
}

// Commit flushes the local high-water mark to the server durably.
func (cg *ConsumerGroup) Commit(ctx context.Context) error {
	cg.mu.Lock()
	off := cg.highWater
	cg.dirty = false
	cg.mu.Unlock()
	return cg.client.CommitOffset(ctx, cg.queue, cg.group, off)
}

// FetchOffset loads the server-side persisted offset (the next to read).
func (cg *ConsumerGroup) FetchOffset(ctx context.Context) (uint64, error) {
	return cg.client.FetchOffset(ctx, cg.queue, cg.group)
}

// Codec marshals typed payloads to/from the byte payloads stored on disk.
type Codec[T any] struct {
	Marshal   func(T) ([]byte, error)
	Unmarshal func([]byte, *T) error
}

// Typed wraps an Engine with a codec so callers work in their domain type T
// instead of []byte. The Engine must already be bound to a single queue.
type Typed[T any] struct {
	engine Engine
	codec  Codec[T]
}

func NewTyped[T any](engine Engine, codec Codec[T]) *Typed[T] {
	return &Typed[T]{engine: engine, codec: codec}
}

func (t *Typed[T]) Produce(ctx context.Context, v T) (uint64, error) {
	payload, err := t.codec.Marshal(v)
	if err != nil {
		return 0, err
	}
	return t.engine.Produce(ctx, payload)
}

func (t *Typed[T]) Read(ctx context.Context, offset uint64) (T, error) {
	var zero T
	msg, err := t.engine.Read(ctx, offset)
	if err != nil {
		return zero, err
	}
	var v T
	if err := t.codec.Unmarshal(msg.Payload, &v); err != nil {
		return zero, err
	}
	return v, nil
}
