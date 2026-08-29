package mikyu

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/protocol"
	"github.com/dmunish/mikyu/queue"
)

var (
    ErrQueueExists    = queue.ErrQueueExists
    ErrQueueNotFound  = queue.ErrQueueNotFound
    ErrOffsetNotFound = queue.ErrOffsetNotFound
)

type Message = queue.Message

const (
    OffsetOldest uint64 = 0
    OffsetNewest uint64 = ^uint64(0)
)

// Engine is the structural seam that unifies the in-process storage engine
// (queue.Queue) and the remote TCP client (Client) behind a single surface.
type Engine interface {
    Produce(ctx context.Context, payload []byte) (uint64, error)
    ProduceBatch(ctx context.Context, payloads [][]byte) ([]uint64, error)
    Read(ctx context.Context, offset uint64) (Message, error)
    ReadBatch(ctx context.Context, startOffset uint64, count int) ([]Message, error)
    LastOffset(ctx context.Context) (uint64, error)
    CommitOffset(ctx context.Context, group string, offset uint64) error
    FetchOffset(ctx context.Context, group string) (uint64, error)
}

// Codec marshals and unmarshals payloads of type T.
type Codec[T any] struct {
    Marshal   func(T) ([]byte, error)
    Unmarshal func([]byte, *T) error
}

// JSONCodec returns a Codec using encoding/json.
func JSONCodec[T any]() Codec[T] {
    return Codec[T]{
        Marshal:   json.Marshal,
        Unmarshal: json.Unmarshal,
    }
}

// Typed wraps an Engine to provide type-safe Produce and Read helpers.
type Typed[T any] struct {
    engine Engine
    queue  string
    codec  Codec[T]
}

// NewTyped creates a Typed view over a queue.
func NewTyped[T any](engine Engine, queue string, codec Codec[T]) *Typed[T] {
    return &Typed[T]{engine: engine, queue: queue, codec: codec}
}

// Produce marshals and sends a value.
func (t *Typed[T]) Produce(ctx context.Context, val T) (uint64, error) {
    data, err := t.codec.Marshal(val)
    if err != nil {
        return 0, err
    }
    return t.engine.Produce(ctx, t.queue, data)
}

// Read unmarshals a message at the given offset.
func (t *Typed[T]) Read(ctx context.Context, offset uint64) (T, error) {
    var zero T
    msg, err := t.engine.Read(ctx, t.queue, offset)
    if err != nil {
        return zero, err
    }
    if err := t.codec.Unmarshal(msg.Payload, &zero); err != nil {
        return zero, err
    }
    return zero, nil
}

// ProduceStream returns an iterator that yields messages currently buffered in
// the queue, starting at startOffset. The iterator stops naturally when it
// reaches the tail (caller should switch to Tail for live messages).
func (c *Client) ProduceStream(queue string) *Streamer {
    return &Streamer{client: c, queue: queue}
}

// Stream yields messages from startOffset up to the current tail, then stops.
func (c *Client) Stream(ctx context.Context, queue string, startOffset uint64) iter.Seq2[Message, error] {
    return func(yield func(Message, error) bool) {
        curr := startOffset
        for {
            if err := ctx.Err(); err != nil {
                return
            }
            msg, err := c.Read(ctx, queue, curr)
            if errors.Is(err, ErrOffsetNotFound) {
                return
            }
            if !yield(msg, err) || err != nil {
                return
            }
            curr++
        }
    }
}

// Tail yields messages starting at startOffset, blocking for new ones as they
// are produced. It stops when ctx is cancelled or the consumer signals stop.
func (c *Client) Tail(ctx context.Context, queue string, startOffset uint64, pollInterval time.Duration) iter.Seq2[Message, error] {
    if pollInterval <= 0 {
        pollInterval = 50 * time.Millisecond
    }
    return func(yield func(Message, error) bool) {
        curr := startOffset
        ticker := time.NewTicker(pollInterval)
        defer ticker.Stop()
        for {
            if err := ctx.Err(); err != nil {
                return
            }
            msg, err := c.Read(ctx, queue, curr)
            if errors.Is(err, ErrOffsetNotFound) {
                select {
                case <-ctx.Done():
                    return
                case <-ticker.C:
                    continue
                }
            }
            if !yield(msg, err) || err != nil {
                return
            }
            curr++
        }
    }
}

// CommitOffset records a consumer group's progress. Stale commits are ignored.
func (c *Client) CommitOffset(ctx context.Context, queue, group string, offset uint64) error {
    var body protocol.Buffer
    body.WriteBytes([]byte(queue))
    body.WriteBytes([]byte(group))
    body.WriteU64(offset)
    _, err := c.exchange(ctx, protocol.OpCommitOffset, body.Bytes())
    return err
}

// FetchOffset returns the last committed offset for a group on a queue, or 0
// if the group has no recorded progress.
func (c *Client) FetchOffset(ctx context.Context, queue, group string) (uint64, error) {
    var body protocol.Buffer
    body.WriteBytes([]byte(queue))
    body.WriteBytes([]byte(group))

    resp, err := c.exchange(ctx, protocol.OpFetchOffset, body.Bytes())
    if err != nil {
        return 0, err
    }
    offset, err := protocol.NewReader(resp).ReadU64()
    if err != nil {
        return 0, fmt.Errorf("invalid response offset: %w", err)
    }
    return offset, nil
}

// ProduceBatch is a thin network-level convenience; the embedded engine will
// gain its own fsync-amortized path in a later phase.
func (c *Client) ProduceBatch(ctx context.Context, queue string, payloads [][]byte) ([]uint64, error) {
    offsets := make([]uint64, len(payloads))
    for i, p := range payloads {
        off, err := c.Produce(ctx, queue, p)
        if err != nil {
            return offsets[:i], err
        }
        offsets[i] = off
    }
    return offsets, nil
}

// ReadBatch fetches up to count messages starting at startOffset. It stops
// early on tail (ErrOffsetNotFound) and returns whatever it collected.
func (c *Client) ReadBatch(ctx context.Context, queue string, startOffset uint64, count int) ([]Message, error) {
    if count <= 0 {
        return nil, nil
    }
    out := make([]Message, 0, count)
    for i := 0; i < count; i++ {
        msg, err := c.Read(ctx, queue, startOffset+uint64(i))
        if errors.Is(err, ErrOffsetNotFound) {
            return out, nil
        }
        if err != nil {
            return out, err
        }
        out = append(out, msg)
    }
    return out, nil
}

type Client struct {
    mu   sync.Mutex
    conn net.Conn
    r    *bufio.Reader
}

func Dial(addr string) (*Client, error) {
    conn, err := net.Dial("tcp", addr)
    if err != nil {
        return nil, err
    }
    return &Client{conn: conn, r: bufio.NewReader(conn)}, nil
}

func (c *Client) Close() error {
    return c.conn.Close()
}

func (c *Client) CreateQueue(ctx context.Context, name string) error {
    var body protocol.Buffer
    body.WriteBytes([]byte(name))

    _, err := c.exchange(ctx, protocol.OpCreateQueue, body.Bytes())
    return err
}

func (c *Client) exchange(ctx context.Context, op byte, body []byte) ([]byte, error) {
    c.mu.Lock()
    defer c.mu.Unlock()

    if err := ctx.Err(); err != nil {
        return nil, err
    }

    if deadline, ok := ctx.Deadline(); ok {
        c.conn.SetDeadline(deadline)
    } else {
        c.conn.SetDeadline(time.Time{})
    }

    if err := protocol.WriteFrame(c.conn, op, body); err != nil {
        return nil, err
    }
    resOp, resBody, err := protocol.ReadFrame(c.r)
    if err != nil {
        return nil, err
    }
    if resOp == protocol.OpError {
        return nil, decodeError(resBody)
    }
    return resBody, nil
}

func decodeError(body []byte) error {
    code, msg := protocol.DecodeError(body)
    switch code {
    case protocol.ErrCodeQueueExists:
        return ErrQueueExists
    case protocol.ErrCodeQueueNotFound:
        return ErrQueueNotFound
    case protocol.ErrCodeOffsetNotFound:
        return ErrOffsetNotFound
    default:
        return fmt.Errorf("server error: %s", msg)
    }
}

// Streamer is a fluent handle over a single queue. Returned by ProduceStream.
type Streamer struct {
    client *Client
    queue  string
}

// From returns a range-over-func iterator from startOffset up to the current
// tail. Use Client.Tail directly for live streaming past the tail.
func (s *Streamer) From(startOffset uint64) iter.Seq2[Message, error] {
    return s.client.Stream(context.Background(), s.queue, startOffset)
}

// JoinGroup returns a ConsumerGroup handle bound to this Client and queue.
func (c *Client) JoinGroup(queue, group string) *ConsumerGroup {
    return &ConsumerGroup{
        client:      c,
        queue:       queue,
        group:       group,
        autoCommit:  time.Second,
    }
}

// ConsumerGroup tracks a single group's progress against a single queue.
// The zero value is not usable; obtain instances via Client.JoinGroup.
type ConsumerGroup struct {
    client *Client
    queue  string
    group  string

    autoCommit time.Duration

    mu        sync.Mutex
    lastAcked uint64
    hasAcked  bool
}

// Stream yields messages starting at the last committed offset for the group.
// As messages are Marked, a background goroutine periodically flushes the
// highest acked offset to the server.
func (cg *ConsumerGroup) Stream(ctx context.Context) iter.Seq2[Message, error] {
    return func(yield func(Message, error) bool) {
        start, err := cg.client.FetchOffset(ctx, cg.queue, cg.group)
        if err != nil {
            yield(Message{}, fmt.Errorf("fetch committed offset: %w", err))
            return
        }

        if cg.autoCommit > 0 {
            go cg.autoCommitLoop(ctx)
        }

        curr := start
        ticker := time.NewTicker(50 * time.Millisecond)
        defer ticker.Stop()
        for {
            if err := ctx.Err(); err != nil {
                return
            }
            msg, err := cg.client.Read(ctx, cg.queue, curr)
            if errors.Is(err, ErrOffsetNotFound) {
                select {
                case <-ctx.Done():
                    return
                case <-ticker.C:
                    continue
                }
            }
            if !yield(msg, err) || err != nil {
                return
            }
            curr = msg.Offset + 1
        }
    }
}

// Mark records offset as successfully processed. The actual server commit
// is debounced and flushed by the background auto-committer.
func (cg *ConsumerGroup) Mark(offset uint64) {
    cg.mu.Lock()
    defer cg.mu.Unlock()
    if !cg.hasAcked || offset > cg.lastAcked {
        cg.lastAcked = offset
        cg.hasAcked = true
    }
}

// Commit immediately flushes offset to the server, bypassing auto-commit.
func (cg *ConsumerGroup) Commit(ctx context.Context, offset uint64) error {
    return cg.client.CommitOffset(ctx, cg.queue, cg.group, offset)
}

func (cg *ConsumerGroup) autoCommitLoop(ctx context.Context) {
    ticker := time.NewTicker(cg.autoCommit)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            cg.flush(context.Background())
            return
        case <-ticker.C:
            cg.flush(ctx)
        }
    }
}

func (cg *ConsumerGroup) flush(ctx context.Context) {
    cg.mu.Lock()
    if !cg.hasAcked {
        cg.mu.Unlock()
        return
    }
    offset := cg.lastAcked
    cg.hasAcked = false
    cg.mu.Unlock()

    _ = cg.client.CommitOffset(ctx, cg.queue, cg.group, offset)
}