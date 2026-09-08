package client

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"iter"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/group"
	"github.com/dmunish/mikyu/internal/ipc"
	"github.com/dmunish/mikyu/internal/logqueue"
	"github.com/dmunish/mikyu/internal/protocol"
	"github.com/dmunish/mikyu/internal/taskqueue"
)

// remoteClient implements Client over a network connection pool.
type remoteClient struct {
	target   string
	network  string
	addr     string
	cfg      Config
	pool     chan *clientConn
	maxConns int
	mu       sync.Mutex
	closed   atomic.Bool
}

type clientConn struct {
	conn net.Conn
	r    *bufio.Reader
}

// NewRemote creates a lazy remote client for target.
func NewRemote(target string, opts ...Option) (*remoteClient, error) {
	cfg := DefaultClientConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	network, addr, err := protocol.ParseTarget(target)
	if err != nil {
		return nil, err
	}
	c := &remoteClient{
		target:   target,
		network:  network,
		addr:     addr,
		cfg:      cfg,
		pool:     make(chan *clientConn, cfg.MaxConns),
		maxConns: cfg.MaxConns,
	}
	return c, nil
}

func (c *remoteClient) acquire(ctx context.Context) (*clientConn, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case ch := <-c.pool:
		return ch, nil
	default:
		return c.dialConn(ctx)
	}
}

func (c *remoteClient) dialConn(ctx context.Context) (*clientConn, error) {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	var conn net.Conn
	var err error
	if c.network == "unix" || c.network == "pipe" {
		conn, err = ipc.DialContext(ctx, c.target)
	} else {
		if c.cfg.TLSConfig != nil {
			conn, err = (&tls.Dialer{Config: c.cfg.TLSConfig, NetDialer: dialer}).DialContext(ctx, c.network, c.addr)
		} else {
			conn, err = dialer.DialContext(ctx, c.network, c.addr)
		}
	}
	if err != nil {
		return nil, err
	}
	return &clientConn{conn: conn, r: bufio.NewReader(conn)}, nil
}

func (c *remoteClient) release(ch *clientConn) {
	if ch == nil {
		return
	}
	if c.closed.Load() {
		_ = ch.conn.Close()
		return
	}
	select {
	case c.pool <- ch:
	default:
		_ = ch.conn.Close()
	}
}

func (c *remoteClient) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	for {
		select {
		case ch := <-c.pool:
			if ch != nil && ch.conn != nil {
				_ = ch.conn.Close()
			}
		default:
			return nil
		}
	}
}

func (c *remoteClient) exchange(ctx context.Context, op byte, body []byte) ([]byte, error) {
	ch, err := c.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer c.release(ch)

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if deadline, ok := ctx.Deadline(); ok {
		_ = ch.conn.SetDeadline(deadline)
	} else {
		_ = ch.conn.SetDeadline(time.Time{})
	}
	defer func() { _ = ch.conn.SetDeadline(time.Time{}) }()

	if err := protocol.WriteFrame(ch.conn, op, body); err != nil {
		return nil, err
	}
	respOp, respBody, err := protocol.ReadFrame(ch.r)
	if err != nil {
		return nil, err
	}
	if respOp == protocol.OpError {
		code, msg := protocol.DecodeError(respBody)
		return nil, decodeProtocolError(code, msg)
	}
	return respBody, nil
}

func (c *remoteClient) CreateQueue(ctx context.Context, name string, opts ...QueueOption) error {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(name))
	_, err := c.exchange(ctx, protocol.OpCreateQueue, body.Bytes())
	return err
}

func (c *remoteClient) LogQueue(name string) (logqueue.ClientLogQueue, error) {
	return &remoteLogQueue{client: c, name: name}, nil
}

func (c *remoteClient) TaskQueue(name string) (TaskQueue, error) {
	return &remoteTaskQueue{client: c, name: name}, nil
}

func (c *remoteClient) ListQueues(ctx context.Context) ([]string, error) {
	body, err := c.exchange(ctx, protocol.OpListQueues, nil)
	if err != nil {
		return nil, err
	}
	r := protocol.NewReader(body)
	count, err := r.ReadU32()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, count)
	for range count {
		name, err := r.ReadBytes()
		if err != nil {
			return nil, err
		}
		names = append(names, string(name))
	}
	return names, nil
}

func (c *remoteClient) DeleteQueue(ctx context.Context, name string) error {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(name))
	_, err := c.exchange(ctx, protocol.OpDeleteQueue, body.Bytes())
	return err
}

func (c *remoteClient) TruncateQueue(ctx context.Context, name string, minOffset uint64) (int64, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(name))
	body.WriteU64(minOffset)
	resp, err := c.exchange(ctx, protocol.OpTruncate, body.Bytes())
	if err != nil {
		return 0, err
	}
	r := protocol.NewReader(resp)
	reclaimed, err := r.ReadU64()
	if err != nil {
		return 0, err
	}
	return int64(reclaimed), nil
}

func (c *remoteClient) Stats(ctx context.Context) (engine.EngineStats, error) {
	body, err := c.exchange(ctx, protocol.OpStats, nil)
	if err != nil {
		return engine.EngineStats{}, err
	}
	r := protocol.NewReader(body)
	active, err := r.ReadU32()
	if err != nil {
		return engine.EngineStats{}, err
	}
	totalBytes, err := r.ReadU64()
	if err != nil {
		return engine.EngineStats{}, err
	}
	return engine.EngineStats{
		ActiveQueues: int(active),
		TotalBytes:   int64(totalBytes),
		Queues:       make(map[string]engine.QueueStats),
	}, nil
}

func decodeProtocolError(code byte, msg string) error {
	switch code {
	case protocol.ErrCodeQueueNotFound:
		return engine.ErrQueueNotFound
	case protocol.ErrCodeQueueExists:
		return engine.ErrQueueExists
	case protocol.ErrCodeOffsetNotFound:
		return engine.ErrOffsetNotFound
	case protocol.ErrCodeOffsetTruncated:
		return engine.ErrOffsetTruncated
	case protocol.ErrCodeTaskNotAvailable:
		return taskqueue.ErrTaskNotAvailable
	case protocol.ErrCodeTaskNotFound:
		return taskqueue.ErrTaskNotFound
	case protocol.ErrCodeTaskNotInflight:
		return taskqueue.ErrTaskNotInflight
	case protocol.ErrCodeTaskStale:
		return taskqueue.ErrTaskStale
	case protocol.ErrCodeTaskClosed:
		return taskqueue.ErrTaskClosed
	case protocol.ErrCodeInvalidTask:
		return taskqueue.ErrInvalidTask
	case protocol.ErrCodeDeadLetterNotFound:
		return taskqueue.ErrDeadLetterNotFound
	case protocol.ErrCodeDeadLetterStale:
		return taskqueue.ErrDeadLetterStale
	default:
		return errors.New(msg)
	}
}

type remoteLogQueue struct {
	client *remoteClient
	name   string
}

func (q *remoteLogQueue) Name() string { return q.name }

func (q *remoteLogQueue) Produce(ctx context.Context, payload []byte) (uint64, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(q.name))
	body.WriteBytes(payload)
	resp, err := q.client.exchange(ctx, protocol.OpProduce, body.Bytes())
	if err != nil {
		return 0, err
	}
	r := protocol.NewReader(resp)
	return r.ReadU64()
}

func (q *remoteLogQueue) ProduceBatch(ctx context.Context, payloads [][]byte) ([]uint64, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(q.name))
	body.WriteU32(uint32(len(payloads)))
	for _, p := range payloads {
		body.WriteBytes(p)
	}
	resp, err := q.client.exchange(ctx, protocol.OpProduceBatch, body.Bytes())
	if err != nil {
		return nil, err
	}
	r := protocol.NewReader(resp)
	count, err := r.ReadU32()
	if err != nil {
		return nil, err
	}
	offsets := make([]uint64, count)
	for i := range count {
		off, err := r.ReadU64()
		if err != nil {
			return nil, err
		}
		offsets[i] = off
	}
	return offsets, nil
}

func (q *remoteLogQueue) Read(ctx context.Context, offset uint64) (engine.Message, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(q.name))
	body.WriteU64(offset)
	resp, err := q.client.exchange(ctx, protocol.OpRead, body.Bytes())
	if err != nil {
		return engine.Message{}, err
	}
	r := protocol.NewReader(resp)
	off, err := r.ReadU64()
	if err != nil {
		return engine.Message{}, err
	}
	idBytes, err := r.ReadBytes()
	if err != nil {
		return engine.Message{}, err
	}
	tsNano, err := r.ReadU64()
	if err != nil {
		return engine.Message{}, err
	}
	payload, err := r.ReadBytes()
	if err != nil {
		return engine.Message{}, err
	}
	return engine.Message{
		Offset:    off,
		ID:        string(idBytes),
		Timestamp: time.Unix(0, int64(tsNano)),
		Payload:   payload,
	}, nil
}

func (q *remoteLogQueue) ReadBatch(ctx context.Context, startOffset uint64, count int) ([]engine.Message, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(q.name))
	body.WriteU64(startOffset)
	body.WriteU32(uint32(count))
	resp, err := q.client.exchange(ctx, protocol.OpReadBatch, body.Bytes())
	if err != nil {
		return nil, err
	}
	r := protocol.NewReader(resp)
	n, err := r.ReadU32()
	if err != nil {
		return nil, err
	}
	msgs := make([]engine.Message, 0, n)
	for range n {
		off, _ := r.ReadU64()
		idBytes, _ := r.ReadBytes()
		tsNano, _ := r.ReadU64()
		payload, _ := r.ReadBytes()
		msgs = append(msgs, engine.Message{
			Offset:    off,
			ID:        string(idBytes),
			Timestamp: time.Unix(0, int64(tsNano)),
			Payload:   payload,
		})
	}
	return msgs, nil
}

func (q *remoteLogQueue) LastOffset(ctx context.Context) (uint64, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(q.name))
	resp, err := q.client.exchange(ctx, protocol.OpTail, body.Bytes())
	if err != nil {
		return 0, err
	}
	r := protocol.NewReader(resp)
	return r.ReadU64()
}

func (q *remoteLogQueue) MinOffset(ctx context.Context) (uint64, error) {
	stats, err := q.client.Stats(ctx)
	if err != nil {
		return 0, err
	}
	qs, ok := stats.Queues[q.name]
	if !ok {
		return 0, engine.ErrQueueNotFound
	}
	return qs.MinOffset, nil
}

func (q *remoteLogQueue) Truncate(ctx context.Context, minOffset uint64) (int64, error) {
	return q.client.TruncateQueue(ctx, q.name, minOffset)
}

func (q *remoteLogQueue) Stream(ctx context.Context, startOffset uint64) iter.Seq2[engine.Message, error] {
	return func(yield func(engine.Message, error) bool) {
		for curr := startOffset; ; curr++ {
			if err := ctx.Err(); err != nil {
				yield(engine.Message{}, err)
				return
			}
			msg, err := q.Read(ctx, curr)
			if errors.Is(err, engine.ErrOffsetNotFound) {
				return
			}
			if !yield(msg, err) || err != nil {
				return
			}
		}
	}
}

func (q *remoteLogQueue) Tail(ctx context.Context, startOffset uint64) iter.Seq2[engine.Message, error] {
	return func(yield func(engine.Message, error) bool) {
		curr := startOffset
		for {
			if err := ctx.Err(); err != nil {
				yield(engine.Message{}, err)
				return
			}
			last, err := q.LastOffset(ctx)
			if err != nil {
				// transient? retry after sleep
				if isRetryable(err) {
					select {
					case <-ctx.Done():
						return
					case <-time.After(5 * time.Millisecond):
						continue
					}
				}
				yield(engine.Message{}, err)
				return
			}
			if curr >= last {
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Millisecond):
					continue
				}
			}
			msg, err := q.Read(ctx, curr)
			if err != nil {
				if isRetryable(err) {
					select {
					case <-ctx.Done():
						return
					case <-time.After(5 * time.Millisecond):
						continue
					}
				}
				yield(engine.Message{}, err)
				return
			}
			if !yield(msg, nil) {
				return
			}
			curr++
		}
	}
}

func (q *remoteLogQueue) Group(name string) logqueue.Group {
	g := &remoteGroup{queue: q}
	g.Base.Name = name
	g.Base.CommitFn = func(ctx context.Context, offset uint64) error {
		body := protocol.Buffer{}
		body.WriteBytes([]byte(q.name))
		body.WriteBytes([]byte(name))
		body.WriteU64(offset)
		_, err := q.client.exchange(ctx, protocol.OpCommitOffset, body.Bytes())
		return err
	}
	return g
}

type remoteGroup struct {
	group.Base
	queue *remoteLogQueue
}

func (g *remoteGroup) Name() string {
	return g.Base.Name
}

func (g *remoteGroup) FetchOffset(ctx context.Context) (uint64, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(g.queue.name))
	body.WriteBytes([]byte(g.Base.Name))
	resp, err := g.queue.client.exchange(ctx, protocol.OpFetchOffset, body.Bytes())
	if err != nil {
		return 0, err
	}
	r := protocol.NewReader(resp)
	return r.ReadU64()
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
} = (*remoteClient)(nil)

func isRetryable(err error) bool {
	return errors.Is(err, engine.ErrOffsetNotFound) ||
		errors.Is(err, engine.ErrOffsetTruncated) || // optional; truncated offsets won't recover, but you may want to fail fast
		errors.Is(err, engine.ErrCorruptRecord) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF)
}

type remoteTaskQueue struct {
	client *remoteClient
	name   string
}

func (q *remoteTaskQueue) Close() error { return nil }
func (q *remoteTaskQueue) Name() string { return q.name }

func (q *remoteTaskQueue) Enqueue(ctx context.Context, payload []byte, opts ...taskqueue.TaskOption) (taskqueue.Task, error) {
	visibility, maxAttempts, scheduled := taskqueue.ResolveTaskOptions(opts...)
	if visibility <= 0 || maxAttempts == 0 || scheduled.IsZero() {
		return taskqueue.Task{}, taskqueue.ErrInvalidTask
	}
	var b protocol.Buffer
	b.WriteBytes([]byte(q.name))
	b.WriteBytes(payload)
	b.WriteU32(maxAttempts)
	b.WriteU64(uint64(int64(visibility)))
	if scheduled.IsZero() {
		b.WriteU64(0)
	} else {
		b.WriteU64(uint64(scheduled.UnixNano()))
	}
	resp, err := q.client.exchange(ctx, protocol.OpTaskEnqueue, b.Bytes())
	if err != nil {
		return taskqueue.Task{}, err
	}
	r := protocol.NewReader(resp)
	return protocol.DecodeTask(r)
}

func (q *remoteTaskQueue) Dequeue(ctx context.Context) (taskqueue.Task, error) {
	for {
		var b protocol.Buffer
		b.WriteBytes([]byte(q.name))
		resp, err := q.client.exchange(ctx, protocol.OpTaskDequeue, b.Bytes())
		if err == nil {
			r := protocol.NewReader(resp)
			return protocol.DecodeTask(r)
		}
		if !errors.Is(err, taskqueue.ErrTaskNotAvailable) {
			return taskqueue.Task{}, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				drainRemoteTimer(timer)
			}
			return taskqueue.Task{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (q *remoteTaskQueue) Ack(ctx context.Context, task taskqueue.Task) error {
	var b protocol.Buffer
	b.WriteBytes([]byte(q.name))
	protocol.EncodeTask(&b, task)
	_, err := q.client.exchange(ctx, protocol.OpTaskAck, b.Bytes())
	return err
}

func (q *remoteTaskQueue) Fail(ctx context.Context, task taskqueue.Task, opts ...taskqueue.FailOption) error {
	delay := taskqueue.ResolveFailOptions(task, opts...)
	var b protocol.Buffer
	b.WriteBytes([]byte(q.name))
	protocol.EncodeTask(&b, task)
	b.WriteU64(uint64(int64(delay)))
	_, err := q.client.exchange(ctx, protocol.OpTaskFail, b.Bytes())
	return err
}

func (q *remoteTaskQueue) Extend(ctx context.Context, task taskqueue.Task, timeout time.Duration) error {
	if timeout <= 0 {
		return taskqueue.ErrInvalidTask
	}
	var b protocol.Buffer
	b.WriteBytes([]byte(q.name))
	protocol.EncodeTask(&b, task)
	b.WriteU64(uint64(int64(timeout)))
	_, err := q.client.exchange(ctx, protocol.OpTaskExtend, b.Bytes())
	return err
}

func (q *remoteTaskQueue) DeadLetters(ctx context.Context) ([]taskqueue.Task, error) {
	var b protocol.Buffer
	b.WriteBytes([]byte(q.name))
	resp, err := q.client.exchange(ctx, protocol.OpTaskDeadLetters, b.Bytes())
	if err != nil {
		return nil, err
	}
	r := protocol.NewReader(resp)
	return protocol.DecodeTaskList(r)
}

func (q *remoteTaskQueue) AckDeadLetter(ctx context.Context, task taskqueue.Task) error {
	var b protocol.Buffer
	b.WriteBytes([]byte(q.name))
	protocol.EncodeTask(&b, task)
	_, err := q.client.exchange(ctx, protocol.OpTaskDeadLetterAck, b.Bytes())
	return err
}

func drainRemoteTimer(t *time.Timer) {
	select {
	case <-t.C:
	default:
	}
}
