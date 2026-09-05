package mikyu

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"iter"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dmunish/mikyu/internal/ipc"
	"github.com/dmunish/mikyu/internal/protocol"
)

// remoteClient implements Client over a network connection pool.
type remoteClient struct {
	target   string
	network  string
	addr     string
	cfg      ClientConfig
	pool     chan *clientConn
	maxConns int
	mu       sync.Mutex
	closed   atomic.Bool
}

type clientConn struct {
	conn net.Conn
	r    *bufio.Reader
}

// Dial connects to a remote Mikyu server.
func Dial(target string, opts ...ClientOption) (Client, error) {
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

func (c *remoteClient) LogQueue(name string) (LogQueue, error) {
	return &remoteLogQueue{client: c, name: name}, nil
}

func (c *remoteClient) TaskQueue(name string) (TaskQueue, error) {
	return nil, ErrTaskNotSupported
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
	for i := uint32(0); i < count; i++ {
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

func (c *remoteClient) Stats(ctx context.Context) (SystemStats, error) {
	body, err := c.exchange(ctx, protocol.OpStats, nil)
	if err != nil {
		return SystemStats{}, err
	}
	r := protocol.NewReader(body)
	active, err := r.ReadU32()
	if err != nil {
		return SystemStats{}, err
	}
	totalBytes, err := r.ReadU64()
	if err != nil {
		return SystemStats{}, err
	}
	return SystemStats{
		ActiveQueues: int(active),
		TotalBytes:   int64(totalBytes),
		Queues:       make(map[string]QueueStats),
	}, nil
}

func decodeProtocolError(code byte, msg string) error {
	switch code {
	case protocol.ErrCodeQueueNotFound:
		return ErrQueueNotFound
	case protocol.ErrCodeQueueExists:
		return ErrQueueExists
	case protocol.ErrCodeOffsetNotFound:
		return ErrOffsetNotFound
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
	for i := uint32(0); i < count; i++ {
		off, err := r.ReadU64()
		if err != nil {
			return nil, err
		}
		offsets[i] = off
	}
	return offsets, nil
}

func (q *remoteLogQueue) Read(ctx context.Context, offset uint64) (Message, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(q.name))
	body.WriteU64(offset)
	resp, err := q.client.exchange(ctx, protocol.OpRead, body.Bytes())
	if err != nil {
		return Message{}, err
	}
	r := protocol.NewReader(resp)
	off, err := r.ReadU64()
	if err != nil {
		return Message{}, err
	}
	idBytes, err := r.ReadBytes()
	if err != nil {
		return Message{}, err
	}
	tsNano, err := r.ReadU64()
	if err != nil {
		return Message{}, err
	}
	payload, err := r.ReadBytes()
	if err != nil {
		return Message{}, err
	}
	return Message{
		Offset:    off,
		ID:        string(idBytes),
		Timestamp: time.Unix(0, int64(tsNano)),
		Payload:   payload,
	}, nil
}

func (q *remoteLogQueue) ReadBatch(ctx context.Context, startOffset uint64, count int) ([]Message, error) {
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
	msgs := make([]Message, 0, n)
	for i := uint32(0); i < n; i++ {
		off, _ := r.ReadU64()
		idBytes, _ := r.ReadBytes()
		tsNano, _ := r.ReadU64()
		payload, _ := r.ReadBytes()
		msgs = append(msgs, Message{
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

func (q *remoteLogQueue) Stream(ctx context.Context, startOffset uint64) iter.Seq2[Message, error] {
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

func (q *remoteLogQueue) Tail(ctx context.Context, startOffset uint64) iter.Seq2[Message, error] {
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
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Millisecond):
					continue
				}
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

func (q *remoteLogQueue) Group(name string) Group {
	return &remoteGroup{queue: q, name: name}
}

type remoteGroup struct {
	queue     *remoteLogQueue
	name      string
	mu        sync.Mutex
	highWater uint64
	closed    bool
}

func (g *remoteGroup) Name() string { return g.name }

func (g *remoteGroup) Mark(offset uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if offset >= g.highWater {
		g.highWater = offset + 1
	}
}

func (g *remoteGroup) Commit(ctx context.Context) error {
	g.mu.Lock()
	off := g.highWater
	g.mu.Unlock()
	body := protocol.Buffer{}
	body.WriteBytes([]byte(g.queue.name))
	body.WriteBytes([]byte(g.name))
	body.WriteU64(off)
	_, err := g.queue.client.exchange(ctx, protocol.OpCommitOffset, body.Bytes())
	return err
}

func (g *remoteGroup) FetchOffset(ctx context.Context) (uint64, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(g.queue.name))
	body.WriteBytes([]byte(g.name))
	resp, err := g.queue.client.exchange(ctx, protocol.OpFetchOffset, body.Bytes())
	if err != nil {
		return 0, err
	}
	r := protocol.NewReader(resp)
	return r.ReadU64()
}

func (g *remoteGroup) Close() error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil
	}
	g.closed = true
	g.mu.Unlock()
	return g.Commit(context.Background())
}

var _ Client = (*remoteClient)(nil)
