package mikyu

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"iter"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dmunish/mikyu/internal/protocol"
)

type remoteClient struct {
	network  string
	addr     string
	pool     chan *clientConn
	maxConns int
	mu       sync.Mutex
	closed   atomic.Bool
}

type clientConn struct {
	conn net.Conn
	r    *bufio.Reader
}

// Dial connects to a remote or daemon Mikyu instance target
// (e.g. "tcp://127.0.0.1:7420", "unix:///tmp/mikyu.sock", or "pipe://./pipe/mikyu").
func Dial(target string, opts ...ClientOption) (Client, error) {
	cfg := DefaultClientConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	network, addr, err := protocol.ParseTarget(target)
	if err != nil {
		return nil, fmt.Errorf("invalid target URI %q: %w", target, err)
	}

	pool := make(chan *clientConn, cfg.MaxConns)
	return &remoteClient{
		network:  network,
		addr:     addr,
		pool:     pool,
		maxConns: cfg.MaxConns,
	}, nil
}

func (c *remoteClient) acquire(ctx context.Context) (*clientConn, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case ch := <-c.pool:
		return ch, nil
	default:
		var d net.Dialer
		conn, err := d.DialContext(ctx, c.network, c.addr)
		if err != nil {
			return nil, err
		}
		return &clientConn{conn: conn, r: bufio.NewReader(conn)}, nil
	}
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
	if c.closed.Swap(true) {
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
		return nil, fmt.Errorf("write frame failed: %w", err)
	}

	respOp, respBody, err := protocol.ReadFrame(ch.r)
	if err != nil {
		return nil, fmt.Errorf("read frame failed: %w", err)
	}

	if respOp == protocol.OpError {
		return nil, decodeProtocolError(respBody)
	}
	if respOp != op {
		return nil, fmt.Errorf("unexpected response opcode 0x%02x (expected 0x%02x)", respOp, op)
	}

	return respBody, nil
}

func (c *remoteClient) CreateQueue(ctx context.Context, name string, opts ...QueueOption) (Queue, error) {
	var body protocol.Buffer
	body.WriteBytes([]byte(name))

	if _, err := c.exchange(ctx, protocol.OpCreateQueue, body.Bytes()); err != nil {
		return nil, err
	}
	return &remoteQueue{client: c, name: name}, nil
}

func (c *remoteClient) Queue(name string) (Queue, error) {
	return &remoteQueue{client: c, name: name}, nil
}

func (c *remoteClient) ListQueues(ctx context.Context) ([]string, error) {
	resp, err := c.exchange(ctx, protocol.OpListQueues, nil)
	if err != nil {
		return nil, err
	}
	r := protocol.NewReader(resp)
	count, err := r.ReadU32()
	if err != nil {
		return nil, err
	}
	queues := make([]string, count)
	for i := range count {
		name, err := r.ReadBytes()
		if err != nil {
			return nil, err
		}
		queues[i] = string(name)
	}
	return queues, nil
}

func (c *remoteClient) DeleteQueue(ctx context.Context, name string) error {
	var body protocol.Buffer
	body.WriteBytes([]byte(name))
	_, err := c.exchange(ctx, protocol.OpDeleteQueue, body.Bytes())
	return err
}

func (c *remoteClient) Stats(ctx context.Context) (SystemStats, error) {
	resp, err := c.exchange(ctx, protocol.OpStats, nil)
	if err != nil {
		return SystemStats{}, err
	}
	r := protocol.NewReader(resp)
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

// remoteQueue implements Queue over the remote connection pool.
type remoteQueue struct {
	client *remoteClient
	name   string
}

func (q *remoteQueue) Name() string {
	return q.name
}

func (q *remoteQueue) Produce(ctx context.Context, payload []byte) (uint64, error) {
	var body protocol.Buffer
	body.WriteBytes([]byte(q.name))
	body.WriteBytes(payload)

	resp, err := q.client.exchange(ctx, protocol.OpProduce, body.Bytes())
	if err != nil {
		return 0, err
	}
	return protocol.NewReader(resp).ReadU64()
}

func (q *remoteQueue) ProduceBatch(ctx context.Context, payloads [][]byte) ([]uint64, error) {
	var body protocol.Buffer
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

func (q *remoteQueue) Read(ctx context.Context, offset uint64) (Message, error) {
	var body protocol.Buffer
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
	id, err := r.ReadBytes()
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
		ID:        string(id),
		Timestamp: time.Unix(0, int64(tsNano)),
		Payload:   payload,
	}, nil
}

func (q *remoteQueue) ReadBatch(ctx context.Context, startOffset uint64, count int) ([]Message, error) {
	var body protocol.Buffer
	body.WriteBytes([]byte(q.name))
	body.WriteU64(startOffset)
	body.WriteU32(uint32(count))

	resp, err := q.client.exchange(ctx, protocol.OpReadBatch, body.Bytes())
	if err != nil {
		return nil, err
	}

	r := protocol.NewReader(resp)
	respCount, err := r.ReadU32()
	if err != nil {
		return nil, err
	}

	msgs := make([]Message, respCount)
	for i := range respCount {
		off, _ := r.ReadU64()
		id, _ := r.ReadBytes()
		tsNano, _ := r.ReadU64()
		payload, _ := r.ReadBytes()
		msgs[i] = Message{
			Offset:    off,
			ID:        string(id),
			Timestamp: time.Unix(0, int64(tsNano)),
			Payload:   payload,
		}
	}
	return msgs, nil
}

func (q *remoteQueue) LastOffset(ctx context.Context) (uint64, error) {
	var body protocol.Buffer
	body.WriteBytes([]byte(q.name))

	resp, err := q.client.exchange(ctx, protocol.OpTail, body.Bytes())
	if err != nil {
		return 0, err
	}
	return protocol.NewReader(resp).ReadU64()
}

func (q *remoteQueue) Stream(ctx context.Context, startOffset uint64) iter.Seq2[Message, error] {
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

func (q *remoteQueue) Tail(ctx context.Context, startOffset uint64) iter.Seq2[Message, error] {
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

func (q *remoteQueue) Group(name string) Group {
	return &remoteGroup{
		client:    q.client,
		queueName: q.name,
		groupName: name,
	}
}

type remoteGroup struct {
	client    *remoteClient
	queueName string
	groupName string
	mu        sync.Mutex
	highWater uint64
}

func (g *remoteGroup) Name() string {
	return g.groupName
}

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

	var body protocol.Buffer
	body.WriteBytes([]byte(g.queueName))
	body.WriteBytes([]byte(g.groupName))
	body.WriteU64(off)

	_, err := g.client.exchange(ctx, protocol.OpCommitOffset, body.Bytes())
	return err
}

func (g *remoteGroup) FetchOffset(ctx context.Context) (uint64, error) {
	var body protocol.Buffer
	body.WriteBytes([]byte(g.queueName))
	body.WriteBytes([]byte(g.groupName))

	resp, err := g.client.exchange(ctx, protocol.OpFetchOffset, body.Bytes())
	if err != nil {
		return 0, err
	}
	return protocol.NewReader(resp).ReadU64()
}

func (g *remoteGroup) Close() error {
	return g.Commit(context.Background())
}

func decodeProtocolError(body []byte) error {
	code, msg := protocol.DecodeError(body)
	switch code {
	case protocol.ErrCodeQueueExists:
		return ErrQueueExists
	case protocol.ErrCodeQueueNotFound:
		return ErrQueueNotFound
	case protocol.ErrCodeOffsetNotFound:
		return ErrOffsetNotFound
	default:
		return errors.New(msg)
	}
}
