package mikyu

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
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

type Client struct {
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

func Dial(addr string, opts ...DialOption) (*Client, error) {
	cfg := dialConfig{addr: addr, maxConns: 10}
	for _, o := range opts {
		o.apply(&cfg)
	}
	pool := make(chan *clientConn, cfg.maxConns)
	return &Client{addr: addr, pool: pool, maxConns: cfg.maxConns}, nil
}

type dialConfig struct {
	addr     string
	maxConns int
}

type DialOption interface {
	apply(*dialConfig)
}

type maxConnsOption int

func (m maxConnsOption) apply(c *dialConfig) { c.maxConns = int(m) }

func WithMaxConns(n int) DialOption { return maxConnsOption(n) }

func (c *Client) acquire(ctx context.Context) (*clientConn, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case ch := <-c.pool:
		return ch, nil
	default:
		conn, err := net.Dial("tcp", c.addr)
		if err != nil {
			return nil, err
		}
		return &clientConn{conn: conn, r: bufio.NewReader(conn)}, nil
	}
}

func (c *Client) release(ch *clientConn) {
	if ch == nil {
		return
	}
	c.mu.Lock()
	closed := c.closed.Load()
	c.mu.Unlock()
	if closed {
		ch.conn.Close()
		return
	}
	select {
	case c.pool <- ch:
	default:
		ch.conn.Close()
	}
}

func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		return nil
	}
	c.closed.Store(true)
	c.mu.Unlock()

	for {
		select {
		case ch := <-c.pool:
			if ch != nil && ch.conn != nil {
				ch.conn.Close()
			}
		default:
			return nil
		}
	}
}

func (c *Client) CreateQueue(ctx context.Context, name string) error {
	var body protocol.Buffer
	body.WriteBytes([]byte(name))

	_, err := c.exchange(ctx, protocol.OpCreateQueue, body.Bytes())
	return err
}

func (c *Client) Produce(ctx context.Context, queue string, payload []byte) (uint64, error) {
	var body protocol.Buffer
	body.WriteBytes([]byte(queue))
	body.WriteBytes(payload)

	resp, err := c.exchange(ctx, protocol.OpProduce, body.Bytes())
	if err != nil {
		return 0, err
	}

	offset, err := protocol.NewReader(resp).ReadU64()
	if err != nil {
		return 0, fmt.Errorf("invalid response payload: %w", err)
	}
	return offset, nil
}

func (c *Client) Read(ctx context.Context, queue string, offset uint64) (Message, error) {
	var body protocol.Buffer
	body.WriteBytes([]byte(queue))
	body.WriteU64(offset)

	resp, err := c.exchange(ctx, protocol.OpRead, body.Bytes())
	if err != nil {
		return Message{}, err
	}

	r := protocol.NewReader(resp)
	respOffset, err := r.ReadU64()
	if err != nil {
		return Message{}, fmt.Errorf("invalid response offset: %w", err)
	}
	id, err := r.ReadBytes()
	if err != nil {
		return Message{}, fmt.Errorf("invalid response ID: %w", err)
	}
	tsNano, err := r.ReadU64()
	if err != nil {
		return Message{}, fmt.Errorf("invalid response timestamp: %w", err)
	}
	payload, err := r.ReadBytes()
	if err != nil {
		return Message{}, fmt.Errorf("invalid response payload: %w", err)
	}

	return Message{Offset: respOffset, Timestamp: time.Unix(0, int64(tsNano)), ID: string(id), Payload: payload}, nil
}

func (c *Client) LastOffset(ctx context.Context, queue string) (uint64, error) {
	var body protocol.Buffer
	body.WriteBytes([]byte(queue))

	resp, err := c.exchange(ctx, protocol.OpTail, body.Bytes())
	if err != nil {
		return 0, err
	}

	offset, err := protocol.NewReader(resp).ReadU64()
	if err != nil {
		return 0, fmt.Errorf("invalid response offset: %w", err)
	}
	return offset, nil
}

func (c *Client) CommitOffset(ctx context.Context, queue, group string, offset uint64) error {
	var body protocol.Buffer
	body.WriteBytes([]byte(queue))
	body.WriteBytes([]byte(group))
	body.WriteU64(offset)

	_, err := c.exchange(ctx, protocol.OpCommitOffset, body.Bytes())
	return err
}

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

func (c *Client) exchange(ctx context.Context, op byte, body []byte) ([]byte, error) {
	ch, err := c.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer c.release(ch)

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if deadline, ok := ctx.Deadline(); ok {
		ch.conn.SetDeadline(deadline)
	} else {
		ch.conn.SetDeadline(time.Time{})
	}
	defer ch.conn.SetDeadline(time.Time{})

	if err := protocol.WriteFrame(ch.conn, op, body); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("write failed: %w", err)
	}

	respOp, respBody, err := protocol.ReadFrame(ch.r)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("read failed: %w", err)
	}

	if respOp == protocol.OpError {
		return nil, decodeError(respBody)
	}
	if respOp != op {
		return nil, fmt.Errorf("unexpected response opcode: 0x%02x (expected 0x%02x)", respOp, op)
	}

	return respBody, nil
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
		return errors.New(msg)
	}
}
