package client

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/ipc"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

// remoteClient implements Client over a network connection pool.
type remoteClient struct {
	target   string
	network  string
	addr     string
	cfg      SessionConfig
	tls      *tls.Config
	pool     chan *clientConn
	maxConns int
	connSem  chan struct{}
	mu       sync.Mutex
	closed   atomic.Bool
}

type clientConn struct {
	conn net.Conn
	r    *bufio.Reader
}

// NewRemote creates a lazy remote client for target.
func NewRemote(target string, opts ...Option) (*remoteClient, error) {
	cfg := DefaultSessionConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.Target != "" {
		target = cfg.Target
	}
	network, addr, err := protocol.ParseTarget(target)
	if err != nil {
		return nil, err
	}
	tlsConfig, err := cfg.TLSConfig()
	if err != nil {
		return nil, err
	}
	c := &remoteClient{
		target:   target,
		network:  network,
		addr:     addr,
		cfg:      cfg,
		tls:      tlsConfig,
		pool:     make(chan *clientConn, cfg.MaxConns),
		maxConns: cfg.MaxConns,
		connSem:  make(chan struct{}, cfg.MaxConns),
	}
	return c, nil
}

func (c *remoteClient) acquire(ctx context.Context) (*clientConn, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case c.connSem <- struct{}{}:
	}

	select {
	case <-ctx.Done():
		<-c.connSem
		return nil, ctx.Err()
	case ch := <-c.pool:
		return ch, nil
	default:
		ch, err := c.dialConn(ctx)
		if err != nil {
			<-c.connSem
			return nil, err
		}
		return ch, nil
	}
}

func (c *remoteClient) dialConn(ctx context.Context) (*clientConn, error) {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	var conn net.Conn
	var err error
	if c.network == "unix" || c.network == "pipe" {
		conn, err = ipc.DialContext(ctx, c.target)
	} else {
		if c.tls != nil {
			conn, err = (&tls.Dialer{Config: c.tls, NetDialer: dialer}).DialContext(ctx, c.network, c.addr)
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
	defer func() { <-c.connSem }()
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

// discard releases the connection slot and closes the connection. A round
// trip that errored or was cancelled may have left a response (or half of
// one) in flight, so such a connection must never return to the pool.
func (c *remoteClient) discard(ch *clientConn) {
	defer func() { <-c.connSem }()
	if ch == nil {
		return
	}
	_ = ch.conn.Close()
}

func (c *remoteClient) exchange(ctx context.Context, op byte, body []byte) ([]byte, error) {
	ch, err := c.acquire(ctx)
	if err != nil {
		return nil, err
	}

	if deadline, ok := ctx.Deadline(); ok {
		_ = ch.conn.SetDeadline(deadline.Add(time.Second))
	} else {
		_ = ch.conn.SetDeadline(time.Time{})
	}

	woken := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		defer close(woken)
		select {
		case <-ctx.Done():
			_ = ch.conn.SetDeadline(time.Unix(0, 1))
		case <-stop:
		}
	}()

	respOp, respBody, rerr := c.roundTrip(ch, op, body)
	close(stop)
	<-woken
	_ = ch.conn.SetDeadline(time.Time{})

	switch {
	case ctx.Err() != nil:
		// Cancellation wins even if the response raced in
		c.discard(ch)
		return nil, ctx.Err()
	case rerr != nil:
		if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
			c.discard(ch)
			return nil, context.DeadlineExceeded
		}
		c.discard(ch)
		return nil, rerr
	}
	if respOp == protocol.OpError {
		code, msg := protocol.DecodeError(respBody)
		c.release(ch)
		return nil, decodeProtocolError(code, msg)
	}
	c.release(ch)
	return respBody, nil
}

// roundTrip performs exactly one request/response frame exchange on ch.
func (c *remoteClient) roundTrip(ch *clientConn, op byte, body []byte) (byte, []byte, error) {
	if err := protocol.WriteFrame(ch.conn, op, body); err != nil {
		return 0, nil, err
	}
	return protocol.ReadFrame(ch.r)
}

// writeSpecFrame encodes a spec-bearing admin request:
// bytes(name) u16 specVersion bytes(json Spec).
func writeSpecFrame(name string, data []byte) []byte {
	var body protocol.Buffer
	body.WriteBytes([]byte(name))
	body.WriteU16(spec.CurrentSpecVersion)
	body.WriteBytes(data)
	return body.Bytes()
}

func (c *remoteClient) CreateQueue(ctx context.Context, name string, opts ...QueueOption) error {
	data, err := spec.EncodeSpec(applyQueueOptions(opts))
	if err != nil {
		return err
	}
	_, err = c.exchange(ctx, protocol.OpCreateQueue, writeSpecFrame(name, data))
	return err
}

func (c *remoteClient) UpdateQueue(ctx context.Context, name string, opts ...QueueOption) error {
	data, err := spec.EncodeSpec(applyQueueOptions(opts))
	if err != nil {
		return err
	}
	_, err = c.exchange(ctx, protocol.OpUpdateQueue, writeSpecFrame(name, data))
	return err
}

func (c *remoteClient) DescribeQueue(ctx context.Context, name string) (QueueDescription, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(name))
	resp, err := c.exchange(ctx, protocol.OpDescribeQueue, body.Bytes())
	if err != nil {
		return QueueDescription{}, err
	}
	r := protocol.NewReader(resp)
	version, err := r.ReadU16()
	if err != nil {
		return QueueDescription{}, fmt.Errorf("%w: %v", ErrProtocol, err)
	}
	if err := spec.CheckSpecVersion(version); err != nil {
		return QueueDescription{}, err
	}
	data, err := r.ReadBytes()
	if err != nil {
		return QueueDescription{}, fmt.Errorf("%w: %v", ErrProtocol, err)
	}
	if len(data) > spec.MaxSpecPayload {
		return QueueDescription{}, fmt.Errorf("%w: describe payload is %d bytes, limit is %d", ErrProtocol, len(data), spec.MaxSpecPayload)
	}
	var d QueueDescription
	if err := json.Unmarshal(data, &d); err != nil {
		return QueueDescription{}, fmt.Errorf("%w: %v", ErrProtocol, err)
	}
	return d, nil
}

// Metrics returns the server's live snapshot over OpMetrics.
func (c *remoteClient) Metrics(ctx context.Context) (metrics.Snapshot, error) {
	resp, err := c.exchange(ctx, protocol.OpMetrics, nil)
	if err != nil {
		return metrics.Snapshot{}, err
	}
	r := protocol.NewReader(resp)
	version, err := r.ReadU16()
	if err != nil {
		return metrics.Snapshot{}, fmt.Errorf("%w: %v", ErrProtocol, err)
	}
	if err := spec.CheckSpecVersion(version); err != nil {
		return metrics.Snapshot{}, err
	}
	data, err := r.ReadBytes()
	if err != nil {
		return metrics.Snapshot{}, fmt.Errorf("%w: %v", ErrProtocol, err)
	}
	if len(data) > spec.MaxSpecPayload {
		return metrics.Snapshot{}, fmt.Errorf("%w: metrics payload is %d bytes, limit is %d", ErrProtocol, len(data), spec.MaxSpecPayload)
	}
	var snap metrics.Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return metrics.Snapshot{}, fmt.Errorf("%w: %v", ErrProtocol, err)
	}
	return snap, nil
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

func (c *remoteClient) Truncate(ctx context.Context, name string, minOffset uint64) (int64, error) {
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

var _ interface {
	io.Closer
	CreateQueue(context.Context, string, ...QueueOption) error
	UpdateQueue(context.Context, string, ...QueueOption) error
	DescribeQueue(context.Context, string) (QueueDescription, error)
	LogQueue(string) (logqueue.ClientLogQueue, error)
	TaskQueue(string) (TaskQueue, error)
	ListQueues(context.Context) ([]string, error)
	DeleteQueue(context.Context, string) error
	Truncate(context.Context, string, uint64) (int64, error)
	Metrics(context.Context) (metrics.Snapshot, error)
} = (*remoteClient)(nil)
