package client

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dmunish/mikyu/internal/daemon"
	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
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
	if cfg.DataDir != "" {
		return nil, fmt.Errorf("%w: data_dir is for embedded clients, use Open", ErrModeMismatch)
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
	if cfg.AutoStart {
		if err := daemon.EnsureDaemonRunning(daemon.SpawnOptions{
			Target:        target,
			DataDir:       cfg.DataDir,
			TLSEnabled:    cfg.TLSEnabled,
			TLSCert:       cfg.TLSCert,
			TLSKey:        cfg.TLSKey,
			AuthMethod:    cfg.AuthMethod,
			AuthTokenFile: cfg.AuthTokenFile,
			TLSClientCA:   cfg.TLSClientCA,
		}); err != nil {
			return nil, err
		}
	}
	c := &remoteClient{
		target:   target,
		network:  network,
		addr:     addr,
		cfg:      cfg,
		tls:      tlsConfig,
		pool:     make(chan *clientConn, cfg.MaxConnections),
		maxConns: cfg.MaxConnections,
		connSem:  make(chan struct{}, cfg.MaxConnections),
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
	method, cred, err := c.authCredential()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	ch := &clientConn{conn: conn, r: bufio.NewReader(conn)}
	if err := writeAuth(ch, method, cred); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return ch, nil
}

// authCredential resolves the method and credential this client presents; an explicit token wins over the implicit mTLS claim.
func (c *remoteClient) authCredential() (byte, []byte, error) {
	if c.cfg.AuthTokenFile != "" {
		data, err := os.ReadFile(c.cfg.AuthTokenFile)
		if err != nil {
			return 0, nil, err
		}
		return protocol.AuthMethodToken, []byte(strings.TrimSpace(string(data))), nil
	}
	if c.cfg.AuthToken != "" {
		return protocol.AuthMethodToken, []byte(c.cfg.AuthToken), nil
	}
	if c.tls != nil {
		return protocol.AuthMethodMTLS, nil, nil
	}
	return protocol.AuthMethodNone, nil, nil
}

// writeAuth sends OpAuth and validates the reply.
func writeAuth(ch *clientConn, method byte, cred []byte) error {
	var b protocol.Buffer
	protocol.EncodeAuth(&b, method, cred)
	if err := protocol.WriteFrame(ch.conn, protocol.OpAuth, b.Bytes()); err != nil {
		return err
	}
	respOp, respBody, err := protocol.ReadFrame(ch.r)
	if err != nil {
		return err
	}
	if respOp == protocol.OpError {
		code, msg := protocol.DecodeError(respBody)
		return decodeProtocolError(code, msg)
	}
	if respOp != protocol.OpAuth {
		return ErrProtocol
	}
	return nil
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
		err := decodeProtocolError(code, msg)
		if code == protocol.ErrCodeTooManyConnections {
			// The server closes after refusing the connection, so it must not
			// return to the pool.
			c.discard(ch)
			return nil, err
		}
		c.release(ch)
		return nil, err
	}
	c.release(ch)
	return respBody, nil
}

// roundTrip performs exactly one request/response frame exchange on ch.
func (c *remoteClient) roundTrip(ch *clientConn, op byte, body []byte) (byte, []byte, error) {
	if err := protocol.WriteFrame(ch.conn, op, body); err != nil {
		return 0, nil, err
	}
	if c.cfg.testFrameSent != nil {
		c.cfg.testFrameSent(op)
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

// writeCreateFrame encodes OpCreate: u8 kind, then the spec frame.
func writeCreateFrame(kind node.Kind, name string, data []byte) []byte {
	var body protocol.Buffer
	body.WriteU8(byte(kind))
	body.WriteBytes([]byte(name))
	body.WriteU16(spec.CurrentSpecVersion)
	body.WriteBytes(data)
	return body.Bytes()
}

// CreateLogQueue declares a log queue and returns its handle.
func (c *remoteClient) CreateLogQueue(ctx context.Context, name string, opts ...QueueOption) (logqueue.Log, error) {
	data, err := spec.EncodeSpec(applyQueueOptions(opts))
	if err != nil {
		return nil, err
	}
	if _, err := c.exchange(ctx, protocol.OpCreate, writeCreateFrame(node.KindLog, name, data)); err != nil {
		return nil, err
	}
	return &remoteLogQueue{client: c, name: name}, nil
}

// CreateTaskQueue declares a task queue and returns its handle.
func (c *remoteClient) CreateTaskQueue(ctx context.Context, name string, opts ...QueueOption) (taskqueue.Queue, error) {
	s := applyQueueOptions(opts)
	if err := rejectCompaction(s); err != nil {
		return nil, err
	}
	data, err := spec.EncodeSpec(s)
	if err != nil {
		return nil, err
	}
	if _, err := c.exchange(ctx, protocol.OpCreate, writeCreateFrame(node.KindTask, name, data)); err != nil {
		return nil, err
	}
	return &remoteTaskQueue{client: c, name: name}, nil
}

func (c *remoteClient) UpdateQueue(ctx context.Context, name string, opts ...QueueOption) error {
	data, err := spec.EncodeSpec(applyQueueOptions(opts))
	if err != nil {
		return err
	}
	_, err = c.exchange(ctx, protocol.OpUpdate, writeSpecFrame(name, data))
	return err
}

func (c *remoteClient) DescribeQueue(ctx context.Context, name string) (QueueDescription, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(name))
	resp, err := c.exchange(ctx, protocol.OpDescribe, body.Bytes())
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

// SubscribeSnapshots polls OpMetrics on the session's metrics interval until the returned cancel runs.
func (c *remoteClient) SubscribeSnapshots() (<-chan metrics.Snapshot, func()) {
	interval := c.cfg.MetricsInterval
	if interval <= 0 {
		interval = time.Second
	}
	ch := make(chan metrics.Snapshot, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer close(ch)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				snap, err := c.Metrics(ctx)
				if err != nil {
					continue
				}
				select {
				case ch <- snap:
				default:
				}
			}
		}
	}()
	return ch, cancel
}

// Schema fetches the server's parameter table over OpSchema.
func (c *remoteClient) Schema(ctx context.Context) ([]spec.Param, error) {
	resp, err := c.exchange(ctx, protocol.OpSchema, nil)
	if err != nil {
		return nil, err
	}
	r := protocol.NewReader(resp)
	version, err := r.ReadU16()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrProtocol, err)
	}
	if err := spec.CheckSpecVersion(version); err != nil {
		return nil, err
	}
	data, err := r.ReadBytes()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrProtocol, err)
	}
	if len(data) > spec.MaxSpecPayload {
		return nil, fmt.Errorf("%w: schema payload is %d bytes, limit is %d", ErrProtocol, len(data), spec.MaxSpecPayload)
	}
	var params []spec.Param
	if err := json.Unmarshal(data, &params); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrProtocol, err)
	}
	return params, nil
}

func (c *remoteClient) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	body, err := c.exchange(ctx, protocol.OpList, nil)
	if err != nil {
		return nil, err
	}
	r := protocol.NewReader(body)
	count, err := r.ReadU32()
	if err != nil {
		return nil, err
	}
	infos := make([]QueueInfo, 0, count)
	for range count {
		name, err := r.ReadBytes()
		if err != nil {
			return nil, err
		}
		kind, err := r.ReadU8()
		if err != nil {
			return nil, err
		}
		infos = append(infos, QueueInfo{Name: string(name), Kind: node.Kind(kind)})
	}
	return infos, nil
}

func (c *remoteClient) DeleteQueue(ctx context.Context, name string) error {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(name))
	_, err := c.exchange(ctx, protocol.OpDelete, body.Bytes())
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

var _ Client = (*remoteClient)(nil)
