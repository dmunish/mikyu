package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
	"github.com/dmunish/mikyu/internal/transport/ipc"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

// Server accepts client connections and dispatches wire protocol frames to a local node.Node.
type Server struct {
	node      *node.Node
	listener  net.Listener
	tlsConfig *tls.Config
	mu        sync.Mutex
	stopCh    chan struct{}
	doneCh    chan struct{}
	metrics   *serverMetrics
}

type ServerOption func(*Server)

// New creates a new Server instance backed by the provided Node.
func New(n *node.Node, opts ...ServerOption) *Server {
	s := &Server{
		node:    n,
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
		metrics: newServerMetrics(n.Collector().Registry().Scope(serverScopeName)),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func WithTLS(tlsConfig *tls.Config) ServerOption {
	return func(s *Server) {
		s.tlsConfig = tlsConfig
	}
}

// getOrCreateLogQueue returns the stable server-side handle for name. The
// cache and retention registration live in the node.
func (s *Server) getOrCreateLogQueue(name string) (*logqueue.LogQueue, error) {
	return s.node.LogQueue(name)
}

// getOrCreateTaskQueue returns the stable server-side task handle for name.
func (s *Server) getOrCreateTaskQueue(name string) (*taskqueue.TaskQueue, error) {
	return s.node.TaskQueue(name)
}

// ListenAndServe binds to target (e.g. "tcp://:7420", "unix:///tmp/mikyu.sock") and serves incoming requests.
func (s *Server) ListenAndServe(target string) error {
	network, addr, err := protocol.ParseTarget(target)
	if err != nil {
		return err
	}

	var ln net.Listener
	if network == "unix" || network == "pipe" {
		ln, err = ipc.Listen(target)
	} else {
		ln, err = net.Listen(network, addr)
	}
	if err != nil {
		return fmt.Errorf("failed to listen on %s (%s): %w", network, addr, err)
	}

	if network == "tcp" && s.tlsConfig != nil {
		ln = tls.NewListener(ln, s.tlsConfig)
	}

	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	defer ln.Close()
	return s.Serve(ln)
}

// Serve accepts client connections on ln until closed.
func (s *Server) Serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return nil // Graceful server stop
			default:
				return err
			}
		}
		go s.serveConn(conn)
	}
}

// Done returns a channel that is closed when the server shuts down.
func (s *Server) Done() <-chan struct{} {
	return s.doneCh
}

// Stop closes the listener and wakes any Serve loop.
func (s *Server) Stop() {
	s.mu.Lock()
	select {
	case <-s.stopCh:
		s.mu.Unlock()
		return
	default:
		close(s.stopCh)
		if s.listener != nil {
			_ = s.listener.Close()
		}
		close(s.doneCh)
	}
	s.mu.Unlock()
}

func (s *Server) serveConn(conn net.Conn) {
	s.metrics.connections.Add(1)
	defer s.metrics.connections.Add(-1)

	ctx, cancel := context.WithCancel(context.Background())

	r := bufio.NewReader(conn)
	reqCh := make(chan request) // unbuffered: one frame of lookahead, natural backpressure

	var readerWG sync.WaitGroup
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		defer close(reqCh)
		defer cancel() // a dead reader must wake a handler parked on a long-poll
		var buf []byte
		for {
			op, body, next, err := protocol.ReadFrameInto(r, buf)
			buf = next
			if err != nil {
				return
			}
			select {
			case reqCh <- request{op: op, body: body}:
			case <-ctx.Done():
				return
			}
		}
	}()

	for req := range reqCh {
		respOp, respBody, shouldShutdown := s.dispatch(ctx, req.op, req.body)
		if err := protocol.WriteFrame(conn, respOp, respBody); err != nil {
			break
		}
		if shouldShutdown {
			go s.Stop()
			break
		}
	}

	cancel()
	_ = conn.Close()
	readerWG.Wait()
}

type request struct {
	op   byte
	body []byte
}
