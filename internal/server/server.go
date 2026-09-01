package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/ipc"
	"github.com/dmunish/mikyu/internal/protocol"
)

// Server accepts client connections and dispatches wire protocol frames to internal/engine.
type Server struct {
	mgr       *engine.Manager
	listener  net.Listener
	tlsConfig *tls.Config
	mu        sync.Mutex
	stopCh    chan struct{}
}

// New creates a new Server instance backed by the provided Manager.
func New(mgr *engine.Manager, opts ...ServerOption) *Server {
	s := &Server{
		mgr:    mgr,
		stopCh: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

type ServerOption func(*Server)

func WithTLS(tlsConfig *tls.Config) ServerOption {
	return func(s *Server) {
		s.tlsConfig = tlsConfig
	}
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

func (s *Server) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.stopCh:
	default:
		close(s.stopCh)
		if s.listener != nil {
			_ = s.listener.Close()
		}
	}
}

func (s *Server) serveConn(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	ctx := context.Background()

	for {
		op, body, err := protocol.ReadFrame(r)
		if err != nil {
			return
		}

		respOp, respBody, shouldShutdown := s.dispatch(ctx, op, body)
		if err := protocol.WriteFrame(conn, respOp, respBody); err != nil {
			return
		}

		if shouldShutdown {
			go func() {
				_ = s.mgr.Close()
				s.Stop()
			}()
			return
		}
	}
}
