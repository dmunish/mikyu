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
	"github.com/dmunish/mikyu/internal/logqueue"
	"github.com/dmunish/mikyu/internal/protocol"
	"github.com/dmunish/mikyu/internal/retention"
	"github.com/dmunish/mikyu/internal/taskqueue"
)

// Server accepts client connections and dispatches wire protocol frames to internal/engine.
type Server struct {
	mgr       *engine.Manager
	listener  net.Listener
	tlsConfig *tls.Config
	queues    map[string]*logqueue.LogQueue
	tasks     map[string]*taskqueue.TaskQueue
	mu        sync.RWMutex
	stopCh    chan struct{}
	doneCh    chan struct{}

	retention       *retention.Coordinator
	retentionCancel context.CancelFunc
	retentionWG     sync.WaitGroup
}

type ServerOption func(*Server)

// New creates a new Server instance backed by the provided Manager.
func New(mgr *engine.Manager, opts ...ServerOption) *Server {
	s := &Server{
		mgr:       mgr,
		queues:    make(map[string]*logqueue.LogQueue),
		tasks:     make(map[string]*taskqueue.TaskQueue),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
		retention: retention.NewCoordinator(0),
	}
	for _, opt := range opts {
		opt(s)
	}
	retentionCtx, cancel := context.WithCancel(context.Background())
	s.retentionCancel = cancel
	s.retentionWG.Go(func() {
		s.retention.Start(retentionCtx)
	})
	return s
}

func WithTLS(tlsConfig *tls.Config) ServerOption {
	return func(s *Server) {
		s.tlsConfig = tlsConfig
	}
}

// getOrCreateLogQueue returns the stable server-side handle for name.
// The fast path only takes a read lock; construction is serialized and uses
// double-checking so concurrent requests cannot create duplicate handles.
func (s *Server) getOrCreateLogQueue(name string) (*logqueue.LogQueue, error) {
	s.mu.RLock()
	q, ok := s.queues[name]
	s.mu.RUnlock()
	if ok {
		return q, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if q, ok := s.queues[name]; ok {
		return q, nil
	}

	storage, err := s.mgr.GetQueue(name)
	if err != nil {
		return nil, err
	}

	q = logqueue.NewLogQueue(name, storage)
	s.queues[name] = q
	s.retention.RegisterDriver(name, storage, q)
	return q, nil
}

// getOrCreateTaskQueue returns the stable server-side task handle for name.
func (s *Server) getOrCreateTaskQueue(name string) (*taskqueue.TaskQueue, error) {
	s.mu.RLock()
	q, ok := s.tasks[name]
	s.mu.RUnlock()
	if ok {
		return q, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if q, ok := s.tasks[name]; ok {
		return q, nil
	}

	storage, err := s.mgr.GetQueue(name)
	if err != nil {
		return nil, err
	}
	q, err = taskqueue.NewTaskQueue(name, storage)
	if err != nil {
		return nil, err
	}
	s.tasks[name] = q
	s.retention.RegisterDriver(name, storage, q)
	return q, nil
}

func (s *Server) closeTaskQueues() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, q := range s.tasks {
		_ = q.Close()
		delete(s.tasks, name)
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

// Done returns a channel that is closed when the server shuts down.
func (s *Server) Done() <-chan struct{} {
	return s.doneCh
}

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
	if s.retentionCancel != nil {
		s.retentionCancel()
	}
	s.retentionWG.Wait()
	s.closeTaskQueues()
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
				s.Stop()
				_ = s.mgr.Close()
			}()
			return
		}
	}
}
