package server

import (
	"bufio"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testWithTimeout ensures a test does not hang forever.
func testWithTimeout(t *testing.T, timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go func() {
		select {
		case <-done:
		case <-time.After(timeout):
			panic("test timed out")
		}
	}()
}

func TestServerQueueHandleCaching(t *testing.T) {
	dir := t.TempDir()
	n, release, err := node.Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	_, err = n.Manager().Create("cached")
	require.NoError(t, err)

	srv := New(n)
	defer srv.Stop()
	first, err := srv.getOrCreateLogQueue("cached")
	require.NoError(t, err)

	second, err := srv.getOrCreateLogQueue("cached")
	require.NoError(t, err)
	assert.Same(t, first, second)

	const goroutines = 32
	results := make(chan any, goroutines)
	for range goroutines {
		go func() {
			q, err := srv.getOrCreateLogQueue("cached")
			if err != nil {
				results <- err
				return
			}
			results <- q
		}()
	}

	for range goroutines {
		value := <-results
		if err, ok := value.(error); ok {
			t.Fatalf("getOrCreateLogQueue failed: %v", err)
		}
		q, ok := value.(logqueue.Log)
		require.True(t, ok, "unexpected cache result type")
		assert.Same(t, first, q)
	}
}

func TestServerListenAndServe(t *testing.T) {
	testWithTimeout(t, 2*time.Second)
	dir := t.TempDir()
	n, release, err := node.Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	srv := New(n)
	// Use ListenAndServe so srv.listener is set and Stop() can close it.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv.listener = ln // manually set to allow Stop() to close it
	go srv.Serve(ln)
	defer srv.Stop()

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	// Send a harmless op (e.g., OpListQueues) to verify the server responds,
	// but do NOT send OpShutdown, which would trigger an async node.Close().
	err = protocol.WriteFrame(conn, protocol.OpListQueues, nil)
	require.NoError(t, err)

	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	respOp, _, err := protocol.ReadFrame(bufio.NewReader(conn))
	require.NoError(t, err)
	assert.Equal(t, protocol.OpListQueues, respOp)
}

func TestServerServeClosesOnStop(t *testing.T) {
	testWithTimeout(t, 2*time.Second)
	dir := t.TempDir()
	n, release, _ := node.Acquire(dir, spec.Spec{})
	defer func() { _ = release() }()
	srv := New(n)
	defer srv.Stop()

	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	srv.listener = ln
	go srv.Serve(ln)

	time.Sleep(20 * time.Millisecond)
	srv.Stop() // closes listener
	_, err := ln.Accept()
	assert.Error(t, err) // should fail because listener closed
}

// TestServerMaxConnsRejectsExtra pins the admission cap: over-limit connections
// get a connection-limit error, and a freed slot admits the next client.
func TestServerMaxConnsRejectsExtra(t *testing.T) {
	testWithTimeout(t, 10*time.Second)
	dir := t.TempDir()
	n, release, err := node.Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	srv := New(n, WithMaxConns(1))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv.listener = ln
	go srv.Serve(ln)
	defer srv.Stop()

	// First connection occupies the only slot.
	c1, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	require.NoError(t, protocol.WriteFrame(c1, protocol.OpListQueues, nil))
	_ = c1.SetReadDeadline(time.Now().Add(2 * time.Second))
	op, _, err := protocol.ReadFrame(bufio.NewReader(c1))
	require.NoError(t, err)
	require.Equal(t, protocol.OpListQueues, op)

	// The second is refused with a clear error and then closed.
	c2, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	require.NoError(t, protocol.WriteFrame(c2, protocol.OpListQueues, nil))
	_ = c2.SetReadDeadline(time.Now().Add(2 * time.Second))
	op, body, err := protocol.ReadFrame(bufio.NewReader(c2))
	require.NoError(t, err)
	require.Equal(t, protocol.OpError, op)
	code, msg := protocol.DecodeError(body)
	assert.Equal(t, protocol.ErrCodeTooManyConnections, code)
	assert.Contains(t, msg, protocol.ErrTooManyConnections.Error())
	assert.Equal(t, int64(1), srv.metrics.rejected.Load())

	// Freeing the slot admits a new client.
	require.NoError(t, c2.Close())
	require.NoError(t, c1.Close())
	require.Eventually(t, func() bool {
		c3, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			return false
		}
		defer c3.Close()
		if err := protocol.WriteFrame(c3, protocol.OpListQueues, nil); err != nil {
			return false
		}
		_ = c3.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
		op, _, err := protocol.ReadFrame(bufio.NewReader(c3))
		return err == nil && op == protocol.OpListQueues
	}, 3*time.Second, 25*time.Millisecond)
}

// TestServerMaxConnsUnlimitedByDefault keeps the zero-value server open to any number of clients.
func TestServerMaxConnsUnlimitedByDefault(t *testing.T) {
	testWithTimeout(t, 10*time.Second)
	dir := t.TempDir()
	n, release, err := node.Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	srv := New(n)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv.listener = ln
	go srv.Serve(ln)
	defer srv.Stop()

	const clients = 5
	conns := make([]net.Conn, 0, clients)
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()
	for range clients {
		c, err := net.Dial("tcp", ln.Addr().String())
		require.NoError(t, err)
		conns = append(conns, c)
		require.NoError(t, protocol.WriteFrame(c, protocol.OpListQueues, nil))
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		op, _, err := protocol.ReadFrame(bufio.NewReader(c))
		require.NoError(t, err)
		require.Equal(t, protocol.OpListQueues, op)
	}
	assert.Zero(t, srv.metrics.rejected.Load())
}

func TestServerTLSHandshake(t *testing.T) {
	testWithTimeout(t, 2*time.Second)
	dir := t.TempDir()
	n, release, err := node.Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	tlsCfg, err := DefaultTLSConfig("", "", "")
	require.NoError(t, err)

	srv := New(n, WithTLS(tlsCfg))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	tlsLn := tls.NewListener(ln, tlsCfg)
	srv.listener = tlsLn
	go srv.Serve(tlsLn)
	defer srv.Stop()

	conn, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{
		InsecureSkipVerify: true,
	})
	require.NoError(t, err)
	defer conn.Close()

	err = protocol.WriteFrame(conn, protocol.OpListQueues, nil)
	require.NoError(t, err)

	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	respOp, _, err := protocol.ReadFrame(bufio.NewReader(conn))
	require.NoError(t, err)
	assert.Equal(t, protocol.OpListQueues, respOp)
}
