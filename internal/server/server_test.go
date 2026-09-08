package server

import (
	"bufio"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/logqueue"
	"github.com/dmunish/mikyu/internal/protocol"
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
	mgr, err := engine.OpenManager(dir, engine.Config{})
	require.NoError(t, err)
	defer mgr.Close()

	_, err = mgr.CreateQueue("cached")
	require.NoError(t, err)

	srv := New(mgr)
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
		q, ok := value.(*logqueue.LogQueue)
		require.True(t, ok, "unexpected cache result type")
		assert.Same(t, first, q)
	}
}

func TestServerListenAndServe(t *testing.T) {
	testWithTimeout(t, 2*time.Second)
	dir := t.TempDir()
	mgr, err := engine.OpenManager(dir, engine.Config{})
	require.NoError(t, err)
	defer mgr.Close()

	srv := New(mgr)
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
	// but do NOT send OpShutdown, which would trigger async mgr.Close().
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
	mgr, _ := engine.OpenManager(dir, engine.Config{})
	defer mgr.Close()
	srv := New(mgr)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	srv.listener = ln
	go srv.Serve(ln)

	time.Sleep(20 * time.Millisecond)
	srv.Stop() // closes listener
	_, err := ln.Accept()
	assert.Error(t, err) // should fail because listener closed
}

func TestServerTLSHandshake(t *testing.T) {
	testWithTimeout(t, 2*time.Second)
	dir := t.TempDir()
	mgr, err := engine.OpenManager(dir, engine.Config{})
	require.NoError(t, err)
	defer mgr.Close()

	tlsCfg, err := DefaultTLSConfig("", "")
	require.NoError(t, err)

	srv := New(mgr, WithTLS(tlsCfg))
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
