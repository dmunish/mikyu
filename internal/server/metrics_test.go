package server

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerOpCounters(t *testing.T) {
	testWithTimeout(t, 2*time.Second)
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

	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	r := bufio.NewReader(conn)
	for range 5 {
		require.NoError(t, protocol.WriteFrame(conn, protocol.OpListQueues, nil))
		_, _, err := protocol.ReadFrame(r)
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		return srv.metrics.ops[protocol.OpListQueues].Load() == 5
	}, time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		return srv.metrics.connections.Load() >= 1
	}, time.Second, time.Millisecond)
	assert.EqualValues(t, 5, srv.metrics.durations[protocol.OpListQueues].Snapshot().Total)
}
