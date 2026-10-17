package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func logsServer(t *testing.T) (*Server, *node.Node, func()) {
	t.Helper()
	n, release, err := node.Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	srv := New(n)
	return srv, n, func() {
		srv.Stop()
		_ = release()
	}
}

func readLogFrames(t *testing.T, r *bufio.Reader) [][]byte {
	t.Helper()
	var lines [][]byte
	for {
		op, body, err := protocol.ReadFrame(r)
		require.NoError(t, err)
		if op != protocol.OpLogs {
			t.Fatalf("unexpected opcode 0x%02x in the log stream", op)
		}
		if len(body) == 0 {
			return lines // empty frame: end of stream
		}
		lines = append(lines, body)
	}
}

func TestHandleLogsReplaysTheRing(t *testing.T) {
	srv, n, stop := logsServer(t)
	defer stop()

	for i := range 3 {
		fmt.Fprintf(n.LogTap(), "line-%d\n", i)
	}

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	done := make(chan struct{})
	go func() {
		srv.handleLogs(t.Context(), server, encodeSubscribe(false))
		close(done)
	}()

	lines := readLogFrames(t, bufio.NewReader(client))
	require.Len(t, lines, 3)
	for i, line := range lines {
		assert.Equal(t, fmt.Sprintf("line-%d", i), string(line))
	}
	<-done
}

func TestHandleLogsFollowsUntilDetach(t *testing.T) {
	srv, n, stop := logsServer(t)
	defer stop()

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		srv.handleLogs(ctx, server, encodeSubscribe(true))
		close(done)
	}()

	reader := bufio.NewReader(client)
	logc := make(chan string, 16)
	go func() {
		for {
			op, body, err := protocol.ReadFrame(reader)
			if err != nil {
				return
			}
			if op == protocol.OpLogs && len(body) > 0 {
				logc <- string(body)
			}
		}
	}()

	fmt.Fprintf(n.LogTap(), "live-line\n")
	select {
	case line := <-logc:
		assert.Equal(t, "live-line", line)
	case <-time.After(5 * time.Second):
		t.Fatal("the followed line never arrived")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler did not return after the client detached")
	}
}

func TestHandleLogsEndsWhenTheFollowerFallsBehind(t *testing.T) {
	srv, n, stop := logsServer(t)
	defer stop()

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	done := make(chan struct{})
	go func() {
		srv.handleLogs(t.Context(), server, encodeSubscribe(true))
		close(done)
	}()

	// Write far past the follower buffer without reading: the tap must drop
	// the follower. The dropped subscription still delivers its buffered
	// lines, then the handler ends the stream with an empty frame.
	for i := range 4096 {
		fmt.Fprintf(n.LogTap(), "flood-%d\n", i)
	}

	lines := readLogFrames(t, bufio.NewReader(client))
	for _, line := range lines {
		assert.Contains(t, string(line), "flood-")
	}
	<-done
}

func TestHandleLogsMalformedRequest(t *testing.T) {
	srv, _, stop := logsServer(t)
	defer stop()

	respOp, respBody, streamed := srv.handleLogs(t.Context(), nil, nil)
	assert.Equal(t, protocol.OpError, respOp)
	assert.False(t, streamed)
	code, msg := protocol.DecodeError(respBody)
	assert.Equal(t, protocol.ErrCodeUnknown, code)
	assert.NotEmpty(t, msg)
}

// encodeSubscribe builds an OpLogs request body.
func encodeSubscribe(follow bool) []byte {
	var b protocol.Buffer
	protocol.EncodeLogsSubscribe(&b, follow)
	return b.Bytes()
}
