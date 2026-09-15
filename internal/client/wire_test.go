package client

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
	"github.com/dmunish/mikyu/internal/server"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newRemoteClient starts a live server over loopback and returns a remote
// client for it.
func newRemoteClient(tb testing.TB) *remoteClient {
	tb.Helper()
	n, release, err := node.Acquire(tb.TempDir(), spec.Spec{
		Engine: spec.EngineSpec{MaxMessageBytes: new(int64(1 << 20))},
	})
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = release() })

	srv := server.New(n)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = ln.Close() })
	go srv.Serve(ln)
	tb.Cleanup(srv.Stop)

	c, err := NewRemote("tcp://" + ln.Addr().String())
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = c.Close() })
	return c
}

// TestRemoteEnqueueUsesQueueDefaults: a queue created with
// default_visibility 1m leases tasks dequeued from an optionless Enqueue for
// about a minute â€” the zeros on the wire inherit the queue spec, not the
// client's pre-resolved built-ins.
func TestRemoteEnqueueUsesQueueDefaults(t *testing.T) {
	n, release, err := node.Acquire(t.TempDir(), spec.Spec{
		Engine: spec.EngineSpec{MaxMessageBytes: new(int64(1 << 20))},
	})
	require.NoError(t, err)
	defer func() { _ = release() }()

	oneMinute := spec.Duration(time.Minute)
	err = n.CreateQueue(t.Context(), "tasks", spec.Spec{
		Engine: spec.EngineSpec{MaxMessageBytes: new(int64(1 << 20))},
		Task:   spec.TaskSpec{DefaultVisibility: &oneMinute},
	})
	require.NoError(t, err)

	srv := server.New(n)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go srv.Serve(ln)
	defer srv.Stop()

	c, err := NewRemote("tcp://" + ln.Addr().String())
	require.NoError(t, err)
	defer c.Close()
	tq, err := c.TaskQueue("tasks")
	require.NoError(t, err)

	_, err = tq.Enqueue(t.Context(), []byte("payload")) // no options
	require.NoError(t, err)
	leased, err := tq.Dequeue(t.Context())
	require.NoError(t, err)

	left := time.Until(leased.Visibility)
	assert.GreaterOrEqual(t, left, 55*time.Second, "lease deadline should be ~1m out, got %v remaining", left)
	assert.LessOrEqual(t, left, time.Minute+5*time.Second)
}

// TestUpdateQueueRejectsImmutableField: a remote update patch touching a
// create-only field comes back as spec.ErrImmutableField naming the field.
func TestUpdateQueueRejectsImmutableField(t *testing.T) {
	c := newRemoteClient(t)
	require.NoError(t, c.CreateQueue(t.Context(), "q"))

	err := c.UpdateQueue(t.Context(), "q", WithSegmentSize(128))
	require.Error(t, err)
	assert.ErrorIs(t, err, spec.ErrImmutableField)
	assert.Contains(t, err.Error(), "segment_size")
}

// TestDescribeQueueRoundTrip: the JSON DescribeResult decodes into the
// client's own QueueDescription with the built-in layer resolved.
func TestDescribeQueueRoundTrip(t *testing.T) {
	c := newRemoteClient(t)
	require.NoError(t, c.CreateQueue(t.Context(), "q"))

	d, err := c.DescribeQueue(t.Context(), "q")
	require.NoError(t, err)
	require.NotNil(t, d.Effective.Engine.SegmentSize)
	assert.Equal(t, int64(64<<20), *d.Effective.Engine.SegmentSize)
}

// TestRemoteSchemaMatchesLocal: Schema over OpSchema returns exactly the
// server's parameter table, keyed by canonical name.
func TestRemoteSchemaMatchesLocal(t *testing.T) {
	c := newRemoteClient(t)
	params, err := c.Schema(t.Context())
	require.NoError(t, err)
	require.Len(t, params, len(spec.Table))

	seen := make(map[string]bool, len(params))
	for _, p := range params {
		seen[p.Key] = true
	}
	for _, p := range spec.Table {
		assert.True(t, seen[p.Key], "wire schema is missing %s", p.Key)
	}
}

// TestEmbeddedRejectsTarget: a target is a remote-client setting.
func TestEmbeddedRejectsTarget(t *testing.T) {
	_, err := NewEmbedded(t.TempDir(), WithTarget("tcp://127.0.0.1:1"))
	require.ErrorIs(t, err, ErrModeMismatch)
}

// TestRemoteRejectsDataDir: a data directory is an embedded-client setting.
func TestRemoteRejectsDataDir(t *testing.T) {
	_, err := NewRemote("tcp://127.0.0.1:1", WithDataDir("./x"))
	require.ErrorIs(t, err, ErrModeMismatch)
}

// TestEmbeddedMetricsHistoryHonored: the session's history depth caps the embedded collector's ring.
func TestEmbeddedMetricsHistoryHonored(t *testing.T) {
	c, err := NewEmbedded(t.TempDir(),
		WithMetricsEnabled(true),
		WithMetricsInterval(20*time.Millisecond),
		WithMetricsHistory(4),
	)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	require.Eventually(t, func() bool { return len(c.node.Collector().History()) >= 4 }, 2*time.Second, 10*time.Millisecond)
	assert.LessOrEqual(t, len(c.node.Collector().History()), 4, "history grew past its configured depth")
}

func TestDecodeProtocolErrorNewCodes(t *testing.T) {
	cases := map[byte]error{
		protocol.ErrCodeInvalidConfig:      spec.ErrInvalidConfig,
		protocol.ErrCodeImmutableField:     spec.ErrImmutableField,
		protocol.ErrCodeSpecTooNew:         spec.ErrSpecTooNew,
		protocol.ErrCodeSpecTooLarge:       spec.ErrSpecTooLarge,
		protocol.ErrCodeMessageTooLarge:    engine.ErrMessageTooLarge,
		protocol.ErrCodeQueueClosed:        taskqueue.ErrTaskClosed,
		protocol.ErrCodeCheckpointCorrupt:  taskqueue.ErrCheckpointCorrupt,
		protocol.ErrCodeTooManyConnections: protocol.ErrTooManyConnections,
	}
	for code, want := range cases {
		assert.True(t, errors.Is(decodeProtocolError(code, "boom"), want), "code 0x%02X", code)
	}

	// Unrecognized codes wrap in ErrProtocol with the server's message.
	err := decodeProtocolError(0x7F, "weird")
	assert.True(t, errors.Is(err, ErrProtocol))
	assert.Contains(t, err.Error(), "weird")
}
