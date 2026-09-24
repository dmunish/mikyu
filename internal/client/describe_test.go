package client

import (
	"encoding/json"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/manager"
	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/server"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newLoopbackServer starts a server on a node with empty session defaults and
// returns a remote client for it.
func newLoopbackServer(t *testing.T) *remoteClient {
	t.Helper()
	n, release, err := node.Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = release() })

	srv := server.New(n)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)

	c, err := NewRemote("tcp://" + ln.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestDescribeAfterCreateWithFlags: the embedded and remote clients store the
// same sparse spec, so DescribeQueue produces identical JSON.
func TestDescribeAfterCreateWithFlags(t *testing.T) {
	ctx := t.Context()
	opts := []QueueOption{WithMaxSegment(128), WithMaxPayload(64)}

	emb, err := NewEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emb.Close() })
	mustCreateLog(t, emb, "q", opts...)
	embDesc, err := emb.DescribeQueue(ctx, "q")
	require.NoError(t, err)
	embJSON, err := json.Marshal(embDesc)
	require.NoError(t, err)

	rem := newLoopbackServer(t)
	mustCreateLog(t, rem, "q", opts...)
	remDesc, err := rem.DescribeQueue(ctx, "q")
	require.NoError(t, err)
	remJSON, err := json.Marshal(remDesc)
	require.NoError(t, err)

	require.JSONEq(t, string(embJSON), string(remJSON))
}

// TestRemoteMetricsRoundTrip: a queue with traffic shows up in the remote
// snapshot with its counters and gauges populated.
func TestRemoteMetricsRoundTrip(t *testing.T) {
	ctx := t.Context()
	c := newLoopbackServer(t)
	mustCreateLog(t, c, "q")

	lq, err := c.LogQueue("q")
	require.NoError(t, err)
	_, err = lq.Append(ctx, []byte("hello"))
	require.NoError(t, err)

	snap, err := c.Metrics(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, snap.Totals.Messages, uint64(1))
	qv, ok := snap.Queue("q")
	require.True(t, ok, "queue view missing from snapshot")
	assert.Positive(t, qv.Counters["appends"])
	assert.Equal(t, int64(1), qv.Gauges["messages"])
	assert.Zero(t, qv.Gauges["min_offset"])
}

// TestEmbeddedMetricsMatchesRemote: embedded and remote snapshots share the
// same JSON shape; timestamps and machine-local values differ and are stripped.
func TestEmbeddedMetricsMatchesRemote(t *testing.T) {
	ctx := t.Context()

	emb, err := NewEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emb.Close() })
	mustCreateLog(t, emb, "q")
	mustCreateTask(t, emb, "tq")
	embLq, err := emb.LogQueue("q")
	require.NoError(t, err)
	_, wrongKindErr := emb.LogQueue("tq")
	require.ErrorIs(t, wrongKindErr, manager.ErrWrongKind, "embedded LogQueue on a task queue returns ErrWrongKind")
	_, err = embLq.Append(ctx, []byte("a"))
	require.NoError(t, err)
	embSnap, err := emb.Metrics(ctx)
	require.NoError(t, err)

	rem := newLoopbackServer(t)
	mustCreateLog(t, rem, "q")
	mustCreateTask(t, rem, "tq")
	remLq, err := rem.LogQueue("q")
	require.NoError(t, err)
	_, err = remLq.Append(ctx, []byte("a"))
	require.NoError(t, err)
	remSnap, err := rem.Metrics(ctx)
	require.NoError(t, err)

	embSnap.Timestamp, remSnap.Timestamp = time.Time{}, time.Time{}
	assert.Equal(t, jsonKeys(t, embSnap), jsonKeys(t, remSnap), "top-level snapshot shape differs")
	embQ, embOK := embSnap.Queue("q")
	remQ, remOK := remSnap.Queue("q")
	require.True(t, embOK)
	require.True(t, remOK)
	assert.Equal(t, jsonKeys(t, embQ), jsonKeys(t, remQ), "queue view shape differs")

	tqEmb, ok := embSnap.Queue("tq")
	require.True(t, ok, "metrics must include task queue tq")
	assert.Equal(t, "task", tqEmb.Kind, "embedded metrics kind for task")
	tqRem, ok := remSnap.Queue("tq")
	require.True(t, ok, "remote metrics must include task queue tq")
	assert.Equal(t, "task", tqRem.Kind, "remote metrics kind for task")
}

// TestRemoteMinOffsetUsesMetrics: MinOffset reads the snapshot's min_offset
// gauge, and a queue absent from the snapshot surfaces as ErrQueueNotFound.
func TestRemoteMinOffsetUsesMetrics(t *testing.T) {
	ctx := t.Context()
	c := newLoopbackServer(t)
	mustCreateLog(t, c, "q")

	lq, err := c.LogQueue("q")
	require.NoError(t, err)
	minOffset, err := lq.MinOffset(ctx)
	require.NoError(t, err)
	assert.Zero(t, minOffset)

	missing, err := c.LogQueue("no-such-queue")
	require.NoError(t, err)
	_, err = missing.MinOffset(ctx)
	require.ErrorIs(t, err, engine.ErrQueueNotFound)
}

// TestOpMinOffsetRoundTrip: remote MinOffset (via OpMinOffset) reports the same
// value as embedded MinOffset after the same operations.
func TestOpMinOffsetRoundTrip(t *testing.T) {
	ctx := t.Context()
	opts := []QueueOption{WithMaxSegment(128), WithMaxPayload(64)}

	c := newLoopbackServer(t)
	mustCreateLog(t, c, "q", opts...)
	lq, err := c.LogQueue("q")
	require.NoError(t, err)
	for range 8 {
		_, err := lq.Append(ctx, []byte("payload"))
		require.NoError(t, err)
	}
	_, err = lq.Truncate(ctx, 4)
	require.NoError(t, err)
	remoteMin, err := lq.MinOffset(ctx)
	require.NoError(t, err)
	assert.Greater(t, remoteMin, uint64(0))
	assert.LessOrEqual(t, remoteMin, uint64(4))

	emb, err := NewEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emb.Close() })
	mustCreateLog(t, emb, "q", opts...)
	elq, err := emb.LogQueue("q")
	require.NoError(t, err)
	for range 8 {
		_, err := elq.Append(ctx, []byte("payload"))
		require.NoError(t, err)
	}
	_, err = elq.Truncate(ctx, 4)
	require.NoError(t, err)
	embeddedMin, err := elq.MinOffset(ctx)
	require.NoError(t, err)

	assert.Equal(t, embeddedMin, remoteMin)
}

// TestDescribeNoStats: describe no longer carries per-queue storage stats.
func TestDescribeNoStats(t *testing.T) {
	ctx := t.Context()
	emb, err := NewEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emb.Close() })
	mustCreateLog(t, emb, "q")

	d, err := emb.DescribeQueue(ctx, "q")
	require.NoError(t, err)
	data, err := json.Marshal(d)
	require.NoError(t, err)
	assert.NotContains(t, string(data), `"stats"`)
}

// TestListCarriesKind: embedded and remote ListQueues both report each
// queue's kind, and the two surfaces agree.
func TestListCarriesKind(t *testing.T) {
	ctx := t.Context()
	emb, err := NewEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emb.Close() })
	mustCreateLog(t, emb, "events")
	mustCreateTask(t, emb, "events")
	embInfos, err := emb.ListQueues(ctx)
	require.NoError(t, err)

	rem := newLoopbackServer(t)
	mustCreateLog(t, rem, "events")
	mustCreateTask(t, rem, "events")
	remInfos, err := rem.ListQueues(ctx)
	require.NoError(t, err)

	require.Equal(t, embInfos, remInfos)
	require.Len(t, embInfos, 2)
	assert.Equal(t, QueueInfo{Name: "events", Kind: node.KindLog}, embInfos[0])
	assert.Equal(t, QueueInfo{Name: "events", Kind: node.KindTask}, embInfos[1])
}

func jsonKeys(t *testing.T, v any) []string {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	var m map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &m))
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

