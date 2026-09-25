package client

import (
	"context"
	"encoding/json"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/manager"
	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
	"github.com/dmunish/mikyu/internal/server"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// opScriptResults captures the observable result of every user-facing opcode,
// so two surfaces that ran the same script can be compared field by field.
type opScriptResults struct {
	read0Payload     []byte
	readBatchN       int
	lastOffset       uint64
	minOffset        uint64
	truncated        int64
	committed        uint64
	streamCount      int
	taskAttempts     uint32
	deadLetters      int
	listNames        []string
	listKinds        []string
	descLogJSON      string
	descTaskJSON     string
	descUpdatedJSON  string
	schemaJSON       string
	metricsNames     []string
	metricsKinds     map[string]string
	metricsKeys      []string
	metricsQueueKeys map[string][]string
}

// runOpScript drives every user-facing opcode once against c: OpCreate for
// both kinds, the full log op set, the full task op set including archive and
// dead letters, then OpList, OpDescribe, OpUpdate, OpSchema, and OpMetrics.
func runOpScript(t *testing.T, c Client) opScriptResults {
	t.Helper()
	ctx := t.Context()
	var r opScriptResults
	r.metricsKinds = map[string]string{}

	mustCreateLog(t, c, "events")
	mustCreateTask(t, c, "jobs")
	ev, err := c.LogQueue("events")
	require.NoError(t, err)
	tq, err := c.TaskQueue("jobs")
	require.NoError(t, err)

	offs, err := ev.AppendBatch(ctx, [][]byte{[]byte("one"), []byte("two"), []byte("three")})
	require.NoError(t, err)
	require.Len(t, offs, 3)
	_, err = ev.Append(ctx, []byte("keyed"), logqueue.WithKey([]byte("user:1")))
	require.NoError(t, err)

	rec0, err := ev.Read(ctx, 0)
	require.NoError(t, err)
	r.read0Payload = append([]byte(nil), rec0.Payload...)
	batch, err := ev.ReadBatch(ctx, 1, 2)
	require.NoError(t, err)
	r.readBatchN = len(batch)

	r.lastOffset, err = ev.LastOffset(ctx)
	require.NoError(t, err)
	r.minOffset, err = ev.MinOffset(ctx)
	require.NoError(t, err)

	require.NoError(t, ev.CommitOffset(ctx, "parity", 2))
	r.committed, err = ev.Committed(ctx, "parity")
	require.NoError(t, err)

	_, err = ev.Fetch(ctx, r.lastOffset, 10, 0)
	require.ErrorIs(t, err, engine.ErrNoData)

	r.truncated, err = c.Truncate(ctx, "events", 2)
	require.NoError(t, err)

	for _, err := range ev.Stream(ctx, r.minOffset) {
		if err != nil {
			break
		}
		r.streamCount++
	}

	_, err = tq.Enqueue(ctx, []byte("work"))
	require.NoError(t, err)
	delivered, err := tq.Dequeue(ctx)
	require.NoError(t, err)
	r.taskAttempts = delivered.Attempts
	require.NoError(t, tq.Ack(ctx, delivered))

	_, err = tq.Enqueue(ctx, []byte("retry me"))
	require.NoError(t, err)
	d2, err := tq.Dequeue(ctx)
	require.NoError(t, err)
	require.NoError(t, tq.Nack(ctx, d2, taskqueue.WithRetryAfter(10*time.Millisecond)))

	_, err = tq.Enqueue(ctx, []byte("extend me"))
	require.NoError(t, err)
	d3, err := tq.Dequeue(ctx)
	require.NoError(t, err)
	require.NoError(t, tq.Extend(ctx, d3, time.Minute))

	_, err = tq.Enqueue(ctx, []byte("keep me"))
	require.NoError(t, err)
	d4, err := tq.Dequeue(ctx)
	require.NoError(t, err)
	require.NoError(t, tq.Archive(ctx, d4))

	mustCreateTask(t, c, "dlq", WithMaxAttempts(1))
	dlq, err := c.TaskQueue("dlq")
	require.NoError(t, err)
	_, err = dlq.Enqueue(ctx, []byte("doomed"))
	require.NoError(t, err)
	doomed, err := dlq.Dequeue(ctx)
	require.NoError(t, err)
	require.NoError(t, dlq.Nack(ctx, doomed))

	dl, err := dlq.DeadLetters(ctx)
	require.NoError(t, err)
	r.deadLetters = len(dl)
	require.NoError(t, dlq.AckDeadLetter(ctx, dl[0]))

	infos, err := c.ListQueues(ctx)
	require.NoError(t, err)
	for _, info := range infos {
		r.listNames = append(r.listNames, info.Name)
		r.listKinds = append(r.listKinds, info.Kind.String())
	}

	descLog, err := c.DescribeQueue(ctx, "events")
	require.NoError(t, err)
	r.descLogJSON = marshalJSON(t, descLog)
	descTask, err := c.DescribeQueue(ctx, "jobs")
	require.NoError(t, err)
	r.descTaskJSON = marshalJSON(t, descTask)

	require.NoError(t, c.UpdateQueue(ctx, "events", WithMaxFlushBytes(64<<10)))
	updated, err := c.DescribeQueue(ctx, "events")
	require.NoError(t, err)
	r.descUpdatedJSON = marshalJSON(t, updated)

	params, err := c.Schema(ctx)
	require.NoError(t, err)
	r.schemaJSON = marshalJSON(t, params)

	snap, err := c.Metrics(ctx)
	require.NoError(t, err)
	for _, qv := range snap.Queues {
		r.metricsNames = append(r.metricsNames, qv.Name)
		r.metricsKinds[qv.Name] = qv.Kind
	}
	r.metricsKeys, r.metricsQueueKeys = snapshotShape(t, snap)
	return r
}

// snapshotShape returns the sorted JSON key set of a snapshot and of each queue view.
func snapshotShape(t *testing.T, snap metrics.Snapshot) ([]string, map[string][]string) {
	t.Helper()
	data, err := json.Marshal(snap)
	require.NoError(t, err)
	var top map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &top))
	var queues []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(top["queues"], &queues))

	perQueue := make(map[string][]string, len(queues))
	for _, qv := range queues {
		var name string
		require.NoError(t, json.Unmarshal(qv["name"], &name))
		perQueue[name] = sortedKeys(t, qv)
	}
	return sortedKeys(t, top), perQueue
}

// sortedKeys returns an object's JSON keys in a stable order.
func sortedKeys(t *testing.T, obj map[string]json.RawMessage) []string {
	t.Helper()
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func marshalJSON(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	return string(data)
}

// TestParityEveryOp runs the full opcode script against embedded and remote
// clients and asserts the observable results agree op for op.
func TestParityEveryOp(t *testing.T) {
	emb, err := NewEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emb.Close() })

	rem := newLoopbackServer(t)

	embRes := runOpScript(t, emb)
	remRes := runOpScript(t, rem)

	assert.Equal(t, embRes.read0Payload, remRes.read0Payload, "OpRead")
	assert.Equal(t, embRes.readBatchN, remRes.readBatchN, "OpReadBatch")
	assert.Equal(t, embRes.lastOffset, remRes.lastOffset, "OpLastOffset")
	assert.Equal(t, embRes.minOffset, remRes.minOffset, "OpMinOffset")
	assert.Equal(t, embRes.committed, remRes.committed, "OpCommitted")
	assert.Equal(t, embRes.truncated, remRes.truncated, "OpTruncate")
	assert.Equal(t, embRes.streamCount, remRes.streamCount, "OpFetch via Stream")
	assert.Equal(t, embRes.taskAttempts, remRes.taskAttempts, "OpDequeue")
	assert.Equal(t, embRes.deadLetters, remRes.deadLetters, "OpDeadLetters")
	assert.Equal(t, embRes.listNames, remRes.listNames, "OpList names")
	assert.Equal(t, embRes.listKinds, remRes.listKinds, "OpList kinds")
	assert.JSONEq(t, embRes.descLogJSON, remRes.descLogJSON, "OpDescribe log")
	assert.JSONEq(t, embRes.descTaskJSON, remRes.descTaskJSON, "OpDescribe task")
	assert.JSONEq(t, embRes.descUpdatedJSON, remRes.descUpdatedJSON, "OpUpdate via Describe")
	assert.JSONEq(t, embRes.schemaJSON, remRes.schemaJSON, "OpSchema")
	assert.Equal(t, embRes.metricsNames, remRes.metricsNames, "OpMetrics queue names")
	assert.Equal(t, embRes.metricsKinds, remRes.metricsKinds, "OpMetrics queue kinds")
	assert.Equal(t, embRes.metricsKeys, remRes.metricsKeys, "OpMetrics snapshot keys")
	assert.Equal(t, embRes.metricsQueueKeys, remRes.metricsQueueKeys, "OpMetrics per-queue keys")
}

// TestParityMixedKinds: a directory holding both kinds reports the right kind
// per queue in List, Describe, and Metrics on both surfaces.
func TestParityMixedKinds(t *testing.T) {
	ctx := t.Context()

	emb, err := NewEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emb.Close() })
	mustCreateLog(t, emb, "events")
	mustCreateTask(t, emb, "jobs")

	rem := newLoopbackServer(t)
	mustCreateLog(t, rem, "events")
	mustCreateTask(t, rem, "jobs")

	for name, c := range map[string]Client{"embedded": emb, "remote": rem} {
		infos, err := c.ListQueues(ctx)
		require.NoError(t, err)
		kinds := map[string]string{}
		for _, info := range infos {
			kinds[info.Name] = info.Kind.String()
		}
		assert.Equal(t, "log", kinds["events"], name+" List")
		assert.Equal(t, "task", kinds["jobs"], name+" List")

		d, err := c.DescribeQueue(ctx, "events")
		require.NoError(t, err)
		assert.Equal(t, "log", d.Kind.String(), name+" Describe events")
		d, err = c.DescribeQueue(ctx, "jobs")
		require.NoError(t, err)
		assert.Equal(t, "task", d.Kind.String(), name+" Describe jobs")

		snap, err := c.Metrics(ctx)
		require.NoError(t, err)
		ev, ok := snap.Queue("events")
		require.True(t, ok, name+" Metrics events")
		assert.Equal(t, "log", ev.Kind, name+" Metrics events")
		assert.NotContains(t, ev.Gauges, "active", name+" task gauges leak into a log queue")
		jb, ok := snap.Queue("jobs")
		require.True(t, ok, name+" Metrics jobs")
		assert.Equal(t, "task", jb.Kind, name+" Metrics jobs")
		assert.Contains(t, jb.Gauges, "parked_dequeue", name+" task queue keeps its own gauges")
		assert.NotContains(t, jb.Gauges, "active", name+" task occupancy is a Tasks field, not a Gauges key")
	}
}

// TestParityEveryError forces the wire server to raise each sentinel the public
// API can trigger and asserts the client decodes it back to the same sentinel
// with the server detail preserved. Sentinels that need fault injection
// (corrupt records, disk full, closed queues) or a newer client (spec too new)
// are covered by the decode-mapping tests instead.
func TestParityEveryError(t *testing.T) {
	ctx := t.Context()
	cases := []struct {
		name  string
		force func(t *testing.T, c Client) error
		want  error
	}{
		{"queue not found", func(t *testing.T, c Client) error {
			lq, err := c.LogQueue("ghost")
			require.NoError(t, err)
			_, err = lq.Read(ctx, 0)
			return err
		}, engine.ErrQueueNotFound},
		{"queue exists", func(t *testing.T, c Client) error {
			mustCreateLog(t, c, "dup")
			_, err := c.CreateLogQueue(ctx, "dup")
			return err
		}, engine.ErrQueueExists},
		{"wrong kind", func(t *testing.T, c Client) error {
			mustCreateTask(t, c, "jobs")
			lq, err := c.LogQueue("jobs")
			require.NoError(t, err)
			_, err = lq.Read(ctx, 0)
			return err
		}, manager.ErrWrongKind},
		{"offset not found", func(t *testing.T, c Client) error {
			mustCreateLog(t, c, "q")
			lq, err := c.LogQueue("q")
			require.NoError(t, err)
			_, err = lq.Read(ctx, 999)
			return err
		}, engine.ErrOffsetNotFound},
		{"offset truncated", func(t *testing.T, c Client) error {
			mustCreateLog(t, c, "q", WithMaxSegment(200), WithMaxPayload(128))
			lq, err := c.LogQueue("q")
			require.NoError(t, err)
			for range 8 {
				_, err = lq.Append(ctx, []byte("payload"))
				require.NoError(t, err)
			}
			_, err = lq.Truncate(ctx, 4)
			require.NoError(t, err)
			_, err = lq.Read(ctx, 0)
			return err
		}, engine.ErrOffsetTruncated},
		{"no data", func(t *testing.T, c Client) error {
			mustCreateLog(t, c, "q")
			lq, err := c.LogQueue("q")
			require.NoError(t, err)
			_, err = lq.Fetch(ctx, 0, 10, 0)
			return err
		}, engine.ErrNoData},
		{"task not found", func(t *testing.T, c Client) error {
			mustCreateTask(t, c, "jobs")
			tq, err := c.TaskQueue("jobs")
			require.NoError(t, err)
			return tq.Ack(ctx, taskqueue.ID("no-such-task"))
		}, taskqueue.ErrTaskNotFound},
		{"task not inflight", func(t *testing.T, c Client) error {
			mustCreateTask(t, c, "jobs")
			tq, err := c.TaskQueue("jobs")
			require.NoError(t, err)
			task, err := tq.Enqueue(ctx, []byte("pending"))
			require.NoError(t, err)
			return tq.Ack(ctx, task)
		}, taskqueue.ErrTaskNotInflight},
		{"task stale", func(t *testing.T, c Client) error {
			mustCreateTask(t, c, "jobs")
			tq, err := c.TaskQueue("jobs")
			require.NoError(t, err)
			_, err = tq.Enqueue(ctx, []byte("leased"))
			require.NoError(t, err)
			delivered, err := tq.Dequeue(ctx)
			require.NoError(t, err)
			delivered.DeliveryID = "stale-delivery"
			return tq.Ack(ctx, delivered)
		}, taskqueue.ErrTaskStale},
		{"task not available", func(t *testing.T, c Client) error {
			mustCreateTask(t, c, "jobs")
			tq, err := c.TaskQueue("jobs")
			require.NoError(t, err)
			_, err = tq.TryDequeue(ctx)
			return err
		}, taskqueue.ErrTaskNotAvailable},
		{"queue full", func(t *testing.T, c Client) error {
			mustCreateTask(t, c, "full", WithMaxInFlight(1))
			tq, err := c.TaskQueue("full")
			require.NoError(t, err)
			_, err = tq.Enqueue(ctx, []byte("first"))
			require.NoError(t, err)
			_, err = tq.Enqueue(ctx, []byte("second"))
			return err
		}, taskqueue.ErrQueueFull},
		{"dead letter not found", func(t *testing.T, c Client) error {
			mustCreateTask(t, c, "jobs")
			tq, err := c.TaskQueue("jobs")
			require.NoError(t, err)
			return tq.AckDeadLetter(ctx, taskqueue.ID("no-such-task"))
		}, taskqueue.ErrDeadLetterNotFound},
		{"dead letter stale", func(t *testing.T, c Client) error {
			mustCreateTask(t, c, "doomed", WithMaxAttempts(1))
			tq, err := c.TaskQueue("doomed")
			require.NoError(t, err)
			_, err = tq.Enqueue(ctx, []byte("doomed"))
			require.NoError(t, err)
			delivered, err := tq.Dequeue(ctx)
			require.NoError(t, err)
			require.NoError(t, tq.Nack(ctx, delivered))
			stale := delivered
			stale.Offset = 999999
			return tq.AckDeadLetter(ctx, stale)
		}, taskqueue.ErrDeadLetterStale},
		{"immutable field", func(t *testing.T, c Client) error {
			mustCreateLog(t, c, "q")
			return c.UpdateQueue(ctx, "q", WithMaxSegment(128))
		}, spec.ErrImmutableField},
		{"invalid config", func(t *testing.T, c Client) error {
			_, err := c.CreateTaskQueue(ctx, "bad", WithMaxFlushBytes(0))
			return err
		}, spec.ErrInvalidConfig},
		{"record compacted", func(t *testing.T, c Client) error {
			mustCreateLog(t, c, "cc", WithCompaction(spec.CompactionKeys), WithMaxSegment(256))
			lq, err := c.LogQueue("cc")
			require.NoError(t, err)
			var offs []uint64
			for range 3 {
				off, err := lq.Append(ctx, make([]byte, 100), logqueue.WithKey([]byte("user-42")))
				require.NoError(t, err)
				offs = append(offs, off)
			}
			_, err = lq.Read(ctx, offs[0])
			return err
		}, engine.ErrRecordCompacted},
		{"record too large", func(t *testing.T, c Client) error {
			mustCreateLog(t, c, "big", WithMaxSegment(128))
			lq, err := c.LogQueue("big")
			require.NoError(t, err)
			_, err = lq.Append(ctx, make([]byte, 200))
			return err
		}, engine.ErrRecordTooLarge},
		{"message too large", func(t *testing.T, c Client) error {
			mustCreateLog(t, c, "small", WithMaxPayload(8))
			lq, err := c.LogQueue("small")
			require.NoError(t, err)
			_, err = lq.Append(ctx, make([]byte, 64))
			return err
		}, engine.ErrMessageTooLarge},
		{"unauthorized", forceUnauthorized, ErrUnauthorized},
		{"too many connections", forceTooManyConnections, protocol.ErrTooManyConnections},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := newLoopbackServer(t)
			err := tc.force(t, c)
			require.Error(t, err, "server must raise %v", tc.want)
			require.ErrorIs(t, err, tc.want)
			assert.Contains(t, err.Error(), tc.want.Error(), "server detail must survive the round trip")
		})
	}
}

// forceUnauthorized dials a token-mode server with a wrong token and expects the handshake to be rejected.
func forceUnauthorized(t *testing.T, c Client) error {
	t.Helper()
	_ = c

	n, release, err := node.Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = release() })

	mode, err := server.NewAuthMode(spec.Session{Auth: spec.AuthConfig{Method: "token", Token: "0123456789abcdef"}})
	require.NoError(t, err)
	srv := server.New(n, server.WithAuth(mode))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)

	bad, err := NewRemote("tcp://"+ln.Addr().String(), WithAuthMethod("token"), WithAuthToken("ffffffffffffffff"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = bad.Close() })
	_, err = bad.ListQueues(t.Context())
	return err
}

// forceTooManyConnections parks one long-poll on the only allowed connection and expects the next dial to be refused.
func forceTooManyConnections(t *testing.T, c Client) error {
	t.Helper()
	_ = c

	n, release, err := node.Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = release() })

	srv := server.New(n, server.WithMaxConnections(1))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)

	hook, sent := frameWatcher(protocol.OpDequeue)
	full, err := NewRemote("tcp://"+ln.Addr().String(), withFrameHook(hook))
	require.NoError(t, err)
	t.Cleanup(func() { _ = full.Close() })

	mustCreateTask(t, full, "jobs")
	tq, err := full.TaskQueue("jobs")
	require.NoError(t, err)

	pollCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	go func() { _, _ = tq.Dequeue(pollCtx) }()

	// The frame has been written; the server's handler now holds the only connection slot.
	select {
	case <-sent:
	case <-time.After(2 * time.Second):
		t.Fatal("Dequeue request never went out")
	}

	_, err = full.ListQueues(t.Context())
	return err
}
