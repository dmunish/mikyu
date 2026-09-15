package mikyu_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestEmbeddedClientLifecycle(t *testing.T) {
	dir := t.TempDir()
	client, err := mikyu.Open(dir)
	require.NoError(t, err)
	defer client.Close()

	err = client.CreateQueue(t.Context(), "test")
	require.NoError(t, err)

	q, err := client.LogQueue("test")
	require.NoError(t, err)
	assert.Equal(t, "test", q.Name())

	off, err := q.Append(t.Context(), []byte("hello"))
	require.NoError(t, err)
	assert.Equal(t, uint64(0), off)

	msg, err := q.Read(t.Context(), off)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(msg.Payload))

	snap, err := client.Metrics(t.Context())
	require.NoError(t, err)
	_, ok := snap.Queue("test")
	assert.True(t, ok, "queue view missing from snapshot")
}

func TestEmbeddedClientDuplicateQueue(t *testing.T) {
	dir := t.TempDir()
	client, _ := mikyu.Open(dir)
	defer client.Close()

	err := client.CreateQueue(t.Context(), "dup")
	require.NoError(t, err)
	err = client.CreateQueue(t.Context(), "dup")
	assert.Error(t, err)
}

func TestEmbeddedClientDeleteQueue(t *testing.T) {
	dir := t.TempDir()
	client, _ := mikyu.Open(dir)
	defer client.Close()

	err := client.CreateQueue(t.Context(), "temp")
	require.NoError(t, err)

	err = client.DeleteQueue(t.Context(), "temp")
	require.NoError(t, err)

	_, err = client.LogQueue("temp")
	assert.Error(t, err)
}

func TestEmbeddedClientConcurrentLogQueue(t *testing.T) {
	dir := t.TempDir()
	client, err := mikyu.Open(dir)
	require.NoError(t, err)
	defer client.Close()

	err = client.CreateQueue(t.Context(), "concurrent-q")
	require.NoError(t, err)

	const goroutines = 10
	var wg sync.WaitGroup
	queues := make([]mikyu.LogQueue, goroutines)

	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			lq, err := client.LogQueue("concurrent-q")
			assert.NoError(t, err)
			queues[idx] = lq
		}(i)
	}
	wg.Wait()

	// All returned handles must point to the exact same LogQueue instance pointer
	first := queues[0]
	for i := 1; i < goroutines; i++ {
		assert.Same(t, first, queues[i], "concurrent LogQueue() returned duplicate instances!")
	}
}

func TestRemoteClientDialInvalid(t *testing.T) {
	// Dial should not return an error for unreachable targets; it's lazy.
	client, err := mikyu.Dial("tcp://127.0.0.1:1")
	require.NoError(t, err)
	defer client.Close()

	// But operations must fail because there's no server.
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, err = client.ListQueues(ctx)
	assert.Error(t, err)
}

func TestTypedQueue(t *testing.T) {
	dir := t.TempDir()
	client, _ := mikyu.Open(dir)
	defer client.Close()

	err := client.CreateQueue(t.Context(), "typed")
	require.NoError(t, err)

	q, err := client.LogQueue("typed")
	require.NoError(t, err)

	type Item struct {
		Name string
	}
	codec := mikyu.Codec[Item]{
		Marshal: func(v Item) ([]byte, error) {
			return []byte(v.Name), nil
		},
		Unmarshal: func(data []byte, v *Item) error {
			v.Name = string(data)
			return nil
		},
	}
	tq := mikyu.NewTyped(q, codec)
	_, err = tq.Append(t.Context(), Item{Name: "test"})
	require.NoError(t, err)
	item, err := tq.Read(t.Context(), 0)
	require.NoError(t, err)
	assert.Equal(t, "test", item.Name)
}

func TestEmbeddedTaskQueueLifecycle(t *testing.T) {
	dir := t.TempDir()
	client, err := mikyu.Open(dir)
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.CreateQueue(t.Context(), "tasks"))

	q, err := client.TaskQueue("tasks")
	require.NoError(t, err)
	task, err := q.Enqueue(t.Context(), []byte("hello"), mikyu.WithMaxAttempts(2), mikyu.WithVisibility(time.Second))
	require.NoError(t, err)
	got, err := q.Dequeue(t.Context())
	require.NoError(t, err)
	assert.Equal(t, task.ID, got.ID)
	assert.Equal(t, uint32(1), got.Attempts)
	require.NoError(t, q.Ack(t.Context(), got))
}

func TestEmbeddedTaskQueueStableHandle(t *testing.T) {
	client, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.CreateQueue(t.Context(), "tasks"))

	const n = 16
	queues := make([]mikyu.TaskQueue, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			q, err := client.TaskQueue("tasks")
			require.NoError(t, err)
			queues[i] = q
		}(i)
	}
	wg.Wait()
	for i := 1; i < n; i++ {
		assert.Equal(t, queues[0], queues[i])
	}
}

// newTestServer starts a live server over loopback with empty session defaults
// and returns its dial target.
func newTestServer(t *testing.T) string {
	t.Helper()
	n, release, err := node.Acquire(t.TempDir(), mikyu.Spec{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = release() })

	srv := server.New(n)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)
	return "tcp://" + ln.Addr().String()
}

func TestEmbeddedUpdateQueueImmutable(t *testing.T) {
	client, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.CreateQueue(t.Context(), "q"))

	err = client.UpdateQueue(t.Context(), "q", mikyu.WithSegmentSize(128))
	require.ErrorIs(t, err, mikyu.ErrImmutableField)
	assert.Contains(t, err.Error(), "segment_size")
}

func TestRemoteUpdateQueueImmutable(t *testing.T) {
	client, err := mikyu.Dial(newTestServer(t))
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.CreateQueue(t.Context(), "q"))

	err = client.UpdateQueue(t.Context(), "q", mikyu.WithSegmentSize(128))
	require.ErrorIs(t, err, mikyu.ErrImmutableField)
	assert.Contains(t, err.Error(), "segment_size")
}

// TestSchemaOverWire: embedded and remote Schema share the same table, field
// for field — the schema is a property of the build, not of the transport.
func TestSchemaOverWire(t *testing.T) {
	emb, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer emb.Close()

	rem, err := mikyu.Dial(newTestServer(t))
	require.NoError(t, err)
	defer rem.Close()

	local, err := emb.Schema(t.Context())
	require.NoError(t, err)
	remote, err := rem.Schema(t.Context())
	require.NoError(t, err)

	require.Equal(t, len(local), len(remote))
	for i := range local {
		assert.Equal(t, local[i].Key, remote[i].Key, "row %d key", i)
		assert.Equal(t, local[i].Path, remote[i].Path, "row %d path", i)
		assert.Equal(t, local[i].Kind, remote[i].Kind, "row %d kind", i)
		assert.Equal(t, local[i].Layer, remote[i].Layer, "row %d layer", i)
		assert.Equal(t, local[i].Mutability, remote[i].Mutability, "row %d mutability", i)
		assert.Equal(t, local[i].EnvName, remote[i].EnvName, "row %d env", i)
		assert.Equal(t, local[i].FlagName, remote[i].FlagName, "row %d flag", i)
		assert.Equal(t, local[i].OptionName, remote[i].OptionName, "row %d option", i)
	}
}

func TestOpenDefaultsMismatch(t *testing.T) {
	dir := t.TempDir()
	first, err := mikyu.Open(dir, mikyu.WithSessionSpec(mikyu.Spec{
		Engine: mikyu.EngineSpec{BatchSize: new(7)},
	}))
	require.NoError(t, err)
	defer first.Close()

	_, err = mikyu.Open(dir, mikyu.WithSessionSpec(mikyu.Spec{
		Engine: mikyu.EngineSpec{BatchSize: new(8)},
	}))
	require.ErrorIs(t, err, mikyu.ErrSessionDefaultsMismatch)
}

func TestOpenDefaultsMatchSucceeds(t *testing.T) {
	dir := t.TempDir()
	session := mikyu.WithSessionSpec(mikyu.Spec{Engine: mikyu.EngineSpec{BatchSize: new(7)}})

	first, err := mikyu.Open(dir, session)
	require.NoError(t, err)
	defer first.Close()
	second, err := mikyu.Open(dir, session)
	require.NoError(t, err)
	defer second.Close()

	// Both handles reach the same shared node.
	require.NoError(t, second.CreateQueue(t.Context(), "shared"))
	names, err := first.ListQueues(t.Context())
	require.NoError(t, err)
	assert.Contains(t, names, "shared")
}
