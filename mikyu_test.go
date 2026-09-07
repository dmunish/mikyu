package mikyu_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dmunish/mikyu"
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

	err = client.CreateQueue(context.Background(), "test")
	require.NoError(t, err)

	q, err := client.LogQueue("test")
	require.NoError(t, err)
	assert.Equal(t, "test", q.Name())

	off, err := q.Produce(context.Background(), []byte("hello"))
	require.NoError(t, err)
	assert.Equal(t, uint64(0), off)

	msg, err := q.Read(context.Background(), off)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(msg.Payload))

	stats, err := client.Stats(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, stats.ActiveQueues)
}

func TestEmbeddedClientDuplicateQueue(t *testing.T) {
	dir := t.TempDir()
	client, _ := mikyu.Open(dir)
	defer client.Close()

	err := client.CreateQueue(context.Background(), "dup")
	require.NoError(t, err)
	err = client.CreateQueue(context.Background(), "dup")
	assert.Error(t, err)
}

func TestEmbeddedClientDeleteQueue(t *testing.T) {
	dir := t.TempDir()
	client, _ := mikyu.Open(dir)
	defer client.Close()

	err := client.CreateQueue(context.Background(), "temp")
	require.NoError(t, err)

	err = client.DeleteQueue(context.Background(), "temp")
	require.NoError(t, err)

	_, err = client.LogQueue("temp")
	assert.Error(t, err)
}

func TestEmbeddedClientConcurrentLogQueue(t *testing.T) {
	dir := t.TempDir()
	client, err := mikyu.Open(dir)
	require.NoError(t, err)
	defer client.Close()

	err = client.CreateQueue(context.TODO(), "concurrent-q")
	require.NoError(t, err)

	const goroutines = 10
	var wg sync.WaitGroup
	queues := make([]mikyu.LogQueue, goroutines)

	for i := 0; i < goroutines; i++ {
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = client.ListQueues(ctx)
	assert.Error(t, err)
}

func TestTypedQueue(t *testing.T) {
	dir := t.TempDir()
	client, _ := mikyu.Open(dir)
	defer client.Close()

	err := client.CreateQueue(context.Background(), "typed")
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
	_, err = tq.Produce(context.Background(), Item{Name: "test"})
	require.NoError(t, err)
	item, err := tq.Read(context.Background(), 0)
	require.NoError(t, err)
	assert.Equal(t, "test", item.Name)
}

func TestEmbeddedTaskQueueLifecycle(t *testing.T) {
	dir := t.TempDir()
	client, err := mikyu.Open(dir)
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.CreateQueue(context.Background(), "tasks"))

	q, err := client.TaskQueue("tasks")
	require.NoError(t, err)
	task, err := q.Enqueue(context.Background(), []byte("hello"), mikyu.WithMaxAttempts(2), mikyu.WithVisibilityTimeout(time.Second))
	require.NoError(t, err)
	got, err := q.Dequeue(context.Background())
	require.NoError(t, err)
	assert.Equal(t, task.ID, got.ID)
	assert.Equal(t, uint32(1), got.Attempts)
	require.NoError(t, q.Ack(context.Background(), got))
}

func TestEmbeddedTaskQueueStableHandle(t *testing.T) {
	client, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.CreateQueue(context.Background(), "tasks"))

	const n = 16
	queues := make([]mikyu.TaskQueue, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
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
