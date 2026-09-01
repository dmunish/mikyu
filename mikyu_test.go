package mikyu_test

import (
	"context"
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

	q, err := client.CreateQueue(context.Background(), "test")
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

	_, _ = client.CreateQueue(context.Background(), "dup")
	_, err := client.CreateQueue(context.Background(), "dup")
	assert.Error(t, err)
}

func TestEmbeddedClientDeleteQueue(t *testing.T) {
	dir := t.TempDir()
	client, _ := mikyu.Open(dir)
	defer client.Close()

	_, _ = client.CreateQueue(context.Background(), "temp")
	err := client.DeleteQueue(context.Background(), "temp")
	require.NoError(t, err)
	_, err = client.Queue("temp")
	assert.Error(t, err)
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
	q, _ := client.CreateQueue(context.Background(), "typed")

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
	_, err := tq.Produce(context.Background(), Item{Name: "test"})
	require.NoError(t, err)
	item, err := tq.Read(context.Background(), 0)
	require.NoError(t, err)
	assert.Equal(t, "test", item.Name)
}
