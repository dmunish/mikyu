package command_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolvePayload(t *testing.T) {
	payload, err := command.ResolvePayload("hello")
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), payload)

	tmp := filepath.Join(t.TempDir(), "data.txt")
	require.NoError(t, os.WriteFile(tmp, []byte("file content"), 0644))
	payload, err = command.ResolvePayload("@" + tmp)
	assert.NoError(t, err)
	assert.Equal(t, []byte("file content"), payload)
}

func TestCommandLifecycle(t *testing.T) {
	dir := t.TempDir()
	client, err := mikyu.Open(dir)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// 1. Create Queue
	err = command.Create(ctx, client, "events")
	require.NoError(t, err)

	// 2. List Queues
	queues, err := command.List(ctx, client)
	require.NoError(t, err)
	assert.Equal(t, []string{"events"}, queues)

	// 3. Produce
	off, size, err := command.Produce(ctx, client, "events", "msg1")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), off)
	assert.Equal(t, 4, size)

	// 4. Read
	msg, err := command.Read(ctx, client, "events", off)
	require.NoError(t, err)
	assert.Equal(t, "msg1", string(msg.Payload))

	// 5. Tail
	tailCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	var received []string
	_ = command.Tail(tailCtx, client, "events", 0, func(m mikyu.Message) error {
		received = append(received, string(m.Payload))
		return nil
	})
	assert.Contains(t, received, "msg1")

	// 6. Stats
	st, err := command.Stats(ctx, client)
	require.NoError(t, err)
	assert.Equal(t, 1, st.ActiveQueues)

	// 7. Compact with confirmation (canceled)
	reclaimed, executed, err := command.Compact(ctx, client, "events", 0, func() (bool, error) {
		return false, nil
	})
	require.NoError(t, err)
	assert.False(t, executed)
	assert.Equal(t, int64(0), reclaimed)

	// 8. Delete with confirmation
	executed, err = command.Delete(ctx, client, "events", func() (bool, error) {
		return true, nil
	})
	require.NoError(t, err)
	assert.True(t, executed)

	queues, err = command.List(ctx, client)
	require.NoError(t, err)
	assert.Empty(t, queues)
}

func TestCommandShutdownConfirmationCancel(t *testing.T) {
	executed, err := command.Shutdown("tcp://127.0.0.1:1", func() (bool, error) {
		return false, nil
	})
	assert.NoError(t, err)
	assert.False(t, executed)
}
