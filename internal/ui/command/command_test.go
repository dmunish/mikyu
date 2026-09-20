package command_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/ui/command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOpenClientForwardsMetricsSession: the CLI's global metrics flags reach
// the embedded client instead of being silently dropped.
func TestOpenClientForwardsMetricsSession(t *testing.T) {
	sess := spec.BuiltinSession()
	sess.DataDir = t.TempDir()
	sess.MetricsEnabled = true
	sess.MetricsInterval = spec.Duration(20 * time.Millisecond)

	client, err := command.OpenClient(sess)
	require.NoError(t, err)
	defer client.Close()

	ch, cancel := client.SubscribeSnapshots()
	defer cancel()

	select {
	case _, ok := <-ch:
		require.True(t, ok)
	case <-time.After(time.Second):
		t.Fatal("OpenClient did not forward the metrics session to the embedded collector")
	}
}

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

	ctx := t.Context()

	// 1. Create Queue
	err = command.Create(ctx, client, mikyu.QueueKindLog, "events")
	require.NoError(t, err)

	// 2. List Queues
	queues, err := command.List(ctx, client)
	require.NoError(t, err)
	require.Len(t, queues, 1)
	assert.Equal(t, "events", queues[0].Name)
	assert.Equal(t, mikyu.QueueKindLog, queues[0].Kind)

	// 3. Produce
	off, size, err := command.Append(ctx, client, "events", "msg1", nil)
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
	_ = command.Tail(tailCtx, client, "events", 0, func(m mikyu.Record) error {
		received = append(received, string(m.Payload))
		return nil
	})
	assert.Contains(t, received, "msg1")

	// 6. Metrics
	snap, err := command.Metrics(ctx, client)
	require.NoError(t, err)
	_, ok := snap.Queue("events")
	assert.True(t, ok)

	// 7. Truncate with confirmation (canceled)
	reclaimed, executed, err := command.Truncate(ctx, client, "events", 0, func() (bool, error) {
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

func TestOpenClientForwardsAuth(t *testing.T) {
	tokenPath := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenPath, []byte("0123456789abcdef"), 0600))

	sess := spec.BuiltinSession()
	sess.DataDir = t.TempDir()
	sess.AuthMethod = "token"
	sess.AuthTokenFile = tokenPath

	client, err := command.OpenClient(sess)
	require.NoError(t, err)
	defer client.Close()

	queues, err := command.List(t.Context(), client)
	require.NoError(t, err)
	assert.Empty(t, queues)
}

func TestCommandShutdownConfirmationCancel(t *testing.T) {
	executed, err := command.Shutdown("tcp://127.0.0.1:1", spec.BuiltinSession(), func() (bool, error) {
		return false, nil
	})
	assert.NoError(t, err)
	assert.False(t, executed)
}
