package command_test

import (
	"context"
	"testing"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/ui/command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendReadTail(t *testing.T) {
	client, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer client.Close()
	ctx := t.Context()
	require.NoError(t, command.Create(ctx, client, mikyu.QueueKindLog, "events"))

	off, size, err := command.Append(ctx, client, "events", "msg1", nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), off)
	assert.Equal(t, 4, size)

	msg, err := command.Read(ctx, client, "events", off)
	require.NoError(t, err)
	assert.Equal(t, "msg1", string(msg.Payload))

	tailCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	var received []string
	_ = command.Tail(tailCtx, client, "events", 0, func(m mikyu.Record) error {
		received = append(received, string(m.Payload))
		return nil
	})
	assert.Contains(t, received, "msg1")
}

func TestFormatSingleLinePayload(t *testing.T) {
	assert.Equal(t, `a\nb`, command.FormatSingleLinePayload([]byte("a\nb"), 0))
	assert.Equal(t, "abc", command.FormatSingleLinePayload([]byte("abc"), 10))
	assert.Equal(t, "ab...", command.FormatSingleLinePayload([]byte("abcdefgh"), 5))
}

func TestResolvePayload(t *testing.T) {
	payload, err := command.ResolvePayload("hello")
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), payload)
}
